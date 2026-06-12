from __future__ import annotations

import asyncio
from contextlib import suppress
import os
from pathlib import Path
import stat
import time
from typing import Any

import pytest

from claude_pool import ClaudePool, ClaudePoolError, PoolClosed, WorkerCrashError, WorkerStartError
from claude_pool import _LIVE_PGIDS


ROOT = Path(__file__).resolve().parents[1]
FAKE = ROOT / "tests" / "fake_claude.py"


def run(coro: Any) -> Any:
    return asyncio.run(coro)


def pool_kwargs(**overrides: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "claude_bin": str(FAKE),
        "extra_args": [],
        "warm": 0,
        "max_workers": 4,
        "default_timeout": 5.0,
    }
    kwargs.update(overrides)
    return kwargs


async def wait_for_warm(pool: ClaudePool, count: int) -> None:
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if len(pool._warm) >= count:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"expected at least {count} warm workers")


async def assert_process_group_gone(pgid: int) -> None:
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"process group {pgid} still exists")


def write_warm_crash_wrapper(tmp_path: Path) -> Path:
    marker = tmp_path / "used"
    wrapper = tmp_path / "claude-wrapper.py"
    wrapper.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import json",
                "import os",
                "from pathlib import Path",
                "import sys",
                f"marker = Path({str(marker)!r})",
                f"fake = {str(FAKE)!r}",
                "if marker.exists():",
                "    os.execv(sys.executable, [sys.executable, fake, *sys.argv[1:]])",
                "marker.write_text('used')",
                "line = sys.stdin.readline()",
                "if line:",
                "    session_id = 'warm-crash-session'",
                "    print(json.dumps({'type':'system','subtype':'init','session_id':session_id}), flush=True)",
                "    print(json.dumps({'type':'assistant','session_id':session_id,'message':{'role':'assistant','content':[{'type':'text','text':'dying'}]}}), flush=True)",
                "sys.stderr.write('warm worker crashed during turn\\n')",
                "sys.stderr.flush()",
                "raise SystemExit(1)",
            ]
        )
        + "\n"
    )
    wrapper.chmod(wrapper.stat().st_mode | stat.S_IXUSR)
    return wrapper


def write_startup_fail_once_wrapper(tmp_path: Path) -> Path:
    marker = tmp_path / "startup-used"
    wrapper = tmp_path / "claude-startup-wrapper.py"
    wrapper.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import os",
                "from pathlib import Path",
                "import sys",
                f"marker = Path({str(marker)!r})",
                f"fake = {str(FAKE)!r}",
                "if not marker.exists():",
                "    marker.write_text('used')",
                "    raise SystemExit(2)",
                "os.execv(sys.executable, [sys.executable, fake, *sys.argv[1:]])",
            ]
        )
        + "\n"
    )
    wrapper.chmod(wrapper.stat().st_mode | stat.S_IXUSR)
    return wrapper


def test_warm_hit_consumes_worker_and_replenishes() -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs(warm=1))
        try:
            await pool.start()
            await wait_for_warm(pool, 1)
            first_pgid = pool._warm[-1]._pgid

            result = await pool.ask("warm-hit")
            await wait_for_warm(pool, 1)

            assert result.text == "warm-hit"
            assert pool._warm[-1]._pgid != first_pgid
            await assert_process_group_gone(first_pgid)
        finally:
            await pool.aclose()

    run(scenario())


def test_warm_pool_sequential_asks_do_not_pay_inline_replenish() -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs(warm=1))
        try:
            warmup = await pool.ask("latency-warmup")
            start = time.monotonic()
            results = [await pool.ask(f"latency-{index}") for index in range(4)]

            assert warmup.text == "latency-warmup"
            assert [result.text for result in results] == [
                "latency-0",
                "latency-1",
                "latency-2",
                "latency-3",
            ]
            assert time.monotonic() - start < 1.5
        finally:
            await pool.aclose()

    run(scenario())


def test_fresh_context_per_plain_ask() -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs())
        try:
            first = await pool.ask("one")
            second = await pool.ask("two")

            assert first.session_id != second.session_id
        finally:
            await pool.aclose()

    run(scenario())


def test_missing_claude_binary_raises_worker_start_error_without_hanging() -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs(claude_bin="/nonexistent/claude", warm=1))
        try:
            start = time.monotonic()
            with pytest.raises(WorkerStartError) as exc_info:
                await pool.ask("missing")

            assert isinstance(exc_info.value, ClaudePoolError)
            assert time.monotonic() - start < 5.0
        finally:
            await pool.aclose()

    run(scenario())


def test_warm_spawn_cooldown_does_not_block_cold_ask(tmp_path: Path) -> None:
    async def scenario() -> None:
        wrapper = write_startup_fail_once_wrapper(tmp_path)
        pool = ClaudePool(**pool_kwargs(claude_bin=str(wrapper), warm=1))
        try:
            await pool.start()

            start = time.monotonic()
            result = await pool.ask("after-cooldown")

            assert result.text == "after-cooldown"
            assert time.monotonic() - start < 2.0
        finally:
            await pool.aclose()

    run(scenario())


def test_dead_warm_worker_is_discarded_at_checkout() -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs(warm=1))
        try:
            await pool.start()
            await wait_for_warm(pool, 1)
            dead = pool._warm[-1]
            await dead.kill()

            result = await pool.ask("after-dead")

            assert result.text == "after-dead"
        finally:
            await pool.aclose()

    run(scenario())


def test_warm_crash_retries_once_with_cold_worker(tmp_path: Path) -> None:
    async def scenario() -> None:
        wrapper = write_warm_crash_wrapper(tmp_path)
        pool = ClaudePool(**pool_kwargs(claude_bin=str(wrapper), extra_args=[], warm=1))
        try:
            await pool.start()
            await wait_for_warm(pool, 1)

            result = await pool.ask("retry-me")

            assert result.text == "retry-me"
            assert result.session_id.startswith("fake-")
        finally:
            await pool.aclose()

    run(scenario())


def test_cold_worker_crash_propagates() -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs(env={"FAKE_CLAUDE_STARTUP": "exit2"}))
        try:
            with pytest.raises(WorkerCrashError):
                await pool.ask("crash")
        finally:
            await pool.aclose()

    run(scenario())


def test_max_workers_serializes_concurrent_asks() -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs(max_workers=1))
        try:
            start = time.monotonic()
            first, second = await asyncio.gather(
                pool.ask("SLEEP:0.5"),
                pool.ask("SLEEP:0.5"),
            )

            assert time.monotonic() - start >= 1.0
            assert first.text == "SLEEP:0.5"
            assert second.text == "SLEEP:0.5"
        finally:
            await pool.aclose()

    run(scenario())


def test_max_idle_expires_warm_workers_at_checkout() -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs(warm=1, max_idle=0.1))
        try:
            await pool.start()
            await wait_for_warm(pool, 1)
            expired_pgid = pool._warm[-1]._pgid
            await asyncio.sleep(0.2)

            result = await pool.ask("after-expiry")

            assert result.text == "after-expiry"
            await assert_process_group_gone(expired_pgid)
        finally:
            await pool.aclose()

    run(scenario())


def test_aclose_retires_all_warm_workers() -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs(warm=2))
        await pool.start()
        await wait_for_warm(pool, 2)
        pgids = [worker._pgid for worker in pool._warm]

        await pool.aclose()

        for pgid in pgids:
            await assert_process_group_gone(pgid)

    run(scenario())


def test_aclose_waits_for_in_flight_ask_and_reaps_process_group() -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs(warm=0, max_workers=1))
        task = asyncio.create_task(pool.ask("SLEEP:1"))
        await asyncio.sleep(0.3)
        pgids = tuple(_LIVE_PGIDS)

        await pool.aclose()
        result = await task

        assert result.text == "SLEEP:1"
        for pgid in pgids:
            await assert_process_group_gone(pgid)

    run(scenario())


def test_aclose_serializes_with_in_flight_start_prewarm() -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs(warm=1))
        start_task = asyncio.create_task(pool.start())
        await asyncio.sleep(0.1)
        pgids = tuple(_LIVE_PGIDS)

        await pool.aclose()
        with suppress(PoolClosed):
            await start_task

        for pgid in pgids:
            await assert_process_group_gone(pgid)

    run(scenario())


def test_ask_after_aclose_raises_pool_closed() -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs())
        await pool.aclose()

        with pytest.raises(PoolClosed):
            await pool.ask("closed")

    run(scenario())


def test_concurrent_asks_return_unmixed_results() -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs(max_workers=4))
        prompts = [f"nonce-{index}" for index in range(4)]
        try:
            results = await asyncio.gather(*(pool.ask(prompt) for prompt in prompts))

            assert sorted(result.text for result in results) == prompts
            assert len({result.session_id for result in results}) == 4
        finally:
            await pool.aclose()

    run(scenario())
