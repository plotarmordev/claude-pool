from __future__ import annotations

import asyncio
import os
from pathlib import Path
import sys
import time
from typing import Any

import pytest

import claude_pool
from claude_pool import (
    AskTimeout,
    ClaudePool,
    ClaudePoolError,
    PoolClosed,
    WorkerStartError,
    _LIVE_PGIDS,
)


ROOT = Path(__file__).resolve().parents[1]
STREAM_FAKE = ROOT / "tests" / "fake_claude.py"
TUI_FAKE = ROOT / "tests" / "fake_claude_tui.py"


BACKENDS = (
    pytest.param(
        {
            "backend": "stream-json",
            "claude_bin": str(STREAM_FAKE),
            "extra_args": [],
        },
        id="stream-json",
    ),
    pytest.param(
        {
            "backend": "tui",
            "claude_bin": sys.executable,
            "extra_args": [str(TUI_FAKE)],
        },
        id="tui",
    ),
)


def run(coro: Any) -> Any:
    return asyncio.run(coro)


def pool_kwargs(config: dict[str, Any], **overrides: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        **config,
        "warm": 0,
        "max_workers": 4,
        "default_timeout": 6.0,
    }
    kwargs.update(overrides)
    return kwargs


def tui_pool_kwargs(**overrides: Any) -> dict[str, Any]:
    return pool_kwargs(
        {
            "backend": "tui",
            "claude_bin": sys.executable,
            "extra_args": [str(TUI_FAKE)],
        },
        **overrides,
    )


async def wait_for_warm(pool: ClaudePool, count: int) -> None:
    deadline = time.monotonic() + 10.0
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


async def assert_no_live_process_groups() -> None:
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        live = []
        for pgid in tuple(_LIVE_PGIDS):
            try:
                os.killpg(pgid, 0)
            except ProcessLookupError:
                continue
            live.append(pgid)
        if not live:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"live process groups remain: {tuple(_LIVE_PGIDS)}")


@pytest.mark.parametrize("config", BACKENDS)
def test_backend_warm_hit_consumes_worker_and_replenishes(config: dict[str, Any]) -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs(config, warm=1))
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


@pytest.mark.parametrize("config", BACKENDS)
def test_backend_plain_asks_use_fresh_sessions(config: dict[str, Any]) -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs(config))
        try:
            first = await pool.ask("one")
            second = await pool.ask("two")

            assert first.session_id != second.session_id
        finally:
            await pool.aclose()

    run(scenario())


@pytest.mark.parametrize("config", BACKENDS)
def test_backend_max_workers_one_serializes(config: dict[str, Any]) -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs(config, max_workers=1))
        try:
            start = time.monotonic()
            first, second = await asyncio.gather(
                pool.ask("SLEEP:0.4"),
                pool.ask("SLEEP:0.4"),
            )

            assert time.monotonic() - start >= 0.8
            assert first.text == "SLEEP:0.4"
            assert second.text == "SLEEP:0.4"
        finally:
            await pool.aclose()

    run(scenario())


@pytest.mark.parametrize("config", BACKENDS)
def test_backend_ask_after_aclose_raises_pool_closed(config: dict[str, Any]) -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs(config))
        await pool.aclose()

        with pytest.raises(PoolClosed):
            await pool.ask("closed")

    run(scenario())


@pytest.mark.parametrize("config", BACKENDS)
def test_backend_aclose_leaves_no_live_groups(config: dict[str, Any]) -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs(config, warm=2))
        await pool.start()
        await wait_for_warm(pool, 2)

        await pool.aclose()
        await assert_no_live_process_groups()

    run(scenario())


@pytest.mark.parametrize("config", BACKENDS)
def test_backend_session_two_sends_share_session_id(config: dict[str, Any]) -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs(config))
        try:
            async with pool.session() as session:
                first = await session.send("first")
                second = await session.send("second")

            assert first.session_id == second.session_id
        finally:
            await pool.aclose()

    run(scenario())


@pytest.mark.parametrize("config", BACKENDS)
def test_backend_timeout_kills_group(config: dict[str, Any]) -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs(config, max_workers=1))
        try:
            with pytest.raises(AskTimeout):
                await pool.ask("SLEEP:30", timeout=2.0)
        finally:
            await pool.aclose()

        await assert_no_live_process_groups()

    run(scenario())


def test_pool_tui_ready_timeout_is_configurable() -> None:
    async def scenario() -> None:
        pool = ClaudePool(
            **tui_pool_kwargs(tui_ready_timeout=0.5, env={"FAKE_TUI_SLOW_START": "10"})
        )
        try:
            started = time.monotonic()
            with pytest.raises(WorkerStartError, match="did not become ready"):
                await pool.ask("never-ready")

            assert time.monotonic() - started < 5.0
        finally:
            await pool.aclose()

    run(scenario())


def test_pool_spawn_concurrency_serializes_cold_starts() -> None:
    async def scenario() -> None:
        pool = ClaudePool(**tui_pool_kwargs(spawn_concurrency=1, env={"FAKE_TUI_SLOW_START": "1"}))
        workers: list[Any] = []
        try:
            started = time.monotonic()
            workers = list(await asyncio.gather(pool._spawn_worker(), pool._spawn_worker()))

            assert time.monotonic() - started >= 2.0
        finally:
            for worker in workers:
                await worker.kill()
            await pool.aclose()
            await assert_no_live_process_groups()

    run(scenario())


def test_pool_spawn_concurrency_default_allows_overlapping_cold_starts() -> None:
    async def scenario() -> None:
        pool = ClaudePool(**tui_pool_kwargs(env={"FAKE_TUI_SLOW_START": "1"}))
        workers: list[Any] = []
        try:
            started = time.monotonic()
            workers = list(await asyncio.gather(pool._spawn_worker(), pool._spawn_worker()))

            assert time.monotonic() - started < 2.0
        finally:
            for worker in workers:
                await worker.kill()
            await pool.aclose()
            await assert_no_live_process_groups()

    run(scenario())


def test_unknown_backend_raises_claude_pool_error() -> None:
    with pytest.raises(ClaudePoolError, match="unknown backend: nope"):
        ClaudePool(backend="nope")


def test_tui_backend_validates_posix_at_construction(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(claude_pool.os, "name", "nt")

    with pytest.raises(ClaudePoolError, match="tui backend requires POSIX ptys"):
        ClaudePool(backend="tui")
