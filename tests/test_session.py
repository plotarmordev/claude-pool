from __future__ import annotations

import asyncio
import os
from pathlib import Path
import threading
import time
from typing import Any

import pytest

from claude_pool import (
    AskTimeout,
    ClaudePool,
    ClaudePoolError,
    PoolClosed,
    Result,
    WorkerCrashError,
)
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


async def assert_process_group_gone(pgid: int) -> None:
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"process group {pgid} still exists")


def live_process_groups() -> list[int]:
    live = []
    for pgid in tuple(_LIVE_PGIDS):
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            continue
        live.append(pgid)
    return live


def wait_for_no_live_process_groups() -> None:
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        live = live_process_groups()
        if not live:
            return
        time.sleep(0.05)
    raise AssertionError(f"live process groups remain: {live_process_groups()}")


def wait_for_thread_baseline(baseline: set[threading.Thread]) -> None:
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if set(threading.enumerate()).issubset(baseline):
            return
        time.sleep(0.05)
    extra = set(threading.enumerate()) - baseline
    raise AssertionError(f"extra threads remain: {[thread.name for thread in extra]}")


def join_threads(threads: list[threading.Thread], timeout: float = 15.0) -> None:
    for thread in threads:
        thread.join(timeout=timeout)
        assert not thread.is_alive(), f"{thread.name} did not finish"


def sync_loop_thread_count() -> int:
    return sum(1 for thread in threading.enumerate() if thread.name == "claude-pool-sync-loop")


def test_session_reuses_worker_between_plain_fresh_asks() -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs())
        try:
            before = await pool.ask("before")
            async with pool.session() as session:
                first = await session.send("first")
                second = await session.send("second")
            after = await pool.ask("after")

            assert first.session_id == second.session_id
            assert before.session_id != first.session_id
            assert after.session_id != first.session_id
            assert before.session_id != after.session_id
            assert [first.raw["num_turns"], second.raw["num_turns"]] == [1, 2]
        finally:
            await pool.aclose()

    run(scenario())


def test_session_holds_semaphore_slot_until_exit() -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs(max_workers=1))
        try:
            async with pool.session():
                start = time.monotonic()
                ask_task = asyncio.create_task(pool.ask("SLEEP:0.1"))
                await asyncio.sleep(0.3)

                assert not ask_task.done()

            result = await ask_task
            assert result.text == "SLEEP:0.1"
            assert time.monotonic() - start >= 0.3
        finally:
            await pool.aclose()

    run(scenario())


def test_worker_crash_poisoned_session_exits_cleanly() -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs())
        session_cm = pool.session()
        session = await session_cm.__aenter__()
        assert session._worker is not None
        pgid = session._worker._pgid
        try:
            with pytest.raises(WorkerCrashError):
                await session.send("DIE")
            with pytest.raises(ClaudePoolError, match="session closed"):
                await session.send("after crash")
        finally:
            await session_cm.__aexit__(None, None, None)
            await pool.aclose()

        await assert_process_group_gone(pgid)

    run(scenario())


def test_ask_timeout_poisoned_session_exits_cleanly() -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs())
        session_cm = pool.session()
        session = await session_cm.__aenter__()
        assert session._worker is not None
        pgid = session._worker._pgid
        try:
            with pytest.raises(AskTimeout):
                await session.send("SLEEP:30", timeout=0.2)
            with pytest.raises(ClaudePoolError, match="session closed"):
                await session.send("after timeout")
        finally:
            await session_cm.__aexit__(None, None, None)
            await pool.aclose()

        await assert_process_group_gone(pgid)

    run(scenario())


def test_concurrent_session_sends_are_serialized() -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs())
        try:
            async with pool.session() as session:
                start = time.monotonic()
                first_task = asyncio.create_task(session.send("SLEEP:0.3"))
                second_task = asyncio.create_task(session.send("SLEEP:0.3"))

                first, second = await asyncio.gather(first_task, second_task)

            assert first.text == "SLEEP:0.3"
            assert second.text == "SLEEP:0.3"
            assert first.session_id == second.session_id
            assert time.monotonic() - start >= 0.6
        finally:
            await pool.aclose()

    run(scenario())


def test_sync_ask_session_close_smoke_without_asyncio_run() -> None:
    baseline_threads = set(threading.enumerate())
    pool = ClaudePool(**pool_kwargs())
    try:
        result = pool.ask_sync("sync-one")
        with pool.session_sync() as session:
            first = session.send("sync-two")
            second = session.send("sync-three")

        assert result.text == "sync-one"
        assert first.session_id == second.session_id
        assert [first.raw["num_turns"], second.raw["num_turns"]] == [1, 2]

        pool.close()
        pool.close()
        with pytest.raises(PoolClosed):
            pool.ask_sync("closed")

        wait_for_thread_baseline(baseline_threads)
        wait_for_no_live_process_groups()
    finally:
        pool.close()


def test_mode_exclusivity_sync_then_async() -> None:
    pool = ClaudePool(**pool_kwargs())
    try:
        assert pool.ask_sync("sync-first").text == "sync-first"
        with pytest.raises(ClaudePoolError, match="sync API"):
            run(pool.ask("async-second"))
    finally:
        pool.close()


def test_mode_exclusivity_async_then_sync() -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs())
        try:
            assert (await pool.ask("async-first")).text == "async-first"
            with pytest.raises(ClaudePoolError, match="async API"):
                pool.ask_sync("sync-second")
        finally:
            await pool.aclose()

    run(scenario())


def test_double_aclose_and_double_session_exit_are_noops() -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs())
        session_cm = pool.session()
        session = await session_cm.__aenter__()
        assert session._worker is not None
        pgid = session._worker._pgid

        result = await session.send("one")
        await session_cm.__aexit__(None, None, None)
        await session_cm.__aexit__(None, None, None)

        assert result.text == "one"
        with pytest.raises(ClaudePoolError, match="session closed"):
            await session.send("after exit")
        with pytest.raises(ClaudePoolError, match="session closed"):
            await session_cm.__aenter__()

        await pool.aclose()
        await pool.aclose()
        await assert_process_group_gone(pgid)

    run(scenario())


def test_concurrent_session_double_enter_only_allows_one_entry() -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs())
        session_cm = pool.session()
        try:
            entries = await asyncio.gather(
                session_cm.__aenter__(),
                session_cm.__aenter__(),
                return_exceptions=True,
            )
            successes = [entry for entry in entries if not isinstance(entry, BaseException)]
            errors = [entry for entry in entries if isinstance(entry, BaseException)]

            assert len(successes) == 1
            assert len(errors) == 1
            assert isinstance(errors[0], ClaudePoolError)

            await session_cm.__aexit__(None, None, None)
            await asyncio.wait_for(pool.aclose(), timeout=10.0)
        finally:
            await session_cm.__aexit__(None, None, None)
            await pool.aclose()

    run(scenario())


def test_session_exit_during_send_reports_session_closed() -> None:
    async def scenario() -> None:
        pool = ClaudePool(**pool_kwargs())
        session_cm = pool.session()
        session = await session_cm.__aenter__()
        assert session._worker is not None
        pgid = session._worker._pgid
        try:
            send_task = asyncio.create_task(session.send("SLEEP:2"))
            await asyncio.sleep(0.3)
            await session_cm.__aexit__(None, None, None)

            with pytest.raises(ClaudePoolError, match="session closed"):
                await send_task

            await pool.aclose()
            await assert_process_group_gone(pgid)
        finally:
            await session_cm.__aexit__(None, None, None)
            await pool.aclose()

    run(scenario())


def test_pool_context_managers_work() -> None:
    async def async_scenario() -> None:
        async with ClaudePool(**pool_kwargs()) as pool:
            result = await pool.ask("async-context")
            assert result.text == "async-context"

    run(async_scenario())

    baseline_threads = set(threading.enumerate())
    with ClaudePool(**pool_kwargs()) as pool:
        result = pool.ask_sync("sync-context")
        assert result.text == "sync-context"
    wait_for_thread_baseline(baseline_threads)
    wait_for_no_live_process_groups()


def test_concurrent_fresh_ask_sync_creates_one_loop_thread() -> None:
    for iteration in range(10):
        baseline_threads = set(threading.enumerate())
        pool = ClaudePool(**pool_kwargs())
        barrier = threading.Barrier(3)
        results: list[Result] = []
        errors: list[BaseException] = []
        lock = threading.Lock()

        def ask_worker(prompt: str) -> None:
            try:
                barrier.wait(timeout=5.0)
                result = pool.ask_sync(prompt)
            except BaseException as exc:
                with lock:
                    errors.append(exc)
            else:
                with lock:
                    results.append(result)

        threads = [
            threading.Thread(
                target=ask_worker,
                args=("SLEEP:0.1",),
                name=f"ask-sync-race-{iteration}-{index}",
            )
            for index in range(2)
        ]
        for thread in threads:
            thread.start()
        try:
            barrier.wait(timeout=5.0)
            max_loop_threads = sync_loop_thread_count()
            while any(thread.is_alive() for thread in threads):
                max_loop_threads = max(max_loop_threads, sync_loop_thread_count())
                time.sleep(0.01)
            join_threads(threads)

            assert not errors
            assert len(results) == 2
            assert all(isinstance(result, Result) for result in results)
            assert max_loop_threads <= 1

            pool.close()
            wait_for_thread_baseline(baseline_threads)
            wait_for_no_live_process_groups()
        finally:
            join_threads(threads)
            pool.close()


def test_concurrent_close_calls_do_not_hang_or_leak() -> None:
    for _iteration in range(10):
        pool = ClaudePool(**pool_kwargs())
        assert pool.ask_sync("before-close").text == "before-close"
        barrier = threading.Barrier(3)
        errors: list[BaseException] = []
        lock = threading.Lock()

        def close_worker() -> None:
            try:
                barrier.wait(timeout=5.0)
                pool.close()
            except BaseException as exc:
                with lock:
                    errors.append(exc)

        threads = [
            threading.Thread(target=close_worker, name=f"close-race-{index}") for index in range(2)
        ]
        for thread in threads:
            thread.start()
        try:
            barrier.wait(timeout=5.0)
            join_threads(threads)

            assert not errors
            wait_for_no_live_process_groups()
        finally:
            join_threads(threads)
            pool.close()


def test_sync_session_exit_during_close_releases_slot_cleanly() -> None:
    for iteration in range(5):
        baseline_threads = set(threading.enumerate())
        pool = ClaudePool(**pool_kwargs(max_workers=1))
        entered = threading.Event()
        errors: list[BaseException] = []
        results: list[Result] = []
        lock = threading.Lock()

        def session_worker() -> None:
            try:
                with pool.session_sync() as session:
                    entered.set()
                    result = session.send("session-before-close")
                    with lock:
                        results.append(result)
                    time.sleep(0.5)
            except BaseException as exc:
                with lock:
                    errors.append(exc)

        thread = threading.Thread(target=session_worker, name=f"sync-session-close-{iteration}")
        thread.start()
        try:
            assert entered.wait(timeout=5.0)
            time.sleep(0.2)
            pool.close()

            join_threads([thread], timeout=10.0)
            assert not errors
            assert len(results) == 1
            assert results[0].text == "session-before-close"
            wait_for_thread_baseline(baseline_threads)
            wait_for_no_live_process_groups()
        finally:
            join_threads([thread], timeout=10.0)
            pool.close()


def test_ask_sync_churn_during_close_returns_results_or_pool_closed() -> None:
    pool = ClaudePool(**pool_kwargs(max_workers=4))
    errors: list[BaseException] = []
    results: list[Result] = []
    lock = threading.Lock()

    def churn_worker(index: int) -> None:
        call_index = 0
        while True:
            try:
                result = pool.ask_sync("SLEEP:0.05")
            except PoolClosed:
                return
            except BaseException as exc:
                with lock:
                    errors.append(exc)
                return
            else:
                with lock:
                    results.append(result)
                call_index += 1

    threads = [
        threading.Thread(target=churn_worker, args=(index,), name=f"sync-churn-{index}")
        for index in range(4)
    ]
    for thread in threads:
        thread.start()
    try:
        time.sleep(0.2)
        pool.close()
        join_threads(threads)

        assert results
        assert not errors
        wait_for_no_live_process_groups()
    finally:
        join_threads(threads)
        pool.close()
