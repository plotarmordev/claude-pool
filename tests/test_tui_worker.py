from __future__ import annotations

import asyncio
from contextlib import suppress
import os
import shlex
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from claude_pool import (
    AskTimeout,
    Result,
    WorkerCrashError,
    WorkerStartError,
    _LIVE_PGIDS,
    _TuiWorker,
)


ROOT = Path(__file__).resolve().parents[1]
FAKE = ROOT / "tests" / "fake_claude_tui.py"


def run(coro: Any) -> Any:
    return asyncio.run(coro)


def fake_command() -> list[str]:
    return [sys.executable, str(FAKE)]


async def spawn_fake(
    *,
    env: dict[str, str] | None = None,
    argv: list[str] | None = None,
) -> _TuiWorker:
    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    return await _TuiWorker.spawn(argv or fake_command(), env=full_env)


async def assert_process_group_gone(pgid: int) -> None:
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"process group {pgid} still exists")


async def assert_no_tui_reader_threads() -> None:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        threads = [
            thread
            for thread in threading.enumerate()
            if thread.name == "claude-pool-tui-pty-reader"
        ]
        if not threads:
            return
        await asyncio.sleep(0.05)
    raise AssertionError("TUI pty reader threads remain")


def shell_wrapped_fake() -> list[str]:
    quoted = " ".join(shlex.quote(arg) for arg in fake_command())
    return ["/bin/sh", "-c", f'sleep 30 & exec {quoted} "$@"', "fake-sh"]


def test_tui_worker_happy_echo_and_result_conversion() -> None:
    async def scenario() -> None:
        worker = await spawn_fake()
        try:
            result_message, rate_limit = await worker.ask("hello", timeout=5.0)
            result = Result.from_result_message(result_message, rate_limit)

            assert result_message["type"] == "result"
            assert result.text == "hello"
            assert result.is_error is False
            assert result.subtype == "success"
            assert result.session_id == worker._session_id
            assert result.usage["input_tokens"] == 3
            assert result.cost_usd == 0.0
            assert result.rate_limit is None
            assert worker.alive is True
        finally:
            await worker.retire()

    run(scenario())


def test_tui_worker_preserves_multiline_prompt() -> None:
    async def scenario() -> None:
        worker = await spawn_fake()
        prompt = "first line\nsecond line"
        try:
            result_message, _rate_limit = await worker.ask(prompt, timeout=5.0)

            assert result_message["result"] == prompt
        finally:
            await worker.retire()

    run(scenario())


def test_tui_worker_accepts_trust_dialog() -> None:
    async def scenario() -> None:
        worker = await spawn_fake(env={"FAKE_TUI_TRUST": "1"})
        try:
            result_message, _rate_limit = await worker.ask("trusted", timeout=5.0)

            assert result_message["result"] == "trusted"
        finally:
            await worker.retire()

    run(scenario())


def test_tui_worker_waits_for_session_start_hook_before_ask() -> None:
    async def scenario() -> None:
        worker = await spawn_fake(env={"FAKE_TUI_SLOW_START": "2"})
        try:
            result_message, _rate_limit = await worker.ask("after-start", timeout=5.0)

            assert result_message["result"] == "after-start"
        finally:
            await worker.retire()

    run(scenario())


def test_tui_worker_startup_exit_raises_worker_start_error() -> None:
    async def scenario() -> None:
        with pytest.raises(WorkerStartError) as raised:
            await spawn_fake(env={"FAKE_TUI_STARTUP": "exit2"})

        assert "fake-tui startup exit2" in raised.value.stderr_tail

    run(scenario())


def test_tui_worker_startup_auth_error_carries_login_tail() -> None:
    async def scenario() -> None:
        with pytest.raises(WorkerStartError) as raised:
            await spawn_fake(env={"FAKE_TUI_STARTUP": "autherr"})

        assert "Please run /login" in raised.value.stderr_tail

    run(scenario())


def test_tui_worker_timeout_kills_process_group() -> None:
    async def scenario() -> None:
        worker = await spawn_fake(argv=shell_wrapped_fake())
        pgid = os.getpgid(worker.process.pid)

        with pytest.raises(AskTimeout):
            await worker.ask("SLEEP:30", timeout=1.0)

        assert worker.alive is False
        await assert_process_group_gone(pgid)

    run(scenario())


def test_tui_worker_large_prompt_timeout_does_not_block_event_loop() -> None:
    async def scenario() -> None:
        worker = await spawn_fake(env={"FAKE_TUI_STALL": "1"})
        pgid = os.getpgid(worker.process.pid)
        started = time.monotonic()

        with pytest.raises(AskTimeout):
            await worker.ask("x" * 200_000, timeout=2.0)

        assert time.monotonic() - started < 3.5
        assert worker.alive is False
        await assert_process_group_gone(pgid)

    run(scenario())


def test_tui_worker_no_hook_times_out_and_kills_process_group() -> None:
    async def scenario() -> None:
        worker = await spawn_fake()
        pgid = os.getpgid(worker.process.pid)

        with pytest.raises(AskTimeout):
            await worker.ask("NOHOOK", timeout=1.0)

        assert worker.alive is False
        await assert_process_group_gone(pgid)

    run(scenario())


def test_tui_worker_die_before_hook_raises_worker_crash_error() -> None:
    async def scenario() -> None:
        worker = await spawn_fake()
        try:
            with pytest.raises(WorkerCrashError) as raised:
                await worker.ask("DIE", timeout=5.0)

            assert "fake-tui dying" in raised.value.stderr_tail
        finally:
            await worker.kill()

    run(scenario())


def test_tui_worker_retire_removes_tempdir_and_process_group() -> None:
    async def scenario() -> None:
        worker = await spawn_fake()
        tempdir = Path(worker._tempdir)
        pgid = os.getpgid(worker.process.pid)

        result_message, _rate_limit = await worker.ask("hello", timeout=5.0)
        started = time.monotonic()
        await worker.retire()

        assert result_message["type"] == "result"
        assert time.monotonic() - started < 1.5
        assert not tempdir.exists()
        assert worker.alive is False
        await assert_process_group_gone(pgid)
        await assert_no_tui_reader_threads()

    run(scenario())


def test_tui_worker_retire_signals_straggler_child_after_clean_leader_exit() -> None:
    async def scenario() -> None:
        worker = await spawn_fake()
        pgid = os.getpgid(worker.process.pid)

        result_message, _rate_limit = await worker.ask("EXIT_WITH_CHILD", timeout=5.0)
        await worker.retire()

        assert result_message["result"] == "EXIT_WITH_CHILD"
        assert pgid not in _LIVE_PGIDS
        await assert_process_group_gone(pgid)
        await assert_no_tui_reader_threads()

    run(scenario())


def test_tui_worker_concurrent_second_ask_raises_runtime_error() -> None:
    async def scenario() -> None:
        worker = await spawn_fake()
        first = asyncio.create_task(worker.ask("SLEEP:2", timeout=5.0))
        await asyncio.sleep(0.3)

        try:
            with pytest.raises(RuntimeError):
                await worker.ask("second", timeout=5.0)
        finally:
            await worker.kill()
            with suppress(WorkerCrashError, AskTimeout):
                await first

    run(scenario())


def test_tui_worker_cancellation_mid_ask_kills_process_group() -> None:
    async def scenario() -> None:
        worker = await spawn_fake()
        pgid = os.getpgid(worker.process.pid)
        task = asyncio.create_task(worker.ask("SLEEP:30", timeout=None))
        await asyncio.sleep(0.3)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await assert_process_group_gone(pgid)

    run(scenario())


def test_tui_worker_waits_for_complete_hook_line() -> None:
    async def scenario() -> None:
        worker = await spawn_fake(env={"FAKE_TUI_PARTIAL_HOOK": "1"})
        started = time.monotonic()
        try:
            result_message, _rate_limit = await worker.ask("partial", timeout=5.0)

            assert result_message["result"] == "partial"
            assert time.monotonic() - started >= 0.25
        finally:
            await worker.retire()

    run(scenario())


def test_tui_worker_skips_junk_hook_lines() -> None:
    async def scenario() -> None:
        worker = await spawn_fake(env={"FAKE_TUI_JUNK_HOOK": "1"})
        try:
            result_message, _rate_limit = await worker.ask("after-junk", timeout=5.0)

            assert result_message["result"] == "after-junk"
        finally:
            await worker.retire()

    run(scenario())
