from __future__ import annotations

import asyncio
from contextlib import suppress
import os
import shlex
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from claude_pool import AskTimeout, Result, WorkerCrashError, _TailBuffer, _Worker


ROOT = Path(__file__).resolve().parents[1]
FAKE = ROOT / "tests" / "fake_claude.py"


def run(coro: Any) -> Any:
    return asyncio.run(coro)


def fake_argv() -> list[str]:
    return [
        sys.executable,
        str(FAKE),
        "-p",
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
        "--verbose",
    ]


async def spawn_fake(
    *,
    env: dict[str, str] | None = None,
    argv: list[str] | None = None,
) -> _Worker:
    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    return await _Worker.spawn(argv or fake_argv(), env=full_env)


async def assert_process_group_gone(pgid: int) -> None:
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"process group {pgid} still exists")


def test_worker_happy_echo_and_result_conversion() -> None:
    async def scenario() -> None:
        worker = await spawn_fake()
        try:
            result_message, rate_limit = await worker.ask("hello", timeout=5.0)
            result = Result.from_result_message(result_message, rate_limit)

            assert result_message["type"] == "result"
            assert result.text == "hello"
            assert result.is_error is False
            assert result.subtype == "success"
            assert result.session_id.startswith("fake-")
            assert result.usage["cache_creation_input_tokens"] == 0
            assert result.cost_usd == 0.001
            assert result.duration_ms == 12
            assert result.rate_limit is None
            assert result.raw is result_message
            assert worker.alive is True
        finally:
            await worker.retire()

    run(scenario())


def test_worker_reads_big_result_line_above_default_asyncio_limit() -> None:
    async def scenario() -> None:
        worker = await spawn_fake()
        try:
            result_message, _rate_limit = await worker.ask("BIGLINE:200000", timeout=5.0)

            assert len(result_message["result"]) == 200000
        finally:
            await worker.retire()

    run(scenario())


def test_worker_tolerates_garbage_and_unknown_messages() -> None:
    async def scenario() -> None:
        worker = await spawn_fake()
        try:
            result_message, rate_limit = await worker.ask("GARBAGE", timeout=5.0)

            assert result_message["type"] == "result"
            assert result_message["result"] == "GARBAGE"
            assert rate_limit is None
        finally:
            await worker.retire()

    run(scenario())


def test_worker_returns_latest_rate_limit_event() -> None:
    async def scenario() -> None:
        worker = await spawn_fake()
        try:
            result_message, rate_limit = await worker.ask("RATELIMIT", timeout=5.0)

            assert result_message["type"] == "result"
            assert result_message["is_error"] is False
            assert rate_limit is not None
            assert rate_limit["type"] == "rate_limit_event"
            assert rate_limit["retry_after_ms"] == 250
        finally:
            await worker.retire()

    run(scenario())


def test_worker_returns_error_result_without_exception() -> None:
    async def scenario() -> None:
        worker = await spawn_fake()
        try:
            result_message, rate_limit = await worker.ask("ERROR", timeout=5.0)

            assert rate_limit is None
            assert result_message["is_error"] is True
            assert result_message["subtype"] == "error_during_execution"
        finally:
            await worker.retire()

    run(scenario())


def test_worker_crash_before_result_carries_stderr_tail() -> None:
    async def scenario() -> None:
        worker = await spawn_fake()
        try:
            try:
                await worker.ask("DIE", timeout=5.0)
            except WorkerCrashError as exc:
                assert "fake claude died" in exc.stderr_tail
            else:
                raise AssertionError("expected WorkerCrashError")
        finally:
            await worker.kill()

    run(scenario())


def test_worker_timeout_kills_entire_process_group() -> None:
    async def scenario() -> None:
        quoted = " ".join(shlex.quote(arg) for arg in fake_argv())
        argv = ["/bin/sh", "-c", f"sleep 30 & exec {quoted}"]
        worker = await spawn_fake(argv=argv)
        pgid = os.getpgid(worker.process.pid)

        try:
            try:
                await worker.ask("SLEEP:30", timeout=1.0)
            except AskTimeout:
                pass
            else:
                raise AssertionError("expected AskTimeout")
        finally:
            await worker.kill()

        assert worker.alive is False
        await assert_process_group_gone(pgid)

    run(scenario())


def test_worker_cancellation_mid_ask_kills_process_group() -> None:
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


def test_worker_concurrent_second_ask_raises_runtime_error() -> None:
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


def test_worker_startup_auth_error_carries_login_stderr() -> None:
    async def scenario() -> None:
        worker = await spawn_fake(env={"FAKE_CLAUDE_STARTUP": "autherr"})
        try:
            try:
                await worker.ask("hello", timeout=5.0)
            except WorkerCrashError as exc:
                assert "Please run /login" in exc.stderr_tail
            else:
                raise AssertionError("expected WorkerCrashError")
        finally:
            await worker.kill()

    run(scenario())


def test_worker_crash_with_pipe_holding_child_raises_worker_crash_error() -> None:
    async def scenario() -> None:
        quoted = " ".join(shlex.quote(arg) for arg in fake_argv())
        argv = ["/bin/sh", "-c", f"sleep 30 0<&- & exec {quoted}"]
        worker = await spawn_fake(argv=argv, env={"FAKE_CLAUDE_STARTUP": "exit2"})
        pgid = os.getpgid(worker.process.pid)
        await asyncio.sleep(0.3)

        with pytest.raises(WorkerCrashError):
            await worker.ask("hello", timeout=None)
        await assert_process_group_gone(pgid)

    run(scenario())


def test_worker_retire_kills_pipe_holding_child() -> None:
    async def scenario() -> None:
        quoted = " ".join(shlex.quote(arg) for arg in fake_argv())
        argv = ["/bin/sh", "-c", f"sleep 30 & exec {quoted}"]
        worker = await spawn_fake(argv=argv)
        pgid = os.getpgid(worker.process.pid)

        result_message, _rate_limit = await worker.ask("hello", timeout=5.0)
        start = time.monotonic()
        await worker.retire()

        assert result_message["type"] == "result"
        assert time.monotonic() - start < 5.0
        await assert_process_group_gone(pgid)

    run(scenario())


def test_worker_oversized_output_line_crashes_worker_cleanly() -> None:
    async def scenario() -> None:
        worker = await spawn_fake()
        try:
            with pytest.raises(WorkerCrashError, match="oversized output line"):
                await worker.ask("BIGLINE:11000000", timeout=10.0)
            assert worker.alive is False
        finally:
            await worker.kill()

    run(scenario())


def test_worker_retire_after_success_exits_cleanly() -> None:
    async def scenario() -> None:
        worker = await spawn_fake()
        result_message, _rate_limit = await worker.ask("hello", timeout=5.0)

        assert result_message["type"] == "result"
        await worker.retire()
        assert worker.process.returncode == 0
        assert worker.alive is False

    run(scenario())


def test_tail_buffer_keeps_last_64kib() -> None:
    buffer = _TailBuffer(64 * 1024)
    buffer.append(b"a" * (64 * 1024))
    buffer.append(b"b" * 100)

    text = buffer.text()
    assert len(text.encode()) == 64 * 1024
    assert text == ("a" * ((64 * 1024) - 100)) + ("b" * 100)
