"""Warm persistent workers for running the Claude Code CLI through stream-json I/O."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from contextlib import AbstractAsyncContextManager, suppress
from dataclasses import dataclass
import json
import os
import signal
import sys
import time
from types import TracebackType
from typing import Any

_STDOUT_LIMIT = 10 * 2**20
_STDERR_LIMIT = 64 * 1024


@dataclass(frozen=True)
class Result:
    """A completed Claude turn returned by a pool ask or session send.

    Attributes mirror the CLI result message. ``raw`` contains the unmodified
    result object, and ``rate_limit`` contains the latest observed rate-limit
    event for the turn when one was emitted.
    """

    text: str
    is_error: bool
    subtype: str
    session_id: str
    usage: Mapping[str, Any]
    cost_usd: float
    duration_ms: int
    rate_limit: Mapping[str, Any] | None
    raw: Mapping[str, Any]

    @classmethod
    def from_result_message(
        cls,
        message: Mapping[str, Any],
        rate_limit: Mapping[str, Any] | None,
    ) -> Result:
        """Create a ``Result`` from one Claude CLI ``result`` stream message.

        ``message`` is retained unchanged in ``raw``. Missing or incorrectly
        typed scalar fields are normalized to the stable public API defaults.
        """
        text = message.get("result")
        usage = message.get("usage")
        subtype = message.get("subtype")
        session_id = message.get("session_id")
        cost_usd = message.get("total_cost_usd")
        duration_ms = message.get("duration_ms")
        return cls(
            text=text if isinstance(text, str) else "",
            is_error=bool(message.get("is_error")),
            subtype=subtype if isinstance(subtype, str) else "",
            session_id=session_id if isinstance(session_id, str) else "",
            usage=usage if isinstance(usage, Mapping) else {},
            cost_usd=float(cost_usd) if isinstance(cost_usd, int | float) else 0.0,
            duration_ms=duration_ms if isinstance(duration_ms, int) else 0,
            rate_limit=rate_limit,
            raw=message,
        )


class _TailBuffer:
    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._data = bytearray()

    def append(self, chunk: bytes) -> None:
        self._data.extend(chunk)
        if len(self._data) > self._limit:
            del self._data[: len(self._data) - self._limit]

    def text(self) -> str:
        return self._data.decode(errors="replace")


class _Worker:
    def __init__(self, process: asyncio.subprocess.Process) -> None:
        self.process = process
        self._pgid = process.pid
        self.spawned_at = time.monotonic()
        self.idle_since = self.spawned_at
        self._stderr = _TailBuffer(_STDERR_LIMIT)
        self._stderr_task = asyncio.create_task(self._drain_stderr())
        self._ask_lock = asyncio.Lock()
        self._killed = False

    @classmethod
    async def spawn(
        cls,
        argv: Sequence[str],
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> _Worker:
        process = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=cwd,
            env=dict(env) if env is not None else None,
            start_new_session=True,
            limit=_STDOUT_LIMIT,
        )
        return cls(process)

    @property
    def stderr_tail(self) -> str:
        return self._stderr.text()

    @property
    def alive(self) -> bool:
        return self.process.returncode is None

    async def ask(
        self,
        prompt: str,
        timeout: float | None,
    ) -> tuple[dict[str, Any], dict[str, Any] | None]:
        if self._ask_lock.locked():
            raise RuntimeError("worker already has an in-flight ask")

        async with self._ask_lock:
            try:
                return await asyncio.wait_for(self._ask(prompt), timeout)
            except asyncio.TimeoutError as exc:
                await self.kill()
                raise AskTimeout("ask timed out") from exc
            except asyncio.CancelledError:
                await asyncio.shield(self.kill())
                raise

    async def retire(self) -> None:
        try:
            try:
                await asyncio.wait_for(self._close_stdin(), timeout=2.0)
            except asyncio.TimeoutError:
                await self.kill()
                return
            try:
                await asyncio.wait_for(self.process.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                await self.kill()
                return
            await self._finish_stderr_task(cancel=False)
        except asyncio.CancelledError:
            await asyncio.shield(self.kill())
            raise

    async def kill(self) -> None:
        if not self._killed:
            self._killed = True
            try:
                os.killpg(self._pgid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        try:
            await asyncio.wait_for(self.process.wait(), timeout=2.0)
        except asyncio.TimeoutError:
            pass
        await self._finish_stderr_task(cancel=True)

    async def _ask(self, prompt: str) -> tuple[dict[str, Any], dict[str, Any] | None]:
        if self.process.stdin is None or self.process.stdout is None:
            raise WorkerCrashError("worker pipes are unavailable", stderr_tail=self.stderr_tail)

        payload = {
            "type": "user",
            "message": {
                "role": "user",
                "content": [{"type": "text", "text": prompt}],
            },
        }
        line = json.dumps(payload, separators=(",", ":")).encode() + b"\n"
        try:
            self.process.stdin.write(line)
            await self.process.stdin.drain()
        except (BrokenPipeError, ConnectionResetError) as exc:
            await self._wait_after_crash()
            await self._finish_stderr_task(cancel=False)
            raise WorkerCrashError(
                "worker exited before accepting input", self.stderr_tail
            ) from exc

        rate_limit: dict[str, Any] | None = None
        while True:
            try:
                raw = await self.process.stdout.readline()
            except (ValueError, asyncio.LimitOverrunError) as exc:
                await self.kill()
                raise WorkerCrashError(
                    "oversized output line", stderr_tail=self.stderr_tail
                ) from exc
            if raw == b"":
                await self._wait_after_crash()
                await self._finish_stderr_task(cancel=False)
                raise WorkerCrashError("worker exited before result", stderr_tail=self.stderr_tail)

            try:
                message = json.loads(raw.decode(errors="replace"))
            except json.JSONDecodeError:
                continue
            if not isinstance(message, dict):
                continue

            message_type = message.get("type")
            if message_type == "rate_limit_event":
                rate_limit = message
            elif message_type == "result":
                self.idle_since = time.monotonic()
                return message, rate_limit

    async def _drain_stderr(self) -> None:
        if self.process.stderr is None:
            return
        while True:
            chunk = await self.process.stderr.read(4096)
            if not chunk:
                return
            self._stderr.append(chunk)

    async def _close_stdin(self) -> None:
        if self.process.stdin is None or self.process.stdin.is_closing():
            return
        self.process.stdin.close()
        with suppress(BrokenPipeError, ConnectionResetError):
            await self.process.stdin.wait_closed()

    async def _wait_after_crash(self) -> None:
        try:
            await asyncio.wait_for(self.process.wait(), timeout=2.0)
        except asyncio.TimeoutError:
            await self.kill()

    async def _finish_stderr_task(self, *, cancel: bool) -> None:
        if cancel:
            await self._cancel_stderr_task()
            return

        try:
            await asyncio.wait_for(self._stderr_task, timeout=1.0)
        except asyncio.TimeoutError:
            await self._cancel_stderr_task()
        except asyncio.CancelledError:
            await self._cancel_stderr_task()
            raise

    async def _cancel_stderr_task(self) -> None:
        if not self._stderr_task.done():
            self._stderr_task.cancel()
        with suppress(asyncio.CancelledError):
            await self._stderr_task


class ClaudePoolError(Exception):
    """Base class for all claude-pool runtime failures."""


class WorkerStartError(ClaudePoolError):
    """Raised when a worker process exits or fails before it can be used.

    ``stderr_tail`` contains the bounded trailing stderr captured from the
    worker process.
    """

    def __init__(self, message: str, stderr_tail: str = "") -> None:
        super().__init__(message)
        self.stderr_tail = stderr_tail


class WorkerCrashError(ClaudePoolError):
    """Raised when a worker exits before producing a result for a turn.

    ``stderr_tail`` contains the bounded trailing stderr captured from the
    worker process.
    """

    def __init__(self, message: str, stderr_tail: str = "") -> None:
        super().__init__(message)
        self.stderr_tail = stderr_tail


class AskTimeout(ClaudePoolError):
    """Raised when a turn exceeds its configured timeout."""


class PoolClosed(ClaudePoolError):
    """Raised when work is requested from a closed pool or session."""


class Session:
    """An explicit multi-turn conversation checked out from a ``ClaudePool``.

    Instances are created by ``ClaudePool.session()``. ``send`` returns one
    ``Result`` per prompt and may raise ``ClaudePoolError`` subclasses for
    worker failures, timeouts, or lifecycle violations.
    """

    async def send(self, prompt: str, timeout: float | None = None) -> Result:
        """Send one prompt on this session and return the completed result.

        Raises ``AskTimeout`` if the timeout elapses, ``WorkerCrashError`` if
        the underlying worker exits mid-turn, and ``PoolClosed`` when the
        session is no longer usable.
        """
        raise NotImplementedError

    async def __aenter__(self) -> Session:
        """Enter this async session context and return the session."""
        raise NotImplementedError

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """Release this session's worker when leaving an async context."""
        raise NotImplementedError


class ClaudePool:
    """A pool of warm Claude Code CLI workers for one-shot asks and sessions.

    Constructor arguments map directly to Claude CLI profile flags and pool
    lifecycle settings. Work methods return ``Result`` or raise
    ``ClaudePoolError`` subclasses.
    """

    def __init__(
        self,
        model: str | None = None,
        effort: str | None = None,
        system_prompt: str | None = None,
        allowed_tools: Sequence[str] | None = None,
        disallowed_tools: Sequence[str] | None = None,
        permission_mode: str | None = None,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        claude_bin: str = "claude",
        extra_args: Sequence[str] | None = None,
        warm: int = 1,
        max_workers: int = 4,
        max_idle: float = 900.0,
        default_timeout: float = 600.0,
    ) -> None:
        """Create a pool configuration without starting workers.

        Raises ``NotImplementedError`` until worker behavior is implemented.
        """
        raise NotImplementedError

    async def ask(self, prompt: str, timeout: float | None = None) -> Result:
        """Send one prompt in a fresh context and return the completed result.

        Raises ``AskTimeout`` if the timeout elapses, ``WorkerCrashError`` if a
        worker exits mid-turn, and ``PoolClosed`` when the pool has closed.
        """
        raise NotImplementedError

    def session(self) -> AbstractAsyncContextManager[Session]:
        """Return an async context manager yielding a multi-turn ``Session``.

        Raises ``PoolClosed`` when the pool has closed.
        """
        raise NotImplementedError

    async def aclose(self) -> None:
        """Close the pool and release all workers.

        After this method completes, new work raises ``PoolClosed``.
        """
        raise NotImplementedError

    def ask_sync(self, prompt: str, timeout: float | None = None) -> Result:
        """Synchronously send one prompt in a fresh context.

        Raises the same exceptions as ``ask``.
        """
        raise NotImplementedError

    def close(self) -> None:
        """Synchronously close the pool and release all workers."""
        raise NotImplementedError

    async def __aenter__(self) -> ClaudePool:
        """Enter this async pool context and return the pool."""
        raise NotImplementedError

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """Close the pool when leaving an async context."""
        raise NotImplementedError

    def __enter__(self) -> ClaudePool:
        """Enter this synchronous pool context and return the pool."""
        raise NotImplementedError

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """Close the pool when leaving a synchronous context."""
        raise NotImplementedError


def main(argv: Sequence[str] | None = None) -> int:
    """Run the claude-pool command-line interface and return a process status."""
    del argv
    sys.stderr.write("not yet implemented\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
