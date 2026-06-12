"""Warm persistent workers for running the Claude Code CLI through stream-json I/O."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
import sys
from types import TracebackType
from typing import Any


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
