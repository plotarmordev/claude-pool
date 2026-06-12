"""Warm persistent workers for running the Claude Code CLI through stream-json I/O."""

from __future__ import annotations

import atexit
import asyncio
import concurrent.futures
from collections import deque
from collections.abc import Coroutine, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, AbstractContextManager, suppress
from dataclasses import dataclass
import json
import logging
import os
import signal
import sys
import threading
import time
from types import TracebackType
from typing import Any

_STDOUT_LIMIT = 10 * 2**20
_STDERR_LIMIT = 64 * 1024
_LIVE_PGIDS: set[int] = set()
logger = logging.getLogger("claude_pool")


def _kill_live_process_groups() -> None:
    for pgid in tuple(_LIVE_PGIDS):
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass


atexit.register(_kill_live_process_groups)


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
        _LIVE_PGIDS.add(self._pgid)
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
            self._signal_process_group()
            await self._finish_stderr_task(cancel=False)
        except asyncio.CancelledError:
            await asyncio.shield(self.kill())
            raise

    async def kill(self) -> None:
        self._signal_process_group()
        try:
            await asyncio.wait_for(self.process.wait(), timeout=2.0)
        except asyncio.TimeoutError:
            pass
        await self._finish_stderr_task(cancel=True)

    def _signal_process_group(self) -> None:
        if not self._killed:
            self._killed = True
            try:
                os.killpg(self._pgid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            _LIVE_PGIDS.discard(self._pgid)

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
        else:
            self._signal_process_group()

    async def _finish_stderr_task(self, *, cancel: bool) -> None:
        if self._stderr_task.done():
            with suppress(asyncio.CancelledError):
                await self._stderr_task
            return

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

    def __init__(self, pool: ClaudePool) -> None:
        """Create an unentered session bound to ``pool``."""
        self._pool = pool
        self._worker: _Worker | None = None
        self._send_lock = asyncio.Lock()
        self._entered = False
        self._exited = False
        self._usable = False
        self._semaphore_acquired = False

    async def send(self, prompt: str, timeout: float | None = None) -> Result:
        """Send one prompt on this session and return the completed result.

        Raises ``AskTimeout`` if the timeout elapses, ``WorkerCrashError`` if
        the underlying worker exits mid-turn, and ``ClaudePoolError`` when the
        session is no longer usable.
        """
        if not self._usable or self._worker is None:
            raise ClaudePoolError("session closed")

        async with self._send_lock:
            if not self._usable or self._worker is None:
                raise ClaudePoolError("session closed")
            ask_timeout = self._pool._default_timeout if timeout is None else timeout
            try:
                result_message, rate_limit = await self._worker.ask(prompt, ask_timeout)
            except (WorkerCrashError, AskTimeout) as exc:
                self._usable = False
                if self._exited:
                    raise ClaudePoolError("session closed") from exc
                raise
            except asyncio.CancelledError:
                self._usable = False
                raise
            if self._exited:
                self._usable = False
                raise ClaudePoolError("session closed")
            return Result.from_result_message(result_message, rate_limit)

    async def __aenter__(self) -> Session:
        """Enter this async session context and return the session."""
        if self._entered or self._exited:
            raise ClaudePoolError("session closed")
        self._entered = True
        if self._pool._closed:
            raise PoolClosed("pool is closed")

        self._pool._ensure_started()
        await self._pool._semaphore.acquire()
        self._semaphore_acquired = True
        if self._pool._closed:
            self._release_semaphore()
            raise PoolClosed("pool is closed")

        try:
            worker, _was_warm = await self._pool._checkout_worker()
        except BaseException:
            self._release_semaphore()
            raise

        self._worker = worker
        self._usable = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """Release this session's worker when leaving an async context."""
        del exc_type, exc, tb
        if self._exited:
            return None

        self._exited = True
        self._usable = False
        worker = self._worker
        self._worker = None
        if worker is not None:
            self._pool._reap(worker)
            self._pool._nudge_replenisher()
        self._release_semaphore()
        return None

    def _release_semaphore(self) -> None:
        if self._semaphore_acquired:
            self._semaphore_acquired = False
            self._pool._semaphore.release()


class _SyncSession:
    def __init__(self, pool: ClaudePool) -> None:
        self._pool = pool
        self._session = Session(pool)
        self._entered = False
        self._closed = False

    def send(self, prompt: str, timeout: float | None = None) -> Result:
        if not self._entered or self._closed:
            raise ClaudePoolError("session closed")
        return self._pool._run_sync(self._session.send(prompt, timeout))

    def __enter__(self) -> _SyncSession:
        if self._entered or self._closed:
            raise ClaudePoolError("session closed")
        self._pool._run_sync(self._session.__aenter__())
        self._entered = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        if self._closed:
            return None
        self._closed = True
        self._pool._run_sync(self._session.__aexit__(exc_type, exc, tb), cleanup=True)
        return None


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
        """Create a pool configuration without starting workers."""
        self._argv = self._build_argv(
            claude_bin=claude_bin,
            model=model,
            effort=effort,
            system_prompt=system_prompt,
            allowed_tools=allowed_tools,
            disallowed_tools=disallowed_tools,
            permission_mode=permission_mode,
            extra_args=extra_args,
        )
        self._cwd = cwd
        self._env = os.environ.copy()
        if env is not None:
            self._env.update(env)
        self._warm_target = max(0, warm)
        self._max_workers = max(1, max_workers)
        self._max_idle = max_idle
        self._default_timeout = default_timeout
        self._warm: deque[_Worker] = deque()
        self._semaphore = asyncio.Semaphore(self._max_workers)
        self._closed = False
        self._started = False
        self._spawns_in_flight = 0
        self._spawn_cooldown_until = 0.0
        self._replenish_event = asyncio.Event()
        self._replenish_lock = asyncio.Lock()
        self._replenisher_task: asyncio.Task[None] | None = None
        self._sweeper_task: asyncio.Task[None] | None = None
        self._reaper_tasks: set[asyncio.Task[None]] = set()
        self._mode: str | None = None
        self._sync_loop: asyncio.AbstractEventLoop | None = None
        self._sync_thread: threading.Thread | None = None
        self._sync_mutex = threading.Lock()
        self._sync_stopping = False
        self._sync_stopped = threading.Event()
        self._sync_inflight: set[concurrent.futures.Future[Any]] = set()

    @staticmethod
    def _build_argv(
        *,
        claude_bin: str,
        model: str | None,
        effort: str | None,
        system_prompt: str | None,
        allowed_tools: Sequence[str] | None,
        disallowed_tools: Sequence[str] | None,
        permission_mode: str | None,
        extra_args: Sequence[str] | None,
    ) -> list[str]:
        argv = [
            claude_bin,
            "-p",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
        ]
        if model is not None:
            argv.extend(["--model", model])
        if effort is not None:
            argv.extend(["--effort", effort])
        if system_prompt is not None:
            argv.extend(["--system-prompt", system_prompt])
        if allowed_tools is not None:
            argv.extend(["--allowedTools", ",".join(allowed_tools)])
        if disallowed_tools is not None:
            argv.extend(["--disallowedTools", ",".join(disallowed_tools)])
        if permission_mode is not None:
            argv.extend(["--permission-mode", permission_mode])
        if extra_args is not None:
            argv.extend(extra_args)
        return argv

    async def start(self) -> None:
        """Start background pool maintenance and pre-warm configured workers.

        Raises ``PoolClosed`` when called after the pool has closed.
        Calling this async method fixes the pool to async mode; later sync API
        calls on the same pool raise ``ClaudePoolError``.
        """
        self._use_async()
        await self._start()

    async def _start(self) -> None:
        if self._closed:
            raise PoolClosed("pool is closed")
        self._ensure_started()
        await self._replenish_once()

    def _ensure_started(self) -> None:
        if not self._started:
            self._started = True
            self._replenisher_task = asyncio.create_task(self._replenisher())
            self._sweeper_task = asyncio.create_task(self._sweeper())
            self._replenish_event.set()

    async def ask(self, prompt: str, timeout: float | None = None) -> Result:
        """Send one prompt in a fresh context and return the completed result.

        Raises ``AskTimeout`` if the timeout elapses, ``WorkerCrashError`` if a
        worker exits mid-turn, and ``PoolClosed`` when the pool has closed.
        Calling this async method fixes the pool to async mode; later sync API
        calls on the same pool raise ``ClaudePoolError``.
        """
        self._use_async()
        return await self._ask(prompt, timeout)

    async def _ask(self, prompt: str, timeout: float | None = None) -> Result:
        if self._closed:
            raise PoolClosed("pool is closed")
        self._ensure_started()
        ask_timeout = self._default_timeout if timeout is None else timeout
        await self._semaphore.acquire()
        if self._closed:
            self._semaphore.release()
            raise PoolClosed("pool is closed")
        try:
            return await self._ask_with_worker(prompt, ask_timeout)
        finally:
            self._semaphore.release()

    def session(self) -> AbstractAsyncContextManager[Session]:
        """Return an async context manager yielding a multi-turn ``Session``.

        Raises ``PoolClosed`` when the pool has closed.
        Calling this async API fixes the pool to async mode; later sync API
        calls on the same pool raise ``ClaudePoolError``.
        """
        self._use_async()
        if self._closed:
            raise PoolClosed("pool is closed")
        return Session(self)

    async def aclose(self) -> None:
        """Close the pool and release all workers.

        After this method completes, new work raises ``PoolClosed``.
        Calling this async method fixes the pool to async mode; later sync API
        calls on the same pool raise ``ClaudePoolError``.
        """
        self._use_async()
        await self._aclose()

    async def _aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        tasks = [task for task in (self._replenisher_task, self._sweeper_task) if task is not None]
        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):
                await task
        acquired = 0
        for _ in range(self._max_workers):
            await self._semaphore.acquire()
            acquired += 1
        async with self._replenish_lock:
            warm = list(self._warm)
            self._warm.clear()
        for worker in warm:
            self._reap(worker)
        if self._reaper_tasks:
            await asyncio.gather(*self._reaper_tasks, return_exceptions=True)
        for _ in range(acquired):
            self._semaphore.release()

    def ask_sync(self, prompt: str, timeout: float | None = None) -> Result:
        """Synchronously send one prompt in a fresh context.

        Raises the same exceptions as ``ask``. Calling this sync method fixes
        the pool to sync mode; later async API calls on the same pool raise
        ``ClaudePoolError``.
        """
        self._use_sync()
        if self._closed:
            raise PoolClosed("pool is closed")
        return self._run_sync(self._ask(prompt, timeout))

    def session_sync(self) -> AbstractContextManager[_SyncSession]:
        """Return a sync context manager yielding a multi-turn session wrapper.

        The returned object has a synchronous ``send(prompt, timeout=None)``
        method mirroring ``Session.send``. Calling this sync method fixes the
        pool to sync mode; later async API calls on the same pool raise
        ``ClaudePoolError``.
        """
        self._use_sync()
        if self._closed:
            raise PoolClosed("pool is closed")
        return _SyncSession(self)

    def close(self) -> None:
        """Synchronously close the pool and release all workers.

        Calling this sync method fixes the pool to sync mode; later async API
        calls on the same pool raise ``ClaudePoolError``. Calling ``close`` on
        a fresh pool that never started the sync loop only marks it closed.
        """
        self._use_sync()
        with self._sync_mutex:
            if self._sync_stopping:
                stopped = self._sync_stopped
                wait_for_existing_close = True
                loop = None
            else:
                self._sync_stopping = True
                self._sync_stopped.clear()
                stopped = self._sync_stopped
                wait_for_existing_close = False
                loop = self._sync_loop
                if loop is None:
                    self._closed = True
                    self._sync_stopped.set()
                    return

        if wait_for_existing_close:
            stopped.wait(timeout=30.0)
            return

        try:
            close_future = asyncio.run_coroutine_threadsafe(self._aclose(), loop)
            close_future.result()
            with self._sync_mutex:
                inflight = tuple(self._sync_inflight)
            concurrent.futures.wait(inflight, timeout=10.0)
            with self._sync_mutex:
                self._stop_sync_loop_locked()
        except BaseException:
            with self._sync_mutex:
                self._sync_stopping = False
            raise
        finally:
            stopped.set()

    def _use_async(self) -> None:
        with self._sync_mutex:
            if self._mode is None:
                self._mode = "async"
            elif self._mode != "async":
                raise ClaudePoolError(
                    "pool already used through sync API; create a separate pool for async use"
                )

    def _use_sync(self) -> None:
        with self._sync_mutex:
            if self._mode is None:
                self._mode = "sync"
            elif self._mode != "sync":
                raise ClaudePoolError(
                    "pool already used through async API; create a separate pool for sync use"
                )

    def _run_sync(self, coro: Coroutine[Any, Any, Any], *, cleanup: bool = False) -> Any:
        with self._sync_mutex:
            if self._sync_stopping and not cleanup:
                coro.close()
                raise PoolClosed("pool is closed")
            if self._sync_stopping and self._sync_loop is None:
                coro.close()
                raise PoolClosed("pool is closed")
            # Cleanup submissions may release resources that the in-flight close is waiting on.
            loop = self._ensure_sync_loop_locked()
            future = asyncio.run_coroutine_threadsafe(coro, loop)
            self._sync_inflight.add(future)
            future.add_done_callback(lambda done: self._sync_inflight.discard(done))
        return future.result()

    def _ensure_sync_loop(self) -> asyncio.AbstractEventLoop:
        with self._sync_mutex:
            return self._ensure_sync_loop_locked()

    def _ensure_sync_loop_locked(self) -> asyncio.AbstractEventLoop:
        if self._sync_loop is not None:
            return self._sync_loop

        self._sync_stopped.clear()
        loop = asyncio.new_event_loop()
        ready = threading.Event()

        def run_loop() -> None:
            asyncio.set_event_loop(loop)
            ready.set()
            loop.run_forever()

        thread = threading.Thread(
            target=run_loop,
            name="claude-pool-sync-loop",
            daemon=True,
        )
        self._sync_loop = loop
        self._sync_thread = thread
        thread.start()
        ready.wait()
        return loop

    def _stop_sync_loop_locked(self) -> None:
        loop = self._sync_loop
        thread = self._sync_thread
        if loop is None:
            return

        loop.call_soon_threadsafe(loop.stop)
        if thread is not None:
            thread.join(timeout=5.0)
            if thread.is_alive():
                raise ClaudePoolError("sync event loop thread did not stop")
        loop.close()
        self._sync_loop = None
        self._sync_thread = None

    async def __aenter__(self) -> ClaudePool:
        """Enter this async pool context and return the pool."""
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """Close the pool when leaving an async context."""
        await self.aclose()
        return None

    async def _ask_with_worker(self, prompt: str, timeout: float | None) -> Result:
        retried_warm_crash = False
        force_cold = False
        while True:
            if force_cold:
                worker, was_warm = await self._spawn_worker(), False
            else:
                worker, was_warm = await self._checkout_worker()
            try:
                result_message, rate_limit = await worker.ask(prompt, timeout)
                return Result.from_result_message(result_message, rate_limit)
            except WorkerCrashError:
                if was_warm and not retried_warm_crash:
                    retried_warm_crash = True
                    force_cold = True
                    continue
                raise
            finally:
                self._reap(worker)
                self._nudge_replenisher()

    async def _checkout_worker(self) -> tuple[_Worker, bool]:
        while self._warm:
            worker = self._warm.pop()
            if not worker.alive:
                self._reap(worker, kill=True)
                continue
            if time.monotonic() - worker.idle_since > self._max_idle:
                self._reap(worker)
                continue
            return worker, True
        return await self._spawn_worker(), False

    async def _spawn_worker(self) -> _Worker:
        try:
            return await _Worker.spawn(self._argv, cwd=self._cwd, env=self._env)
        except OSError as exc:
            raise WorkerStartError(str(exc)) from exc

    async def _replenisher(self) -> None:
        while True:
            waiter = asyncio.create_task(self._replenish_event.wait())
            try:
                await asyncio.wait({waiter}, timeout=1.0)
            finally:
                waiter.cancel()
                with suppress(asyncio.CancelledError):
                    await waiter
            self._replenish_event.clear()
            self._discard_dead_warm()
            await self._replenish_once()

    async def _replenish_once(self) -> None:
        async with self._replenish_lock:
            while not self._closed and len(self._warm) + self._spawns_in_flight < self._warm_target:
                cooldown_remaining = self._spawn_cooldown_until - time.monotonic()
                if cooldown_remaining > 0:
                    return
                self._spawns_in_flight += 1
                worker: _Worker | None = None
                try:
                    worker = await self._spawn_worker()
                    await asyncio.sleep(0.5)
                    if not worker.alive:
                        error = WorkerStartError(
                            "warm worker exited during startup",
                            stderr_tail=worker.stderr_tail,
                        )
                        logger.warning("%s", error)
                        self._spawn_cooldown_until = time.monotonic() + 30.0
                        await worker.kill()
                        return
                    if self._closed:
                        self._reap(worker)
                    else:
                        self._warm.append(worker)
                except asyncio.CancelledError:
                    if worker is not None:
                        await worker.kill()
                    raise
                except WorkerStartError as exc:
                    logger.warning("%s", exc)
                    self._spawn_cooldown_until = time.monotonic() + 30.0
                    return
                finally:
                    self._spawns_in_flight -= 1

    def _nudge_replenisher(self) -> None:
        if self._started and not self._closed:
            self._replenish_event.set()

    async def _sweeper(self) -> None:
        while True:
            await asyncio.sleep(60.0)
            await self._retire_expired_warm()

    async def _retire_expired_warm(self) -> None:
        kept: deque[_Worker] = deque()
        now = time.monotonic()
        while self._warm:
            worker = self._warm.popleft()
            if not worker.alive:
                self._reap(worker, kill=True)
            elif now - worker.idle_since > self._max_idle:
                self._reap(worker)
            else:
                kept.append(worker)
        self._warm = kept

    def _discard_dead_warm(self) -> None:
        kept: deque[_Worker] = deque()
        while self._warm:
            worker = self._warm.popleft()
            if worker.alive:
                kept.append(worker)
            else:
                self._reap(worker, kill=True)
        self._warm = kept

    def _reap(self, worker: _Worker, *, kill: bool = False) -> None:
        task = asyncio.create_task(worker.kill() if kill else worker.retire())
        self._reaper_tasks.add(task)
        task.add_done_callback(self._reaper_tasks.discard)

    def __enter__(self) -> ClaudePool:
        """Enter this synchronous pool context and return the pool."""
        self._use_sync()
        self._run_sync(self._start())
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """Close the pool when leaving a synchronous context."""
        del exc_type, exc, tb
        self.close()
        return None


def main(argv: Sequence[str] | None = None) -> int:
    """Run the claude-pool command-line interface and return a process status."""
    del argv
    sys.stderr.write("not yet implemented\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
