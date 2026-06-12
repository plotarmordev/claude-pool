# Goal: claude-pool v0.1 — warm-worker runner for the Claude Code CLI

A public, generic, zero-dependency Python utility that runs Claude Code
headlessly the way a human runs it interactively: persistent stream-json worker
processes on the user's subscription login, structured NDJSON in/out, warm
workers answering without Node-bootstrap latency. One-shot `ask()` gets a fresh
context every time; conversations exist only via an explicit `Session`. This is
a substitute for spawning `claude -p` per call — not an API-key client, not a
TUI scraper.

Design evidence: PROTOCOL.md in this repo (live-captured against Claude Code
2.1.175 on the target Pi 5; warm turn ~2.0 s vs cold one-shot 3.6–4.4 s; worker
idles silently until first stdin message; `result` message is the end-of-turn
marker; stdin close exits rc=0).

General rules: work in /home/smith/claude-pool, branch off `main`, one task per
branch/PR, open PR against `main`, do NOT merge — the reviewer merges. Follow
AGENTS.md strictly: zero runtime deps, single vendorable `claude_pool.py`,
POSIX-only process handling, Python 3.10+, default tests pass with no `claude`
CLI and no network, ruff clean. Test venv: `python3 -m venv /tmp/pool-test-venv
&& /tmp/pool-test-venv/bin/pip install -e .[dev]`, then
`/tmp/pool-test-venv/bin/pytest` and `/tmp/pool-test-venv/bin/ruff check .`.

Public API frozen for v0.1 (signatures may not drift between tasks):

```python
ClaudePool(model=None, effort=None, system_prompt=None, allowed_tools=None,
           disallowed_tools=None, permission_mode=None, cwd=None, env=None,
           claude_bin="claude", extra_args=None, warm=1, max_workers=4,
           max_idle=900.0, default_timeout=600.0)
await pool.ask(prompt, timeout=None) -> Result
pool.session() -> async context manager yielding Session; await Session.send(prompt, timeout=None) -> Result
await pool.aclose(); pool.ask_sync(...); pool.close(); both context-manager forms
Result: text, is_error, subtype, session_id, usage, cost_usd, duration_ms, rate_limit, raw
Exceptions: ClaudePoolError base; WorkerStartError, WorkerCrashError (both carry .stderr_tail), AskTimeout, PoolClosed
```

### Task 55: Scaffold, fake CLI, and frozen API surface (pyproject.toml, claude_pool.py, tests/fake_claude.py, tests/test_fake_claude.py, CI, LICENSE, .gitignore)

1. `pyproject.toml`: setuptools backend, `py-modules = ["claude_pool"]`, name
   `claude-pool`, version `0.1.0.dev0`, `requires-python = ">=3.10"`, zero
   runtime dependencies, `[project.optional-dependencies] dev = ["pytest",
   "ruff"]`, console script `claude-pool = "claude_pool:main"`. Ruff config:
   line length 100, target py310. Pytest config: `addopts = "-m 'not live'"`,
   markers `live: hits the real claude CLI and the API`.
2. `claude_pool.py`: module docstring (one-paragraph product statement), the
   full public API from the header above as stubs — dataclass `Result`, the
   exception hierarchy complete and functional, `ClaudePool`/`Session` with
   typed signatures raising `NotImplementedError`, `main()` printing "not yet
   implemented" and exiting 2. The exceptions and `Result` are FINAL here;
   later tasks only fill in behavior.
3. `tests/fake_claude.py`: executable stdlib-only script mimicking the CLI per
   PROTOCOL.md. Accepts and ignores the real flags (`-p`, `--input-format`,
   `--output-format`, `--verbose`, `--model X`, `--effort X`,
   `--system-prompt X`, `--allowedTools X`, `--disallowedTools X`,
   `--permission-mode X`, plus unknown extras). Reads NDJSON user messages on
   stdin until EOF (then exits 0), one constant session_id per process.
   Startup behavior via env: `FAKE_CLAUDE_STARTUP=ok|exit2|autherr` (autherr:
   stderr line containing "Invalid API key · Please run /login", exit 1
   immediately), `FAKE_CLAUDE_STARTUP_DELAY=<seconds>`. Per-turn behavior via
   magic prompt prefixes: default echoes the prompt text back in a
   schema-faithful init+assistant+result sequence (result carries `result`,
   `is_error: false`, `subtype: "success"`, `session_id`, `num_turns`,
   `usage` with cache fields, `total_cost_usd`, `duration_ms`); `SLEEP:<s>`
   waits before the result; `DIE` emits assistant then exits 1 with no result;
   `GARBAGE` emits non-JSON lines and unknown-type JSON before a normal
   result; `BIGLINE:<n>` puts an n-char payload in the result text;
   `ERROR` sets `is_error: true`, `subtype: "error_during_execution"`;
   `RATELIMIT` emits a `rate_limit_event` message before a normal result.
4. `tests/test_fake_claude.py`: drives the fake over real pipes with
   `subprocess` and asserts every behavior above — the fake is the foundation
   of all later tests, so it gets its own contract tests now.
5. `.github/workflows/ci.yml`: matrix py 3.10–3.13 on ubuntu-latest,
   `pip install -e .[dev]`, `ruff check .`, `pytest`. `.gitignore` standard
   Python. `LICENSE` MIT, holder plotarmordev.
6. Commit message: `feat: scaffold, frozen API surface, fake claude CLI for tests`.

### Task 56: Worker — spawn, ask, retire (claude_pool.py, tests/test_worker.py)

1. Internal class `_Worker` (not exported; tests may import it by name).
   `await _Worker.spawn(argv, cwd, env, claude_bin)` builds the full command
   per PROTOCOL.md (`claude_bin -p --input-format stream-json --output-format
   stream-json --verbose` + profile flags), spawns with
   `asyncio.create_subprocess_exec(..., start_new_session=True)` and a 10 MiB
   stdout reader limit. No ready-wait (PROTOCOL.md: there is none); records
   `spawned_at`/`idle_since` monotonic timestamps.
2. A background task drains stderr into a bounded ring buffer (last 64 KiB)
   exposed as `worker.stderr_tail`; it must never block when the buffer is full
   and must be cancelled in `kill()`/`retire()`.
3. `await worker.ask(prompt, timeout)`: writes one NDJSON user message, then
   reads stdout lines tolerating undecodable/unknown lines, remembering the
   last `rate_limit_event`, until `type == "result"`; returns the parsed dict
   plus the rate-limit event. stdout EOF before result → `WorkerCrashError`
   with stderr_tail. Timeout → `kill()` then `AskTimeout`. Exactly one ask may
   be in flight per worker (internal lock; concurrent use is a bug → assert).
4. `await worker.retire()`: close stdin, wait up to 2 s for exit, else
   `kill()`. `worker.kill()`: SIGKILL the whole process group via `os.killpg`,
   guarding `ProcessLookupError`; idempotent. Property `worker.alive`.
5. `Result.from_result_message(d, rate_limit)` classmethod filling every
   Result field (`text` from `result`, `cost_usd` from `total_cost_usd`,
   `raw` = full dict).
6. Tests against the fake: happy echo path; BIGLINE:200000 (line-limit);
   GARBAGE robustness; DIE → WorkerCrashError with stderr_tail populated;
   SLEEP:30 with timeout=1 → AskTimeout and the entire process group is gone
   (spawn a fake that forks a child via `sh -c`, assert no survivors by pgid);
   autherr startup → first ask raises WorkerCrashError whose stderr_tail
   contains the /login text; retire-on-idle exits cleanly without kill.
7. Commit message: `feat: stream-json worker with full failure taxonomy`.

### Task 57: Pool — warm checkout, replenish, lifecycle (claude_pool.py, tests/test_pool.py)

1. `ClaudePool` builds the worker argv once from its profile args
   (`extra_args` appended last). `warm` workers are pre-spawned lazily —
   first call to `ask()`/`session()`/`start()` triggers replenishment;
   an explicit `await pool.start()` pre-warms eagerly and is idempotent.
2. Replenisher: background task keeping `len(warm deque) + spawns in flight
   <= warm`, capped, no spawn storms. Warm spawns do a 0.5 s instant-death
   check (`returncode` set → `WorkerStartError` logged via the `claude_pool`
   logger, replenishment enters a 30 s cooldown). Cold spawns (pool empty at
   checkout) skip the check — startup failures surface as `WorkerCrashError`
   on the ask, carrying stderr.
3. `ask()`: acquire `asyncio.Semaphore(max_workers)`; checkout = pop newest
   warm worker, discarding dead or `max_idle`-expired ones; else cold spawn.
   Send; in `finally` retire the worker (one request per worker, always) and
   nudge the replenisher. If a WARM worker raises `WorkerCrashError`, retry
   exactly once with a cold spawn; cold-worker crashes propagate. Honors
   `default_timeout` when `timeout=None`.
4. Sweeper: lightweight periodic task (60 s) retiring idle-expired warm
   workers so memory is not held by stale workers. Both background tasks are
   tracked and shut down in `aclose()`.
5. `aclose()`: idempotent; cancel replenisher+sweeper, retire all warm
   workers, reject new work with `PoolClosed`. `atexit` best-effort kill of
   any process groups still alive (sync, no event loop). Async context
   manager wires `start()`/`aclose()`.
6. Tests: warm hit (ask consumes pre-warmed worker; a new one replenishes);
   fresh context per ask (two asks → two distinct fake session_ids); dead
   warm worker at checkout is discarded and ask still succeeds; warm-crash
   retried once (fake with FAKE_CLAUDE_STARTUP=exit2 swapped in via
   claude_bin trickery or a DIE first turn), cold-crash propagates;
   max_workers=1 serializes two concurrent asks (assert via fake SLEEP
   overlap timing); max_idle=0.1 expires warm workers; aclose leaves zero
   live process groups; ask after aclose → PoolClosed; concurrent asks with
   unique nonces come back unmixed (n=4).
7. Commit message: `feat: warm pool with replenishment, expiry, and clean shutdown`.

### Task 58: Session and sync mirrors (claude_pool.py, tests/test_session.py)

1. `pool.session()` async context manager: checks out one worker (counts
   against the semaphore for its whole lifetime, warm-preferred) and yields
   `Session`. `await session.send(prompt, timeout=None)` → `Result`; calls
   are serialized with an internal lock; same worker, same conversation
   (PROTOCOL.md multi-turn). On `WorkerCrashError`/`AskTimeout` the session
   is dead: worker killed, subsequent `send` raises `PoolClosed`-style
   `ClaudePoolError("session closed")`. Exit retires the worker and releases
   the semaphore.
2. Sync mirrors for non-async consumers: a lazily started daemon thread
   running a private event loop owned by the pool; `pool.ask_sync(...)`,
   `pool.close()`, and `Session` via `pool.session_sync()` returning a
   context-manager wrapper with `send(...)`. Implemented with
   `asyncio.run_coroutine_threadsafe`; `close()` joins the thread. The async
   and sync surfaces may not be mixed on one pool instance — first use picks
   the mode, the other then raises `ClaudePoolError` (document this).
3. Tests: two sends share a session_id while plain asks get fresh ones;
   session holds a semaphore slot (max_workers=1 → concurrent ask blocks
   until session exit, assert by timing with fake SLEEP); crash mid-session
   kills worker and poisons the session; sync smoke — `ask_sync` from a
   plain function, `session_sync` two turns, `close()` leaves no threads or
   process groups (assert `threading.enumerate()` shrinks back).
4. Commit message: `feat: explicit multi-turn sessions and sync mirrors`.

### Task 59: CLI — serve, ask, status, doctor (claude_pool.py, tests/test_cli.py)

1. `main()` argparse with subcommands. `serve`: flags mirroring the
   constructor (`--model`, `--effort`, `--system-prompt`, `--allowed-tools`
   comma-split, `--disallowed-tools`, `--permission-mode`, `--cwd`,
   `--claude-bin`, `--extra-arg` repeatable, `--warm`, `--max-workers`,
   `--max-idle`, `--default-timeout`, `--socket PATH`). One profile per
   daemon — multi-profile means multiple daemons on distinct sockets.
   Socket default `$XDG_RUNTIME_DIR/claude-pool.sock`, fallback
   `/tmp/claude-pool-<uid>.sock`; created with 0600 perms, stale socket file
   replaced; SIGTERM/SIGINT → graceful `aclose()` and socket cleanup.
2. Wire protocol: one JSON line per request →` one JSON line per response.
   `{"op":"ask","prompt":"...","timeout":n?}` →
   `{"ok":true,"text":...,"is_error":...,"subtype":...,"session_id":...,
   "usage":...,"cost_usd":...}` or `{"ok":false,"kind":"AskTimeout",
   "error":"..."}` (kind = exception class name). `{"op":"status"}` → warm
   count, in-flight count, profile summary, pid. Malformed request → ok:false
   with kind "BadRequest"; the server never dies from a bad client.
3. `ask` subcommand: `claude-pool ask "prompt" [--socket PATH] [--timeout N]`
   prints `text` to stdout; transport errors and `ok:false` go to stderr with
   exit code 1 (is_error results: text still to stdout, exit 1). `status`
   pretty-prints the status response.
4. `doctor [--claude-bin X]`: checks the binary is on PATH, prints
   `claude --version`, spawns one real worker, sends "Reply with exactly:
   OK", prints round-trip latency and session_id, exits non-zero with a
   plain-English diagnosis on any failure (binary missing, startup death
   with stderr tail shown, timeout). Document that doctor makes one real
   API call.
5. Tests (all against the fake via --claude-bin): serve+ask round trip over
   a tmpdir socket; status; concurrent CLI asks; ok:false propagation for
   ERROR and for AskTimeout; malformed JSON request; socket perms are 0600;
   SIGTERM leaves no workers (pgid sweep). Doctor gets a fake-CLI test of
   its failure paths (binary missing → exit non-zero; happy path against the
   fake).
6. Commit message: `feat: unix-socket daemon, client, and doctor CLI`.

### Task 60: Examples, packaging, release prep (examples/, README.md, CHANGELOG.md, pyproject.toml)

1. `examples/one_shot.py` (async ask + the Result fields), `examples/chat.py`
   (session multi-turn), `examples/sync_usage.py`, `examples/shell.md`
   (serve/ask/status from plain shell, two-daemon multi-profile pattern).
   Examples must run as-is against the real CLI.
2. README.md full draft per the structure already stubbed: pitch, install
   (pip + vendoring the single file), 5-line quickstart that works verbatim,
   async/sync/CLI usage, how-it-works diagram (spawn-warm → ask → retire →
   replenish), FAQ (subscription auth — same login and same limits as the
   interactive CLI, explicitly not a limit bypass; not affiliated with
   Anthropic; when you should use the official Agent SDK instead), supported
   platforms, protocol-drift caveat pointing at PROTOCOL.md and doctor.
3. CHANGELOG.md (0.1.0), version bump to `0.1.0`, packaging verification:
   build sdist+wheel in a clean venv, install the wheel, import, run
   `claude-pool --help` and the fake-CLI test suite against the installed
   copy. Record the exact commands and output in the PR body.
4. Commit message: `chore: examples, README, changelog — release 0.1.0`.
