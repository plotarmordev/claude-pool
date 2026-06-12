# GOAL: claude-pool v0.2 — TUI backend (no `-p`), backend abstraction, v0.1 hygiene

## Why

v0.1 replaced *cold* `claude -p` spawns with warm ones — but every worker still runs
`claude -p --input-format stream-json`. The owner's core requirement is removing the
reliance on print mode itself. The only programmatic surface that does not touch `-p`
is the interactive TUI. v0.2 adds a TUI backend behind the existing public API.

## Feasibility (spiked 2026-06-12, CLI 2.1.175, Pi 5)

Spawning plain `claude` (NO `-p`) in a pty with `--session-id <uuid>` and
`--settings <file>` registering a `Stop` hook (`"type":"command","command":"cat >> <path>"`):

- The hook fires at end of turn and its stdin JSON payload contains
  `last_assistant_message` (the full reply text), `session_id`, `transcript_path`,
  `permission_mode`, `effort`. **No screen scraping, no stream-json, no `-p`.**
- The transcript JSONL flushes asynchronously (may lag the hook) → usage metadata is
  best-effort; the reply text comes from the hook payload, never the transcript.
- A trust dialog may appear on first use of a cwd; answerable with a single `\r`.
- Verified end to end: prompt typed into the pty → hook payload contained exactly the
  requested reply.

## Design rules

- Public API frozen as in GOAL-pool-v01.md. One new constructor parameter:
  `backend="stream-json"` (default, unchanged behavior) | `"tui"`.
- `_Worker` stays as-is. New `_TuiWorker` implements the same interface contract
  (spawn/ask/retire/kill, alive, stderr_tail-equivalent diagnostics) so
  `ClaudePool` code paths stay backend-agnostic.
- Result mapping for TUI: `text` = hook `last_assistant_message`; `session_id` from
  payload; `usage`/`cost_usd`/`duration_ms` best-effort (transcript grace-read ≤1s,
  else `{}`/None; `duration_ms` measured client-side); `subtype` synthesized
  ("success" | "error_during_execution"); `raw` = hook payload.
- Same exception taxonomy. Same process-group kill discipline (pty leader owns the group).
- Known version traps remain law: no `asyncio.wait_for`-on-event (pre-3.12 swallow);
  track-and-cancel for anything `Server.wait_closed`-like; py3.10 `process.wait()`
  vs pipe-holding children. Verify on /tmp/pool-venv-310, /tmp/pool-venv-311,
  /tmp/pool-test-venv before every commit.

### Task 61: v0.1 hygiene (claude_pool.py, PROTOCOL.md, README.md, tests)

1. Daemon ask response gains `rate_limit` and `duration_ms` (keep wire compat: add
   fields, never remove). `status` response gains `backend`.
2. `_send_client_request`: socket timeout (default 600s, `--timeout` reuse for ask;
   status fixed 10s); timeout → clean stderr + exit 1.
3. `tests/fixtures/`: sanitized real stream-json captures (init, assistant, result,
   rate_limit_event lines) + a test that parses them with the production parser —
   the drift sentinel PROTOCOL.md already promises.
4. README: honest "How this relates to `claude -p`" section — stream-json backend IS
   print mode kept warm; the TUI backend (v0.2) is the no-`-p` path.
5. Document that `Result.is_error=True` is returned, not raised, and integrators must
   branch on it.
6. Commit: `fix: daemon metadata, client timeouts, protocol fixtures, README honesty`.

### Task 62: TUI worker (claude_pool.py, tests/fake_claude_tui.py, tests/test_tui_worker.py)

1. `_TuiWorker.spawn(argv_profile, cwd, env)`:
   - `pty.openpty()`; spawn `claude --session-id <uuid4> [--model M] [--effort E]
     --settings <generated-file>` with stdin/stdout/stderr on the slave fd,
     `start_new_session=True`, TERM=xterm-256color, COLUMNS/LINES sane.
   - Generated per-worker settings tempfile: Stop hook `cat >> <per-worker hook file>`
     (absolute path baked in; no env-var dependence). Hook file + settings file
     cleaned up on retire/kill.
   - Readiness: drain pty until idle prompt appears or timeout; answer the trust
     dialog (single `\r`) if detected by plain-text match; startup failure →
     `WorkerStartError` with drained-pty tail as diagnostics.
   - system_prompt profile arg maps to `--append-system-prompt`. allowed/disallowed
     tools map to their flags. Scratch cwd: pool-owned tmpdir per pool (no CLAUDE.md,
     no repo) unless caller passes cwd. Investigate and apply whatever settings
     minimize session overhead (disable auto-memory if a setting exists; skip MCP).
2. `ask(prompt, timeout)`: write prompt via bracketed paste (ESC[200~ … ESC[201~) so
   multi-line prompts don't submit early, then `\r`. Await the hook file gaining a
   complete JSON line (poll ≤50ms; the version-trap-safe wait pattern). Parse payload
   → Result fields per design rules. Timeout → killpg → `AskTimeout`. Pty EOF/child
   exit before hook → `WorkerCrashError` with pty tail.
3. Limit/auth surfacing: probe how usage-limit and logged-out states present in TUI
   mode (likely as assistant/system text or startup screen). Map: detectable
   auth-failure at spawn → `WorkerStartError` whose diagnostics contain the on-screen
   text (so callers' existing "Invalid API key"/"/login" matchers work); mid-session
   limit text returns as a normal Result (text) — document that callers classify text
   exactly as they do for `-p` stdout today. Record findings in PROTOCOL-TUI.md.
4. `tests/fake_claude_tui.py`: stdlib fake that emulates the contract: prints a fake
   prompt banner, optionally a trust dialog (env FAKE_TUI_TRUST=1), reads bracketed-
   paste input, executes the Stop-hook command from the provided settings file with a
   payload containing last_assistant_message (echo semantics + SLEEP:/DIE/NOHOOK magic
   prompts), env FAKE_TUI_STARTUP=ok|exit2|autherr.
5. Tests: happy echo; multi-line prompt integrity; trust-dialog path; startup death
   taxonomy; timeout kills group (sh + sleep child pattern); hook-never-fires + child
   alive → AskTimeout; crash before hook → WorkerCrashError; retire cleans temp files;
   concurrent ask RuntimeError; cancellation mid-ask kills group.
6. PROTOCOL-TUI.md: observed contract (hook payload fields, trust dialog text, flush
   lag, CLI version pinned).
7. Commit: `feat: TUI worker — pty + stop-hook turns, no print mode`.

### Task 63: backend wiring + release 0.2.0 (claude_pool.py, README, CHANGELOG, examples)

1. `ClaudePool(backend=...)` validated at construction; worker factory dispatches;
   everything else (warm pool, sessions, sync mirrors, daemon) backend-agnostic.
   Session multi-turn on TUI = same worker, successive asks (the TUI session carries
   context natively; session_id constant from payload).
2. `serve --backend`, status reports it. `doctor --backend both|stream-json|tui`
   runs the round trip per backend with plain-English diagnoses.
3. Cross-backend parity test: the test-suite contract cases (echo, timeout, crash,
   concurrency, retire hygiene) run against both fakes via parametrization where the
   semantics are shared.
4. README: backend section + decision table (stream-json: richer metadata, `-p`
   dependent; tui: no `-p`, text-first metadata). CHANGELOG 0.2.0. Version bump.
   Examples gain `backend="tui"` variants.
5. One real-CLI verification run of each backend recorded in the PR body.
6. Commit: `feat: selectable backends, doctor coverage, release 0.2.0`.

## Out of scope (separate goal)

smithyx integration (`CLAUDE_RUNNER_MODE` adapter) — designed after v0.2 ships so the
bot can choose its backend per config from day one.
