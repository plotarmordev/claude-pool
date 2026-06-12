# AGENTS.md — conventions for implementers

This is a small public utility. Its value is that anyone can read the whole
thing in one sitting. Every rule below is enforced at review.

## Hard rules

- **Zero runtime dependencies.** The library uses the Python standard library
  only. Test/dev dependencies (pytest, ruff) live in the `dev` extra.
- **One vendorable module.** All library + CLI code lives in `claude_pool.py`
  at the repo root. No package directory, no second module. If a feature does
  not fit, the feature is wrong, not the layout.
- **Generic or it does not ship.** Nothing tailored to any one consumer: no
  provider-fallback logic, no usage-limit state machines, no telemetry hooks
  beyond returning what the CLI already reports. Knobs are passed through to
  the CLI (`extra_args` is the escape hatch); defaults are the CLI's defaults.
- **POSIX only for v0.x.** Process-group handling may use `os.killpg` freely.
  Guard imports so the module still imports on Windows, but APIs may raise
  `NotImplementedError` there.
- Python **3.10+**. CI runs 3.10–3.13.

## Code quality

- Type hints on the entire public API; dataclasses for value types.
- Docstrings on every public class/function — written for a stranger, stating
  contract (inputs, outputs, raises), not narrating implementation.
- No `print()` in the library; use the `claude_pool` stdlib logger. The CLI
  subcommands print to stdout/stderr as their interface.
- `ruff check .` and `ruff format --check .` must be clean (config in
  pyproject.toml, line length 100).
- Asyncio discipline: every spawned task is tracked and awaited/cancelled in
  `aclose()`; no fire-and-forget; no bare `except`.

## Testing

- The default test run must pass **without the `claude` CLI installed and with
  no network**: everything runs against `tests/fake_claude.py` via the
  `claude_bin=` parameter.
- Live tests (real CLI, real subscription) are marked `@pytest.mark.live` and
  excluded by default (`-m "not live"` is the configured default).
- Every failure path in PROTOCOL.md gets a test: oversized result lines,
  garbage interleaved output, mid-turn worker death, startup death, timeout
  (must kill the whole process group — assert no surviving children).
- Run: `/tmp/pool-test-venv/bin/pytest` and `/tmp/pool-test-venv/bin/ruff
  check .` (create the venv with `python3 -m venv /tmp/pool-test-venv &&
  /tmp/pool-test-venv/bin/pip install -e .[dev]`).

## Workflow

- Base branch: `main`. One task per branch per PR.
- Open PRs against `main`; **do not merge** — the reviewer merges after review.
- Conventional commit messages (`feat:`, `fix:`, `test:`, `docs:`, `chore:`).
- PR body: WHAT / WHY / CHANGES / RESULTS (test+ruff output) / RISK.
