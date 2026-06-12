# claude-pool

`claude-pool` is a zero-dependency Python utility for running the Claude Code
CLI through warm persistent workers.

It keeps Claude Code worker processes ready, sends prompts through either the
default stream-json print-mode backend or the TUI pty backend, and returns
structured result objects. A plain `ask()` uses a fresh worker context for each
request. Multi-turn conversations are only kept when you explicitly open a
`Session`. The optional Unix-socket daemon speaks NDJSON so programs in any
language can send prompt requests.

`claude-pool` is not affiliated with Anthropic.

## Install

```sh
pip install claude-pool
```

You can also vendor it by copying `claude_pool.py` into your project. The
runtime uses only the Python standard library, supports Python 3.10+, and is
intended for POSIX platforms.

## Quickstart

```python
from claude_pool import ClaudePool

with ClaudePool() as pool:
    result = pool.ask_sync("Reply with exactly: OK")
    print(result.text)
```

## Async Usage

```python
import asyncio

from claude_pool import ClaudePool


async def main() -> None:
    async with ClaudePool(warm=1, max_workers=4) as pool:
        result = await pool.ask("Summarize what a warm worker pool does.")
        print(result.text)

        async with pool.session() as session:
            first = await session.send("Remember the number 12.")
            second = await session.send("What number did I ask you to remember?")
            print(first.session_id, second.session_id)
            print(second.text)


asyncio.run(main())
```

Async and sync methods are mutually exclusive per `ClaudePool` instance. Create
separate pools if one part of a program uses async code and another uses sync
code.

## Sync Usage

```python
from claude_pool import ClaudePool

pool = ClaudePool()
try:
    result = pool.ask_sync("Reply with one sentence.")
    print(result.text)

    with pool.session_sync() as session:
        first = session.send("Remember the word: river.")
        second = session.send("What word did I ask you to remember?")
        print(first.session_id, second.session_id)
        print(second.text)
finally:
    pool.close()
```

## CLI Daemon

Start a daemon:

```sh
claude-pool serve --socket /tmp/claude-pool.sock --warm 1 --max-workers 4
```

Ask through the daemon:

```sh
claude-pool ask "Reply with exactly: OK" --socket /tmp/claude-pool.sock
```

Check status:

```sh
claude-pool status --socket /tmp/claude-pool.sock
```

Check the local Claude Code setup. This makes one real Claude request:

```sh
claude-pool doctor
```

Use `claude-pool doctor --backend both` to check both backends. That makes two
real Claude requests.

See [examples/shell.md](examples/shell.md) for a two-daemon multi-profile
pattern and a `systemd --user` unit sketch.

## Backends

Choose a backend with `ClaudePool(backend=...)` or `claude-pool serve --backend`.

| Backend | Use when | Tradeoff |
| --- | --- | --- |
| `stream-json` | You want structured metadata including usage, cost, duration, and rate-limit events. | Uses Claude Code print mode (`claude -p`). This is the default. |
| `tui` | You need to avoid print mode and drive plain `claude` in a pty. | Text-first result metadata, best-effort usage extraction, and heavier worker startup. |

## How It Works

```text
start()
  |
  v
spawn warm workers
  |
  v
ask() checks out one worker -> sends one prompt -> reads result
  |
  v
retire consumed worker
  |
  v
replenisher spawns a replacement warm worker
```

Plain `ask()` always consumes and retires its worker so the next plain request
gets a fresh context. `Session` keeps one checked-out worker for the context
manager lifetime, so the Claude CLI process carries conversation state across
turns.

## How this relates to `claude -p`

The default `stream-json` backend is Claude Code print mode kept warm:

```text
claude -p --input-format stream-json --output-format stream-json --verbose
```

It uses the same CLI flags, same local Claude Code login, same account, and
same limits as running `claude -p` yourself. The difference is process
lifecycle: `claude-pool` starts workers ahead of time and reuses a checked-out
process for exactly one plain `ask()` or for the lifetime of an explicit
`Session`.

The `tui` backend is the shipped no-`-p` path. It starts plain `claude` in a
pty, registers Claude Code hooks, and reads completed turn text from the Stop
hook payload.

## Result Fields

`ask()` and `Session.send()` return `Result`:

| Field | Meaning |
| --- | --- |
| `text` | The CLI result text. |
| `is_error` | Whether Claude Code marked the result as an error. This is returned as a normal `Result`, not raised. |
| `subtype` | The CLI result subtype. |
| `session_id` | The worker session id reported by Claude Code. |
| `usage` | Token and cache usage reported by Claude Code. |
| `cost_usd` | Reported total cost in USD. |
| `duration_ms` | Reported turn duration. |
| `rate_limit` | Latest rate-limit event seen during the turn, if any. |
| `raw` | The original result message. |

Branch on `result.is_error` before treating `result.text` as ordinary model
output. Claude Code can put error text in the same field as successful output.

## Exceptions

| Exception | When it is raised |
| --- | --- |
| `ClaudePoolError` | Base class for pool errors and closed sessions. |
| `PoolClosed` | Work is requested after a pool has closed. |
| `WorkerStartError` | A worker process cannot be started. |
| `WorkerCrashError` | A worker exits before producing a result. |
| `AskTimeout` | A prompt exceeds its timeout and the worker is killed. |

`WorkerStartError` and `WorkerCrashError` expose `stderr_tail`, a bounded tail
of worker stderr.

## Supported Platforms

Linux and macOS are supported. Windows is unsupported in v0.x because worker
cleanup uses POSIX process groups.

## FAQ

### Does this bypass subscription limits?

No. `claude-pool` runs the local `claude` CLI with your existing Claude Code
login. It uses the same account, subscription, authentication state, and rate
limits as the interactive CLI.

### Is this an Anthropic project?

No. This project is not affiliated with Anthropic.

### When should I use the official Claude Agent SDK instead?

Use the official Claude Agent SDK for long-running agents, tool orchestration,
streaming partial outputs, and API-key based applications. `claude-pool` is for
fire-and-forget prompt-to-result calls through the local Claude Code CLI.

### What if the Claude Code protocol changes?

The implementation is based on the stream-json behavior captured in
[PROTOCOL.md](PROTOCOL.md). If behavior changes, run `claude-pool doctor` first
to check the local binary, authentication, and one real round trip.

## Changelog

See [CHANGELOG.md](CHANGELOG.md).
