# The Claude Code TUI Stop-hook worker protocol (observed)

`claude-pool` v0.2 will add a TUI backend that drives plain `claude` in a pty,
without `-p` and without stream-json. Task 62 adds the private worker only; the
public pool still uses the stream-json backend until Task 63 wires backend
selection.

Everything below was observed live against **Claude Code 2.1.175** (Linux/arm64,
Raspberry Pi 5) on 2026-06-12 and is covered by `tests/fake_claude_tui.py`.

## Process lifecycle

The worker starts:

```text
claude --session-id <uuid> --settings <settings.json> [profile flags]
```

The generated settings file registers a `Stop` hook:

```json
{"hooks":{"Stop":[{"hooks":[{"type":"command","command":"cat >> <hook.ndjson>"}]}]}}
```

The process owns its own POSIX process group and runs on a pty. The pty output
is drained only for diagnostics. Turn completion is detected from the Stop-hook
file, not by screen scraping.

## Turn completion

Prompts are written through bracketed paste, followed by carriage return:

```text
ESC [ 200 ~ <prompt bytes> ESC [ 201 ~ \r
```

The Stop hook receives a JSON object on stdin at the end of a turn and appends
one JSON line to the worker hook file. Observed payload fields include:

| field | meaning |
| --- | --- |
| `last_assistant_message` | Full assistant reply text. This is the source of `Result.text`. |
| `session_id` | TUI session id passed at process start. |
| `transcript_path` | Path to the conversation transcript JSONL. |
| `permission_mode` | Effective permission mode. |
| `effort` | Effective effort setting. |
| `hook_event_name` | Hook event name, observed as `Stop`. |

The transcript JSONL flushes asynchronously and may lag the hook. The worker may
read it for best-effort usage metadata, but reply text must come from
`last_assistant_message`. If the process is killed immediately after a hook,
transcript metadata can be missing.

## Startup and errors

A workspace trust dialog can appear before the prompt. The observed dialog is
answerable by sending a single carriage return to accept the default.

Authentication failures can surface as TUI startup text followed by process
exit. The worker maps that to `WorkerStartError` with the drained pty tail so
callers can match text such as `Invalid API key` or `/login`.

Mid-session usage-limit or policy text is ordinary assistant output from the
TUI. It is returned as normal `Result.text`; callers classify that text the same
way they do for stream-json stdout today.
