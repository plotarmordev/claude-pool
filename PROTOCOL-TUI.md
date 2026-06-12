# The Claude Code TUI Stop-hook worker protocol (observed)

`claude-pool` v0.2 includes a TUI backend that drives plain `claude` in a pty,
without `-p` and without stream-json. Select it with
`ClaudePool(backend="tui")` or `claude-pool serve --backend tui`.

Everything below was observed live against **Claude Code 2.1.175** (Linux/arm64,
Raspberry Pi 5) on 2026-06-12 and is covered by `tests/fake_claude_tui.py`.

## Process lifecycle

The worker starts:

```text
claude --session-id <uuid> --settings <settings.json> [profile flags]
```

The generated settings file registers `SessionStart` and `Stop` hooks:

```json
{"hooks":{"SessionStart":[{"hooks":[{"type":"command","command":"cat >> <ready.marker>"}]}],"Stop":[{"hooks":[{"type":"command","command":"cat >> <hook.ndjson>"}]}]}}
```

The process owns its own POSIX process group and runs on a pty. The pty output
is drained only for diagnostics. Turn completion is detected from the Stop-hook
file, not by screen scraping.

When no cwd is provided, the worker starts in `~/.cache/claude-pool/cwd`.
Claude Code persists workspace trust by path, so this stable scratch directory
avoids paying a trust prompt for every worker. A caller-provided cwd still wins.

Readiness is detected by the `SessionStart` hook writing to the marker file.
The worker keeps answering a visible trust prompt with carriage return while it
waits for that hook; it does not infer readiness from pty silence or screen
text.

## Turn completion

Prompts are written through bracketed paste, followed by carriage return:

```text
ESC [ 200 ~ <prompt bytes> ESC [ 201 ~ \r
```

Observed submit timing on Claude Code 2.1.175:

- Wait at least 0.75 s after the `SessionStart` marker before the first paste
  on a worker.
- Wait 0.3 s between the end of bracketed paste and the carriage return.
- If no Stop-hook line arrives within 3.0 s after the first carriage return,
  send one additional carriage return. Pressing Enter on an empty input box is
  a no-op; if the first carriage return was swallowed while the TUI was still
  processing paste insertion, the retry submits the existing prompt.

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

The TUI also renders a weekly-limit usage banner in the pty tail, for example
`Youve used N% of your weekly limit`. This is useful future signal but is not
parsed today.
