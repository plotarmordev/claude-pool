# The Claude Code stream-json worker protocol (observed)

`claude-pool` drives the Claude Code CLI in its headless streaming mode:

```
claude -p --input-format stream-json --output-format stream-json --verbose [profile flags]
```

Everything below was observed live against **Claude Code 2.1.175** (Linux/arm64,
Raspberry Pi 5) on 2026-06-12. This mode is CLI-internal and may drift between
releases; `tests/fixtures/stream_json/` pins sanitized real captures, and
`claude-pool doctor` checks your installed CLI end to end.

## Process lifecycle

- After spawn the process **emits nothing and makes no API traffic** until the
  first user message arrives on stdin. It idles indefinitely. This is what makes
  pre-warmed workers free: Node bootstrap (~1.5–2.5 s on a Pi 5) happens at spawn
  time, off the request path.
- The process stays alive after answering and accepts further user messages on
  the **same conversation** (same `session_id`, context accumulates).
- Closing stdin makes the process exit cleanly (rc=0, observed ~0.4 s).
- There is **no ready handshake** before the first input. Liveness
  (`returncode is None`) is the only spawn-time health signal.

## Input framing

One JSON object per line on stdin (NDJSON):

```json
{"type": "user", "message": {"role": "user", "content": [{"type": "text", "text": "..."}]}}
```

## Output framing

NDJSON on stdout. Observed per-turn sequence:

| order | type / subtype | notes |
|---|---|---|
| 1 | `system` / `init` | re-emitted at the start of **every** turn; carries `session_id`, `model`, `claude_code_version`, `tools`, `permissionMode` |
| 2? | `rate_limit_event` | optional; structured usage-limit telemetry |
| 3* | `system` / `thinking_tokens` | progress chatter; ignorable |
| 4* | `assistant` | streamed assistant message chunks |
| 5 | `result` / `success` (or error subtype) | **end-of-turn marker** — exactly one per turn |

A reader needs only: *scan lines, tolerate anything unknown, stop at `type == "result"`.*

## The `result` message

Fields observed (2.1.175): `result` (final text), `is_error`, `subtype`,
`session_id`, `num_turns`, `usage` (incl. `cache_creation_input_tokens`,
`cache_read_input_tokens`, `server_tool_use`), `modelUsage`, `total_cost_usd`,
`duration_ms`, `duration_api_ms`, `time_to_request_ms`, `ttft_ms`,
`ttft_stream_ms`, `stop_reason`, `terminal_reason`, `permission_denials`,
`api_error_status`, `uuid`.

`result` lines are single long JSON lines — size scales with output. Readers
must raise their line-length limit well above asyncio's 64 KiB default
(claude-pool uses 10 MiB).

## Latency reference (Pi 5, haiku, trivial prompt, 2026-06-12)

- Warm worker, send → result: **~2.0 s**
- Cold `claude -p` one-shot: **3.6–4.4 s**

## Caching note

Prompt caching happens server-side keyed on the prompt prefix, so fresh
sessions still hit the cache (observed: 17.7k cache-read tokens on the very
first turn of a new session). Isolating every request in its own session costs
nothing in cache efficiency.
