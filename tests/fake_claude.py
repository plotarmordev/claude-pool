#!/usr/bin/env python3
"""A stdlib-only fake Claude CLI for claude-pool tests."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from typing import Any


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-p", action="store_true")
    parser.add_argument("--input-format")
    parser.add_argument("--output-format")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--model")
    parser.add_argument("--effort")
    parser.add_argument("--system-prompt")
    parser.add_argument("--allowedTools")
    parser.add_argument("--disallowedTools")
    parser.add_argument("--permission-mode")
    args, _unknown = parser.parse_known_args()
    return args


def _write_stdout(obj: dict[str, Any] | str) -> None:
    if isinstance(obj, str):
        sys.stdout.write(obj + "\n")
    else:
        sys.stdout.write(json.dumps(obj, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def _prompt_text(message: dict[str, Any]) -> str:
    content = message.get("message", {}).get("content", [])
    if not isinstance(content, list):
        return ""
    parts = []
    for item in content:
        if isinstance(item, dict) and item.get("type") == "text":
            text = item.get("text", "")
            if isinstance(text, str):
                parts.append(text)
    return "".join(parts)


def _usage() -> dict[str, Any]:
    return {
        "input_tokens": 3,
        "output_tokens": 5,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0,
        "server_tool_use": {},
    }


def _init_message(session_id: str) -> dict[str, Any]:
    return {
        "type": "system",
        "subtype": "init",
        "session_id": session_id,
        "model": "fake-claude",
        "claude_code_version": "fake",
        "tools": [],
        "permissionMode": "default",
    }


def _assistant_message(text: str, session_id: str) -> dict[str, Any]:
    return {
        "type": "assistant",
        "session_id": session_id,
        "message": {
            "role": "assistant",
            "content": [{"type": "text", "text": text}],
        },
    }


def _result_message(
    text: str,
    session_id: str,
    num_turns: int,
    *,
    is_error: bool = False,
    subtype: str = "success",
) -> dict[str, Any]:
    return {
        "type": "result",
        "subtype": subtype,
        "result": text,
        "is_error": is_error,
        "session_id": session_id,
        "num_turns": num_turns,
        "usage": _usage(),
        "modelUsage": {},
        "total_cost_usd": 0.001,
        "duration_ms": 12,
        "duration_api_ms": 10,
        "time_to_request_ms": 1,
        "ttft_ms": 2,
        "ttft_stream_ms": 3,
        "stop_reason": "end_turn",
        "terminal_reason": None,
        "permission_denials": [],
        "api_error_status": None,
        "uuid": str(uuid.uuid4()),
    }


def _rate_limit_message(session_id: str) -> dict[str, Any]:
    return {
        "type": "rate_limit_event",
        "session_id": session_id,
        "message": "fake rate limit event",
        "retry_after_ms": 250,
    }


def _handle_startup() -> None:
    delay = os.environ.get("FAKE_CLAUDE_STARTUP_DELAY")
    if delay:
        time.sleep(float(delay))

    startup = os.environ.get("FAKE_CLAUDE_STARTUP", "ok")
    if startup == "exit2":
        raise SystemExit(2)
    if startup == "autherr":
        sys.stderr.write("Invalid API key · Please run /login\n")
        sys.stderr.flush()
        raise SystemExit(1)
    if startup == "authstall":
        sys.stderr.write("Failed to authenticate: OAuth session expired\n")
        sys.stderr.flush()
        time.sleep(30)
    if startup == "ratelimitstall":
        sys.stderr.write("Rate limit reached; service overloaded\n")
        sys.stderr.flush()
        time.sleep(30)


def _handle_prompt(prompt: str, session_id: str, num_turns: int) -> None:
    if prompt.startswith("SLEEP:"):
        text = prompt
        sleep_for = float(prompt.removeprefix("SLEEP:"))
        _write_stdout(_init_message(session_id))
        _write_stdout(_assistant_message(text, session_id))
        time.sleep(sleep_for)
        _write_stdout(_result_message(text, session_id, num_turns))
        return

    if prompt.startswith("DIE"):
        _write_stdout(_init_message(session_id))
        _write_stdout(_assistant_message("dying", session_id))
        sys.stderr.write("fake claude died during turn\n")
        sys.stderr.flush()
        raise SystemExit(1)

    if prompt.startswith("GARBAGE"):
        _write_stdout("this is not json")
        _write_stdout({"type": "unknown_type", "payload": "ignored"})
        text = prompt
        _write_stdout(_init_message(session_id))
        _write_stdout(_assistant_message(text, session_id))
        _write_stdout(_result_message(text, session_id, num_turns))
        return

    if prompt.startswith("BIGLINE:"):
        size = int(prompt.removeprefix("BIGLINE:"))
        text = "x" * size
        _write_stdout(_init_message(session_id))
        _write_stdout(_assistant_message(text, session_id))
        _write_stdout(_result_message(text, session_id, num_turns))
        return

    if prompt.startswith("ERROR"):
        _write_stdout(_init_message(session_id))
        _write_stdout(_assistant_message("error", session_id))
        _write_stdout(
            _result_message(
                "error",
                session_id,
                num_turns,
                is_error=True,
                subtype="error_during_execution",
            )
        )
        return

    if prompt.startswith("RATELIMIT"):
        text = prompt
        _write_stdout(_init_message(session_id))
        _write_stdout(_rate_limit_message(session_id))
        _write_stdout(_assistant_message(text, session_id))
        _write_stdout(_result_message(text, session_id, num_turns))
        return

    _write_stdout(_init_message(session_id))
    _write_stdout(_assistant_message(prompt, session_id))
    _write_stdout(_result_message(prompt, session_id, num_turns))


def main() -> int:
    """Run the fake CLI until stdin reaches EOF."""
    _parse_args()
    _handle_startup()
    session_id = f"fake-{os.getpid()}-{uuid.uuid4()}"
    num_turns = 0

    for line in sys.stdin:
        if not line.strip():
            continue
        message = json.loads(line)
        num_turns += 1
        _handle_prompt(_prompt_text(message), session_id, num_turns)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
