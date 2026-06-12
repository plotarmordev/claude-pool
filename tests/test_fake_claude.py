from __future__ import annotations

import json
import os
import select
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
FAKE = ROOT / "tests" / "fake_claude.py"


def _user(prompt: str) -> str:
    return json.dumps(
        {
            "type": "user",
            "message": {
                "role": "user",
                "content": [{"type": "text", "text": prompt}],
            },
        }
    )


def _run_fake(
    prompts: list[str],
    *,
    env: dict[str, str] | None = None,
    timeout: float = 5.0,
    extra_args: list[str] | None = None,
) -> subprocess.CompletedProcess[str]:
    stdin = "".join(f"{_user(prompt)}\n" for prompt in prompts)
    command = [
        sys.executable,
        str(FAKE),
        "-p",
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
        "--verbose",
        "--model",
        "fake-model",
        "--effort",
        "low",
        "--system-prompt",
        "system",
        "--allowedTools",
        "Read",
        "--disallowedTools",
        "Write",
        "--permission-mode",
        "acceptEdits",
        "--unknown",
        "ignored",
    ]
    if extra_args:
        command.extend(extra_args)
    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    return subprocess.run(
        command,
        input=stdin,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
        env=full_env,
    )


def _json_lines(stdout: str) -> list[dict[str, Any]]:
    return [json.loads(line) for line in stdout.splitlines()]


def _read_turn(process: subprocess.Popen[str]) -> list[dict[str, Any]]:
    assert process.stdout is not None
    deadline = time.monotonic() + 5.0
    lines = []
    while time.monotonic() < deadline:
        ready, _, _ = select.select([process.stdout], [], [], 0.1)
        if not ready:
            continue
        line = process.stdout.readline()
        assert line
        message = json.loads(line)
        lines.append(message)
        if message["type"] == "result":
            return lines
    raise AssertionError("timed out waiting for result")


def test_default_echo_emits_schema_faithful_turn_and_exits_zero() -> None:
    completed = _run_fake(["hello"])

    assert completed.returncode == 0
    assert completed.stderr == ""
    lines = _json_lines(completed.stdout)
    assert [line["type"] for line in lines] == ["system", "assistant", "result"]
    init, assistant, result = lines
    assert init["subtype"] == "init"
    assert init["session_id"] == result["session_id"]
    assert assistant["message"]["content"][0]["text"] == "hello"
    assert result["result"] == "hello"
    assert result["is_error"] is False
    assert result["subtype"] == "success"
    assert result["num_turns"] == 1
    assert result["usage"]["cache_creation_input_tokens"] == 0
    assert result["usage"]["cache_read_input_tokens"] == 0
    assert "server_tool_use" in result["usage"]
    assert result["total_cost_usd"] == 0.001
    assert result["duration_ms"] == 12


def test_constant_session_id_per_process_and_incrementing_turns() -> None:
    completed = _run_fake(["one", "two"])
    other_completed = _run_fake(["other"])

    assert completed.returncode == 0
    assert other_completed.returncode == 0
    results = [line for line in _json_lines(completed.stdout) if line["type"] == "result"]
    other_result = _json_lines(other_completed.stdout)[-1]
    assert len(results) == 2
    assert results[0]["session_id"] == results[1]["session_id"]
    assert results[0]["session_id"] != other_result["session_id"]
    assert [result["num_turns"] for result in results] == [1, 2]
    assert [result["result"] for result in results] == ["one", "two"]


def test_fake_idles_until_stdin_message_and_reuses_interactive_session() -> None:
    full_env = os.environ.copy()
    full_env["FAKE_CLAUDE_STARTUP"] = "ok"
    process = subprocess.Popen(
        [sys.executable, str(FAKE), "-p", "--input-format", "stream-json"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=full_env,
    )
    assert process.stdin is not None
    assert process.stdout is not None

    try:
        ready, _, _ = select.select([process.stdout], [], [], 0.3)
        assert ready == []
        assert process.poll() is None

        process.stdin.write(f"{_user('first')}\n")
        process.stdin.flush()
        first_turn = _read_turn(process)

        process.stdin.write(f"{_user('second')}\n")
        process.stdin.flush()
        second_turn = _read_turn(process)

        first_init = first_turn[0]
        second_init = second_turn[0]
        first_result = first_turn[-1]
        second_result = second_turn[-1]
        assert first_init["type"] == "system"
        assert first_init["subtype"] == "init"
        assert second_init["type"] == "system"
        assert second_init["subtype"] == "init"
        assert first_result["session_id"] == second_result["session_id"]
        assert first_result["result"] == "first"
        assert second_result["result"] == "second"

        process.stdin.close()
        assert process.wait(timeout=5.0) == 0
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5.0)


def test_startup_exit2_exits_immediately() -> None:
    completed = _run_fake(["hello"], env={"FAKE_CLAUDE_STARTUP": "exit2"})

    assert completed.returncode == 2
    assert completed.stdout == ""


def test_startup_autherr_prints_login_guidance_and_exits_one() -> None:
    completed = _run_fake(["hello"], env={"FAKE_CLAUDE_STARTUP": "autherr"})

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert "Invalid API key" in completed.stderr
    assert "Please run /login" in completed.stderr


def test_startup_delay_waits_before_processing() -> None:
    start = time.monotonic()
    completed = _run_fake(["hello"], env={"FAKE_CLAUDE_STARTUP_DELAY": "0.2"})

    assert completed.returncode == 0
    assert time.monotonic() - start >= 0.18


def test_sleep_prefix_delays_result() -> None:
    start = time.monotonic()
    completed = _run_fake(["SLEEP:0.2"])

    assert completed.returncode == 0
    assert time.monotonic() - start >= 0.18
    result = _json_lines(completed.stdout)[-1]
    assert result["type"] == "result"
    assert result["result"] == "SLEEP:0.2"


def test_die_prefix_emits_assistant_then_exits_without_result() -> None:
    completed = _run_fake(["DIE"])

    assert completed.returncode == 1
    lines = _json_lines(completed.stdout)
    assert [line["type"] for line in lines] == ["system", "assistant"]
    assert "died during turn" in completed.stderr


def test_garbage_prefix_emits_invalid_and_unknown_lines_before_result() -> None:
    completed = _run_fake(["GARBAGE"])

    assert completed.returncode == 0
    raw_lines = completed.stdout.splitlines()
    assert raw_lines[0] == "this is not json"
    assert json.loads(raw_lines[1])["type"] == "unknown_type"
    assert json.loads(raw_lines[-1])["type"] == "result"
    assert json.loads(raw_lines[-1])["result"] == "GARBAGE"


def test_bigline_prefix_puts_requested_payload_size_in_result() -> None:
    completed = _run_fake(["BIGLINE:200000"])

    assert completed.returncode == 0
    result = _json_lines(completed.stdout)[-1]
    assert result["type"] == "result"
    assert len(result["result"]) == 200000


def test_error_prefix_marks_result_as_error() -> None:
    completed = _run_fake(["ERROR"])

    assert completed.returncode == 0
    result = _json_lines(completed.stdout)[-1]
    assert result["type"] == "result"
    assert result["is_error"] is True
    assert result["subtype"] == "error_during_execution"


def test_ratelimit_prefix_emits_event_before_normal_result() -> None:
    completed = _run_fake(["RATELIMIT"])

    assert completed.returncode == 0
    lines = _json_lines(completed.stdout)
    assert [line["type"] for line in lines] == [
        "system",
        "rate_limit_event",
        "assistant",
        "result",
    ]
    assert lines[1]["retry_after_ms"] == 250
    assert lines[-1]["is_error"] is False
