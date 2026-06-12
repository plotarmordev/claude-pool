from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from claude_pool import Result


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "stream_json"


def read_jsonl(name: str) -> list[dict[str, Any]]:
    messages = []
    for line in (FIXTURE_DIR / name).read_text().splitlines():
        message = json.loads(line)
        assert isinstance(message, dict)
        messages.append(message)
    return messages


def test_stream_json_fixture_lines_parse() -> None:
    for path in sorted(FIXTURE_DIR.glob("*.jsonl")):
        assert read_jsonl(path.name), path


def test_result_success_fixture_maps_to_result() -> None:
    result_message = read_jsonl("result_success.jsonl")[0]
    result = Result.from_result_message(result_message, rate_limit=None)

    assert result.text == "FIXTURE"
    assert result.subtype == "success"
    assert result.session_id == "00000000-0000-4000-8000-000000000001"
    assert result.usage["input_tokens"] > 0


def test_documented_per_turn_sequence_is_represented() -> None:
    init = read_jsonl("init.jsonl")[0]
    assistant = read_jsonl("assistant.jsonl")[0]
    result = read_jsonl("result_success.jsonl")[0]

    assert (init["type"], init["subtype"]) == ("system", "init")
    assert assistant["type"] == "assistant"
    assert (result["type"], result["subtype"]) == ("result", "success")
