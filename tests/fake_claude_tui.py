#!/usr/bin/env python3
"""A stdlib-only fake Claude TUI for _TuiWorker tests."""

from __future__ import annotations

import argparse
from contextlib import suppress
import json
import os
from pathlib import Path
import subprocess
import shlex
import sys
import time

try:
    import termios
    import tty
except ImportError:  # pragma: no cover - POSIX test helper.
    termios = None
    tty = None


BRACKETED_PASTE_START = b"\x1b[200~"
BRACKETED_PASTE_END = b"\x1b[201~"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--settings", required=True)
    parser.add_argument("--model")
    parser.add_argument("--effort")
    parser.add_argument("--append-system-prompt")
    parser.add_argument("--allowedTools")
    parser.add_argument("--disallowedTools")
    args, _unknown = parser.parse_known_args()
    return args


def set_raw_stdin() -> None:
    if termios is None or tty is None:
        return
    with suppress(termios.error):
        tty.setraw(sys.stdin.fileno())


def write_screen(text: str) -> None:
    sys.stdout.write(text + "\r\n")
    sys.stdout.flush()


def read_until_submit() -> None:
    while True:
        chunk = os.read(sys.stdin.fileno(), 1024)
        if not chunk:
            raise SystemExit(0)
        if b"\r" in chunk or b"\n" in chunk:
            return


def hook_command(settings_path: str, event: str) -> str:
    settings = json.loads(Path(settings_path).read_text())
    hooks = settings["hooks"][event]
    return hooks[0]["hooks"][0]["command"]


def hook_target(command: str) -> Path:
    if ">>" not in command:
        raise RuntimeError(f"unsupported hook command: {command}")
    tail = command.split(">>", 1)[1].strip()
    parts = shlex.split(tail)
    return Path(parts[0])


def write_transcript(path: Path, text: str) -> None:
    payload = {
        "type": "assistant",
        "message": {
            "role": "assistant",
            "content": [{"type": "text", "text": text}],
            "usage": {
                "input_tokens": 3,
                "output_tokens": 5,
                "cache_creation_input_tokens": 0,
                "cache_read_input_tokens": 0,
                "server_tool_use": {},
            },
        },
    }
    path.write_text(json.dumps(payload, separators=(",", ":")) + "\n")


def emit_hook(args: argparse.Namespace, text: str) -> None:
    command = hook_command(args.settings, "Stop")
    target = hook_target(command)
    transcript_path = target.parent / "fake-transcript.jsonl"
    write_transcript(transcript_path, text)
    payload = {
        "session_id": args.session_id,
        "transcript_path": str(transcript_path),
        "last_assistant_message": text,
        "hook_event_name": "Stop",
        "permission_mode": "default",
        "effort": args.effort,
    }
    line = json.dumps(payload, separators=(",", ":")) + "\n"
    if os.environ.get("FAKE_TUI_JUNK_HOOK") == "1":
        with target.open("a", encoding="utf-8") as file:
            file.write("[1,2,3]\n\n")
    if os.environ.get("FAKE_TUI_PARTIAL_HOOK") == "1":
        with target.open("a", encoding="utf-8") as file:
            file.write(line.rstrip("\n"))
        time.sleep(0.3)
        with target.open("a", encoding="utf-8") as file:
            file.write("\n")
        return
    subprocess.run(command, input=line, text=True, shell=True, check=False)


def emit_session_start(args: argparse.Namespace) -> None:
    command = hook_command(args.settings, "SessionStart")
    payload = {
        "session_id": args.session_id,
        "hook_event_name": "SessionStart",
    }
    subprocess.run(
        command,
        input=json.dumps(payload, separators=(",", ":")) + "\n",
        text=True,
        shell=True,
        check=False,
    )


def handle_prompt(args: argparse.Namespace, prompt: str) -> None:
    if prompt.startswith("SLEEP:"):
        time.sleep(float(prompt.removeprefix("SLEEP:")))
        emit_hook(args, prompt)
        return
    if prompt.startswith("EXIT_WITH_CHILD"):
        subprocess.Popen(["sleep", "30"])
        emit_hook(args, prompt)
        raise SystemExit(0)
    if prompt.startswith("DIE"):
        write_screen("fake-tui dying")
        raise SystemExit(3)
    if prompt.startswith("NOHOOK"):
        write_screen("fake-tui no hook")
        return
    emit_hook(args, prompt)


def consume_input(args: argparse.Namespace) -> None:
    pending = bytearray()
    prompt = bytearray()
    in_paste = False
    swallow_next_submit = os.environ.get("FAKE_TUI_SWALLOW_FIRST_CR") == "1"
    while True:
        chunk = os.read(sys.stdin.fileno(), 1024)
        if not chunk:
            return
        pending.extend(chunk)

        while pending:
            pending_bytes = bytes(pending)
            if not in_paste:
                if pending_bytes.startswith(BRACKETED_PASTE_START):
                    del pending[: len(BRACKETED_PASTE_START)]
                    in_paste = True
                    continue
                if BRACKETED_PASTE_START.startswith(pending_bytes):
                    break
            if in_paste:
                if pending_bytes.startswith(BRACKETED_PASTE_END):
                    del pending[: len(BRACKETED_PASTE_END)]
                    in_paste = False
                    continue
                if BRACKETED_PASTE_END.startswith(pending_bytes):
                    break

            byte = pending.pop(0)
            if in_paste:
                prompt.append(byte)
            elif byte in {10, 13}:
                if prompt:
                    if swallow_next_submit:
                        swallow_next_submit = False
                        continue
                    text = prompt.decode(errors="replace")
                    prompt.clear()
                    handle_prompt(args, text)
            else:
                prompt.append(byte)


def main() -> int:
    args = parse_args()
    set_raw_stdin()
    startup = os.environ.get("FAKE_TUI_STARTUP", "ok")
    if startup == "exit2":
        write_screen("fake-tui startup exit2")
        return 2
    if startup == "autherr":
        write_screen("Invalid API key . Please run /login")
        return 1

    if os.environ.get("FAKE_TUI_TRUST") == "1":
        write_screen("Do you trust the files in this folder?")
        read_until_submit()
    slow_start = os.environ.get("FAKE_TUI_SLOW_START")
    if slow_start:
        time.sleep(float(slow_start))
    emit_session_start(args)
    write_screen("fake-tui ready")
    if os.environ.get("FAKE_TUI_STALL") == "1":
        while True:
            time.sleep(60.0)
    consume_input(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
