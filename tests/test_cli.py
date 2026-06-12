from __future__ import annotations

import json
from pathlib import Path
import socket
import stat
import subprocess
import sys
import threading
import time
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
FAKE = ROOT / "tests" / "fake_claude.py"


def cli_args(*args: str) -> list[str]:
    return [sys.executable, "-m", "claude_pool", *args]


def run_cli(args: list[str], *, timeout: float = 15.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cli_args(*args),
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )


def wait_for_socket(path: Path) -> None:
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if path.exists():
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                    client.connect(str(path))
                return
            except OSError:
                pass
        time.sleep(0.05)
    raise AssertionError(f"socket did not appear: {path}")


def start_server(tmp_path: Path, *extra_args: str) -> tuple[subprocess.Popen[str], Path]:
    socket_path = tmp_path / "claude-pool.sock"
    process = subprocess.Popen(
        cli_args(
            "serve",
            "--socket",
            str(socket_path),
            "--claude-bin",
            str(FAKE),
            *extra_args,
        ),
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    wait_for_socket(socket_path)
    return process, socket_path


def stop_server(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10.0)
    except subprocess.TimeoutExpired as exc:
        process.kill()
        process.wait(timeout=10.0)
        raise AssertionError("server did not terminate") from exc


def request(socket_path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.connect(str(socket_path))
        with client.makefile("rwb") as file:
            file.write(json.dumps(payload, separators=(",", ":")).encode() + b"\n")
            file.flush()
            line = file.readline()
    assert line
    response = json.loads(line.decode())
    assert isinstance(response, dict)
    return response


def parse_status(stdout: str) -> dict[str, str]:
    parsed = {}
    for line in stdout.splitlines():
        if ": " in line:
            key, value = line.split(": ", 1)
            parsed[key] = value
    return parsed


def wait_for_warm_status(socket_path: Path) -> dict[str, str]:
    deadline = time.monotonic() + 5.0
    last = None
    while time.monotonic() < deadline:
        completed = run_cli(["status", "--socket", str(socket_path)])
        last = completed
        if completed.returncode == 0:
            status = parse_status(completed.stdout)
            if int(status.get("warm", "0")) >= 1:
                return status
        time.sleep(0.1)
    raise AssertionError(f"warm status not reached: {last.stdout if last else ''}")


def fake_process_ids() -> list[int]:
    marker = str(FAKE).encode()
    proc = Path("/proc")
    if not proc.exists():
        return []
    pids = []
    for entry in proc.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            cmdline = (entry / "cmdline").read_bytes()
        except OSError:
            continue
        if marker in cmdline:
            pids.append(int(entry.name))
    return pids


def wait_for_no_fake_processes() -> None:
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        pids = fake_process_ids()
        if not pids:
            return
        time.sleep(0.05)
    raise AssertionError(f"fake claude processes remain: {fake_process_ids()}")


def join_threads(threads: list[threading.Thread], timeout: float = 15.0) -> None:
    for thread in threads:
        thread.join(timeout=timeout)
        assert not thread.is_alive(), f"{thread.name} did not finish"


def test_serve_and_ask_round_trip(tmp_path: Path) -> None:
    process, socket_path = start_server(tmp_path, "--warm", "0")
    try:
        completed = run_cli(["ask", "hello", "--socket", str(socket_path)])

        assert completed.returncode == 0
        assert completed.stdout.strip() == "hello"
    finally:
        stop_server(process)
        wait_for_no_fake_processes()


def test_raw_connection_handles_two_sequential_asks(tmp_path: Path) -> None:
    process, socket_path = start_server(tmp_path, "--warm", "0")
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.connect(str(socket_path))
            with client.makefile("rwb") as file:
                for prompt in ("one", "two"):
                    payload = {"op": "ask", "prompt": prompt}
                    file.write(json.dumps(payload, separators=(",", ":")).encode() + b"\n")
                    file.flush()
                    response = json.loads(file.readline().decode())

                    assert response["ok"] is True
                    assert response["text"] == prompt
    finally:
        stop_server(process)
        wait_for_no_fake_processes()


def test_concurrent_cli_asks(tmp_path: Path) -> None:
    process, socket_path = start_server(tmp_path, "--warm", "0", "--max-workers", "4")
    completions: list[subprocess.CompletedProcess[str]] = []
    errors: list[BaseException] = []
    lock = threading.Lock()

    def ask_worker(index: int) -> None:
        try:
            completed = run_cli(
                ["ask", f"prompt-{index}", "--socket", str(socket_path)],
                timeout=20.0,
            )
        except BaseException as exc:
            with lock:
                errors.append(exc)
        else:
            with lock:
                completions.append(completed)

    threads = [
        threading.Thread(target=ask_worker, args=(index,), name=f"cli-ask-{index}")
        for index in range(4)
    ]
    for thread in threads:
        thread.start()
    try:
        join_threads(threads)

        assert not errors
        assert len(completions) == 4
        assert {completed.stdout.strip() for completed in completions} == {
            "prompt-0",
            "prompt-1",
            "prompt-2",
            "prompt-3",
        }
        assert all(completed.returncode == 0 for completed in completions)
    finally:
        join_threads(threads)
        stop_server(process)
        wait_for_no_fake_processes()


def test_error_result_prints_text_and_exits_one(tmp_path: Path) -> None:
    process, socket_path = start_server(tmp_path, "--warm", "0")
    try:
        completed = run_cli(["ask", "ERROR", "--socket", str(socket_path)])

        assert completed.returncode == 1
        assert completed.stdout.strip() == "error"
    finally:
        stop_server(process)
        wait_for_no_fake_processes()


def test_ask_timeout_reports_error(tmp_path: Path) -> None:
    process, socket_path = start_server(tmp_path, "--warm", "0")
    try:
        completed = run_cli(
            ["ask", "SLEEP:30", "--timeout", "0.5", "--socket", str(socket_path)],
            timeout=10.0,
        )

        assert completed.returncode == 1
        assert "AskTimeout" in completed.stderr
    finally:
        stop_server(process)
        wait_for_no_fake_processes()


def test_malformed_line_keeps_connection_open(tmp_path: Path) -> None:
    process, socket_path = start_server(tmp_path, "--warm", "0")
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.connect(str(socket_path))
            with client.makefile("rwb") as file:
                file.write(b"notjson\n")
                file.flush()
                bad = json.loads(file.readline().decode())

                file.write(b'{"op":"ask","prompt":"after-bad"}\n')
                file.flush()
                good = json.loads(file.readline().decode())

        assert bad["ok"] is False
        assert bad["kind"] == "BadRequest"
        assert good["ok"] is True
        assert good["text"] == "after-bad"
    finally:
        stop_server(process)
        wait_for_no_fake_processes()


def test_status_reports_warm_pid_and_profile(tmp_path: Path) -> None:
    process, socket_path = start_server(tmp_path, "--warm", "1")
    try:
        status = wait_for_warm_status(socket_path)

        assert int(status["warm"]) >= 1
        assert int(status["pid"]) == process.pid
        assert status["profile.claude_bin"] == str(FAKE)
    finally:
        stop_server(process)
        wait_for_no_fake_processes()


def test_socket_file_mode_is_0600(tmp_path: Path) -> None:
    process, socket_path = start_server(tmp_path, "--warm", "0")
    try:
        assert stat.S_IMODE(socket_path.stat().st_mode) == 0o600
    finally:
        stop_server(process)
        wait_for_no_fake_processes()


def test_sigterm_gracefully_stops_server_and_workers(tmp_path: Path) -> None:
    process, socket_path = start_server(tmp_path, "--warm", "1")
    try:
        wait_for_warm_status(socket_path)
        process.terminate()
        assert process.wait(timeout=10.0) == 0

        assert not socket_path.exists()
        wait_for_no_fake_processes()
    finally:
        stop_server(process)
        wait_for_no_fake_processes()


def test_doctor_missing_binary_reports_diagnosis() -> None:
    completed = run_cli(["doctor", "--claude-bin", "/nonexistent/claude"], timeout=10.0)
    combined = completed.stdout + completed.stderr

    assert completed.returncode != 0
    assert "not found" in combined
    assert "Diagnosis" in combined


def test_doctor_against_fake_reports_latency_and_session_id() -> None:
    completed = run_cli(
        ["doctor", "--claude-bin", str(FAKE), "--timeout", "5"],
        timeout=15.0,
    )

    assert completed.returncode == 0
    assert "round_trip_ms:" in completed.stdout
    assert "session_id: fake-" in completed.stdout


def test_ask_without_server_fails_helpfully(tmp_path: Path) -> None:
    socket_path = tmp_path / "missing.sock"
    completed = run_cli(["ask", "hello", "--socket", str(socket_path)])

    assert completed.returncode == 1
    assert "failed to contact claude-pool server" in completed.stderr
