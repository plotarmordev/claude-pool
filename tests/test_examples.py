from __future__ import annotations

from pathlib import Path
import py_compile
import re


ROOT = Path(__file__).resolve().parents[1]


def source_root() -> Path | None:
    if (ROOT / "pyproject.toml").exists():
        return ROOT
    return None


def assert_installed_import() -> None:
    import claude_pool

    assert claude_pool.__name__ == "claude_pool"


def test_python_examples_compile() -> None:
    root = source_root()
    if root is None:
        assert_installed_import()
        return

    for path in sorted((root / "examples").glob("*.py")):
        py_compile.compile(str(path), doraise=True)


def test_shell_example_mentions_cli_commands() -> None:
    root = source_root()
    if root is None:
        assert_installed_import()
        return

    text = (root / "examples" / "shell.md").read_text()
    for command in ("serve", "ask", "status", "doctor"):
        assert command in text


def test_readme_quickstart_compiles() -> None:
    root = source_root()
    if root is None:
        assert_installed_import()
        return

    readme = (root / "README.md").read_text()
    quickstart = readme.split("## Quickstart", 1)[1]
    match = re.search(r"```python\n(.*?)```", quickstart, re.DOTALL)

    assert match is not None
    compile(match.group(1), "README.md quickstart", "exec")
