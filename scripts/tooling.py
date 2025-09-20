"""Command entry points for project automation tasks."""

from __future__ import annotations

import shlex
import subprocess
from collections.abc import Iterable, Sequence


def _invoke(command: Sequence[str]) -> None:
    """Run ``command`` and exit on failure."""

    print(f"→ {' '.join(shlex.quote(token) for token in command)}", flush=True)
    result = subprocess.run(command, check=False)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def _run_all(commands: Iterable[Sequence[str]]) -> None:
    for command in commands:
        _invoke(command)


def lint() -> None:
    """Run static analysis suite."""

    _run_all([("ruff", "check"), ("black", "--check", ".")])


def format_code() -> None:
    """Format the codebase with Black."""

    _invoke(("black", "."))


def typecheck() -> None:
    """Execute mypy type checks."""

    _invoke(("mypy", "src"))


def tests() -> None:
    """Run the pytest suite."""

    _invoke(("pytest",))


def ci() -> None:
    """Execute the full CI toolchain locally."""

    try:
        lint()
        typecheck()
        tests()
    except SystemExit as exc:
        raise SystemExit(exc.code) from None
