"""Command line interface for the trading system toolkit."""

import os
import shutil
import sys
from collections.abc import Iterable

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(help="Utilities for research, reporting, and operations.")
console = Console()

_DEFAULT_DOCTOR_TOOLS: tuple[str, ...] = (
    "poetry",
    "python",
    "black",
    "ruff",
    "mypy",
    "pytest",
)


@app.command()
def version() -> None:
    """Show the current package version."""

    from trading_system import __version__

    console.print(f"trading-system version: [bold green]{__version__}[/bold green]")


@app.command()
def info() -> None:
    """Display a short project summary."""

    console.print(
        "Mid/long-horizon trading workflow with daily risk alerts and periodic rebalances."
    )


def _tools_to_check() -> tuple[str, ...]:
    """Resolve the list of tools the doctor command should verify."""

    override = os.environ.get("TS_DOCTOR_REQUIRED")
    if override:
        tools = [tool.strip() for tool in override.split(",") if tool.strip()]
        if tools:
            return tuple(dict.fromkeys(tools))
    return _DEFAULT_DOCTOR_TOOLS


def _check_tools(tools: Iterable[str]) -> list[tuple[str, str, str | None]]:
    """Return a table-friendly structure describing tool availability."""

    rows: list[tuple[str, str, str | None]] = []
    for tool in tools:
        location = shutil.which(tool)
        if location:
            status = "[green]found[/green]"
        else:
            status = "[red]missing[/red]"
        rows.append((tool, status, location))
    return rows


@app.command()
def doctor() -> None:
    """Verify required tooling and environment prerequisites."""

    tools = _tools_to_check()
    rows = _check_tools(tools)

    table = Table("tool", "status", "location")
    for tool, status, location in rows:
        table.add_row(tool, status, location or "—")

    console.print(table)

    python_row = next((row for row in rows if row[0] == "python"), None)
    if python_row and python_row[2] and python_row[2] != sys.executable:
        console.print(
            "[yellow]Note:[/] active interpreter differs from located python executable:\n"
            f"  listed: {python_row[2]}\n  active: {sys.executable}"
        )

    missing = [tool for tool, status, _ in rows if "missing" in status]
    if missing:
        console.print(
            "[red]Environment check failed.[/] Missing tools: " + ", ".join(missing)
        )
        raise typer.Exit(code=1)

    console.print("[green]All required tooling available.[/green]")


def main() -> None:
    """Entry-point for the Typer CLI."""

    app()


if __name__ == "__main__":  # pragma: no cover - CLI passthrough
    main()
