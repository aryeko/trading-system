"""Command line interface for the trading system toolkit."""

from __future__ import annotations

import typer
from rich.console import Console

app = typer.Typer(help="Utilities for research, reporting, and operations.")
console = Console()


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


def main() -> None:
    """Entry-point for the Typer CLI."""

    app()


if __name__ == "__main__":  # pragma: no cover - CLI passthrough
    main()
