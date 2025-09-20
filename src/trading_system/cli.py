"""Command line interface for the trading system toolkit."""

import os
import shutil
import sys
from collections.abc import Iterable
from pathlib import Path

import typer
from pydantic import ValidationError
from rich.console import Console
from rich.table import Table

from trading_system.config import Config, load_config

app = typer.Typer(help="Utilities for research, reporting, and operations.")
config_app = typer.Typer(help="Configuration management commands.")
app.add_typer(config_app, name="config")
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


@config_app.command("inspect")
def config_inspect(
    path: Path = typer.Option(  # noqa: B008 - CLI option definition
        ...,
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="Path to configuration file.",
    )
) -> None:
    """Validate a config file and print key details."""

    try:
        config = load_config(path)
    except (
        FileNotFoundError,
        ValidationError,
        ValueError,
    ) as exc:  # pragma: no cover - defensive
        console.print(f"[red]Configuration invalid:[/] {exc}")
        raise typer.Exit(code=1) from exc

    _print_config_summary(config, path)


@config_app.command("new")
def config_new(
    path: Path = typer.Option(  # noqa: B008 - CLI option definition
        ...,
        file_okay=True,
        dir_okay=False,
        writable=True,
        help="Destination path for generated config.",
    ),
    template: str = typer.Option("default", help="Configuration template to use."),
    force: bool = typer.Option(
        False, help="Overwrite existing file if it already exists."
    ),
) -> None:
    """Generate a configuration file from the default template."""

    if template != "default":
        console.print(f"[red]Unknown template:[/] {template}")
        raise typer.Exit(code=1)

    if path.exists() and not force:
        console.print(
            f"[red]Refusing to overwrite existing file:[/] {path}. Use --force to replace."
        )
        raise typer.Exit(code=1)

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_DEFAULT_CONFIG_TEMPLATE.strip() + "\n", encoding="utf-8")
    console.print(f"[green]Wrote configuration template to[/green] {path}")


def _print_config_summary(config: Config, path: Path) -> None:
    table = Table("section", "summary")
    table.add_row("Base", f"ccy={config.base_ccy}, calendar={config.calendar}")
    table.add_row(
        "Data",
        f"provider={config.data.provider}, lookback={config.data.lookback_days or 0}d",
    )
    table.add_row("Universe", ", ".join(config.universe.tickers))
    table.add_row(
        "Paths", " | ".join(str(directory) for directory in config.paths.directories)
    )
    notify_channels: list[str] = []
    if config.notify.email:
        notify_channels.append(f"email={config.notify.email}")
    if config.notify.slack_webhook:
        notify_channels.append("slack")
    table.add_row("Notify", ", ".join(notify_channels) or "None")

    console.print(f"Inspecting configuration: [bold]{path}[/bold]")
    console.print(table)


_DEFAULT_CONFIG_TEMPLATE = """
# Default trading-system configuration
base_ccy: USD
calendar: NYSE
data:
  provider: yahoo
  adjust: splits_dividends
  lookback_days: 420
universe:
  tickers: [AAPL, MSFT, NVDA, SPY]
strategy:
  type: trend_follow
  entry: "close > sma_100"
  exit: "close < sma_100"
  rank: "momentum_63d"
risk:
  crash_threshold_pct: -0.08
  drawdown_threshold_pct: -0.20
  market_filter:
    benchmark: SPY
    rule: "close > sma_200"
rebalance:
  cadence: monthly
  max_positions: 8
  equal_weight: true
  min_weight: 0.05
  cash_buffer: 0.05
  turnover_cap_pct: 0.40
notify:
  email: ops@example.com
  slack_webhook: "https://hooks.slack.com/services/XXXXX/XXXXX/XXXXX"
paths:
  data_raw: data/raw
  data_curated: data/curated
  reports: reports
preprocess:
  forward_fill_limit: 1
  rolling_peak_window: 252
"""


def main() -> None:
    """Entry-point for the Typer CLI."""

    app()


if __name__ == "__main__":  # pragma: no cover - CLI passthrough
    main()
