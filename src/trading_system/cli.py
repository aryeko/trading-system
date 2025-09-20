"""Command line interface for the trading system toolkit."""

import json
import os
import shutil
import sys
from collections.abc import Callable, Iterable
from datetime import date
from pathlib import Path

import typer
from pydantic import ValidationError
from rich.console import Console
from rich.table import Table

from trading_system.config import Config, load_config
from trading_system.data import DataProvider, YahooDataProvider, run_data_pull
from trading_system.data.storage import DataRunMeta

app = typer.Typer(help="Utilities for research, reporting, and operations.")
config_app = typer.Typer(help="Configuration management commands.")
data_app = typer.Typer(help="Raw data acquisition commands.")
app.add_typer(config_app, name="config")
app.add_typer(data_app, name="data")
console = Console()

_DEFAULT_DOCTOR_TOOLS: tuple[str, ...] = (
    "poetry",
    "python",
    "black",
    "ruff",
    "mypy",
    "pytest",
)

ProviderFactory = Callable[[], DataProvider]

DATA_PROVIDER_FACTORIES: dict[str, ProviderFactory] = {
    "yahoo": YahooDataProvider,
}

_PROVIDER_DESCRIPTIONS: dict[str, str] = {
    "yahoo": "Yahoo Finance CSV daily bars (OHLCV + benchmark)",
    "eodhd": "EOD Historical Data (adapter planned)",
}


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


def _resolve_provider(name: str) -> DataProvider:
    factory = DATA_PROVIDER_FACTORIES.get(name)
    if factory is None:
        available = ", ".join(sorted(DATA_PROVIDER_FACTORIES))
        console.print(
            f"[red]Unknown provider:[/] {name}. Available providers: {available}"
        )
        raise typer.Exit(code=1)
    return factory()


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


@data_app.command("providers")
def data_providers() -> None:
    """List available and planned raw data providers."""

    table = Table("name", "status", "details")
    for name, description in sorted(_PROVIDER_DESCRIPTIONS.items()):
        available = name in DATA_PROVIDER_FACTORIES
        status = "[green]available[/green]" if available else "[yellow]planned[/yellow]"
        table.add_row(name, status, description)
    console.print(table)


@data_app.command("pull")
def data_pull(
    config_path: Path = typer.Option(  # noqa: B008 - CLI option definition
        ...,
        "--config",
        "--config-path",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="Config file to load.",
    ),
    as_of: str = typer.Option(  # noqa: B008 - CLI option definition
        ..., help="As-of date for the pull (YYYY-MM-DD)."
    ),
    provider: str | None = typer.Option(
        None,
        help="Override data provider (defaults to config data.provider).",
    ),
    skip_benchmark: bool = typer.Option(
        False, help="Skip benchmark download even if configured."
    ),
) -> None:
    """Fetch raw bars for the configured universe and persist artifacts."""

    config = load_config(config_path)
    provider_name = provider or config.data.provider
    provider_instance = _resolve_provider(provider_name)

    try:
        as_of_date = date.fromisoformat(as_of)
    except ValueError as exc:
        console.print(f"[red]Invalid as-of date:[/] {as_of}")
        raise typer.Exit(code=1) from exc

    console.print(
        f"Pulling data for [bold]{as_of_date}[/bold] via provider [bold]{provider_name}[/bold]"
    )

    try:
        result = run_data_pull(
            config,
            provider_instance,
            as_of=as_of_date,
            include_benchmark=not skip_benchmark,
        )
    except Exception as exc:  # pragma: no cover - defensive log surface
        console.print(f"[red]Data pull failed:[/] {exc}")
        raise typer.Exit(code=1) from exc

    _print_data_pull_summary(result, include_benchmark=not skip_benchmark)


def _print_data_pull_summary(result: DataRunMeta, *, include_benchmark: bool) -> None:
    console.print(f"[green]Raw data written to[/green] {result.directory}")
    table = Table("field", "value")
    table.add_row("timestamp", result.timestamp.isoformat())
    table.add_row("symbols", ", ".join(result.symbols) or "—")
    table.add_row("last_bar", _format_meta_value(result.last_bar_date))
    table.add_row(
        "window",
        f"{_format_meta_value(result.start)} → {_format_meta_value(result.end)}",
    )
    benchmark_value = (
        result.benchmark
        if include_benchmark and result.benchmark
        else ("skipped" if not include_benchmark else "—")
    )
    table.add_row("benchmark", benchmark_value)
    console.print(table)


def _format_meta_value(value: object) -> str:
    if value is None:
        return "—"
    return str(value)


@data_app.command("inspect")
def data_inspect(
    run: Path = typer.Option(  # noqa: B008 - CLI option definition
        ...,
        exists=True,
        readable=True,
        help="Run directory or meta_run.json to inspect.",
    )
) -> None:
    """Summarize a persisted raw data run."""

    if run.is_dir():
        run_dir = run
        meta_path = run_dir / "meta_run.json"
    else:
        if run.name != "meta_run.json":
            console.print(
                "[red]Provided path must be a run directory or meta_run.json file.[/]"
            )
            raise typer.Exit(code=1)
        run_dir = run.parent
        meta_path = run

    if not meta_path.is_file():
        console.print(f"[red]meta_run.json not found in:[/] {run_dir}")
        raise typer.Exit(code=1)

    meta = json.loads(meta_path.read_text(encoding="utf-8"))

    console.print(f"Inspecting raw run: [bold]{run_dir}[/bold]")

    meta_table = Table("field", "value")
    for key in ("timestamp", "symbols", "start", "end", "last_bar_date", "benchmark"):
        value = meta.get(key)
        if isinstance(value, list):
            formatted = ", ".join(str(item) for item in value)
        else:
            formatted = value if value is not None else "—"
        meta_table.add_row(key, str(formatted))
    console.print(meta_table)

    parquet_files = sorted(run_dir.glob("*.parquet"))
    if parquet_files:
        file_table = Table("file", "size")
        for file_path in parquet_files:
            size = file_path.stat().st_size
            file_table.add_row(file_path.name, f"{size} bytes")
        console.print(file_table)
    else:
        console.print("[yellow]No parquet files found for this run.[/yellow]")


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
