"""Command line interface for the trading system toolkit."""

import json
import math
import os
import shutil
import sys
import webbrowser
from collections.abc import Callable, Iterable, Mapping
from datetime import date
from numbers import Real
from pathlib import Path
from typing import Any

import pandas as pd
import typer
from pydantic import ValidationError
from rich.console import Console
from rich.table import Table

from trading_system.backtest import BacktestEngine
from trading_system.config import Config, load_config
from trading_system.data import DataProvider, YahooDataProvider, run_data_pull
from trading_system.data.storage import DataRunMeta
from trading_system.notify import (
    NotificationService,
    NotificationStatus,
    load_report_summary,
)
from trading_system.orchestrator import (
    PipelineExecutionError,
    PipelineSummary,
    pipeline_logging,
    run_daily_pipeline,
    run_rebalance_pipeline,
)
from trading_system.preprocess import Preprocessor, PreprocessResult
from trading_system.rebalance import RebalanceEngine, RebalanceResult
from trading_system.report import ReportBuilder, ReportResult
from trading_system.risk import HoldingsSnapshot, RiskEngine, load_holdings
from trading_system.signals import StrategyEngine

app = typer.Typer(help="Utilities for research, reporting, and operations.")
config_app = typer.Typer(help="Configuration management commands.")
data_app = typer.Typer(help="Raw data acquisition commands.")
signals_app = typer.Typer(help="Strategy signal evaluation commands.")
rebalance_app = typer.Typer(help="Rebalance proposal commands.")
report_app = typer.Typer(help="Daily report generation commands.")
risk_app = typer.Typer(help="Risk evaluation commands.")
notify_app = typer.Typer(help="Notification delivery commands.")
backtest_app = typer.Typer(help="Backtesting commands.")
run_app = typer.Typer(help="Pipeline orchestration commands.")
app.add_typer(config_app, name="config")
app.add_typer(data_app, name="data")
app.add_typer(signals_app, name="signals")
app.add_typer(rebalance_app, name="rebalance")
app.add_typer(report_app, name="report")
app.add_typer(risk_app, name="risk")
app.add_typer(notify_app, name="notify")
app.add_typer(backtest_app, name="backtest")
app.add_typer(run_app, name="run")
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

_PIPELINE_STEPS: tuple[tuple[str, str, str], ...] = (
    (
        "data pull",
        "Fetch raw OHLCV data for the configured universe",
        "data/raw/<asof>/*.parquet",
    ),
    (
        "data preprocess",
        "Derive indicators and curated datasets",
        "data/curated/<asof>/*.parquet",
    ),
    (
        "signals build",
        "Evaluate strategy rules and write signals parquet",
        "reports/<asof>/signals.parquet",
    ),
    (
        "risk evaluate",
        "Compute crash/drawdown alerts and market filter",
        "reports/<asof>/risk_alerts.json",
    ),
    (
        "rebalance propose",
        "Generate target weights and draft orders",
        "reports/<asof>/rebalance_proposal.json",
    ),
    (
        "report build",
        "Render the operator report payloads",
        "reports/<asof>/daily_report.{json,html,pdf}",
    ),
    (
        "notify send",
        "Deliver report summaries to configured channels",
        "email/slack dispatch (no artifact)",
    ),
    (
        "backtest run",
        "Historical simulation of the strategy (story S-11)",
        "reports/backtests/<run>/",
    ),
)


def _config_option() -> Any:
    return typer.Option(  # noqa: B008 - CLI option definition
        ...,
        "--config",
        "--config-path",
        envvar="TS_CONFIG_PATH",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="Config file to load (overridable via TS_CONFIG_PATH).",
    )


def _asof_option(help_text: str) -> Any:
    return typer.Option(  # noqa: B008 - CLI option definition
        ...,
        "--asof",
        help=help_text,
        envvar="TS_ASOF",
    )


def _holdings_option() -> Any:
    return typer.Option(  # noqa: B008 - CLI option definition
        ...,
        "--holdings",
        envvar="TS_HOLDINGS_PATH",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="Holdings snapshot JSON (overridable via TS_HOLDINGS_PATH).",
    )


def _dry_run_flag(help_text: str) -> Any:
    return typer.Option(False, "--dry-run", help=help_text)


def _force_flag(help_text: str) -> Any:
    return typer.Option(False, "--force", help=help_text)


def _channel_option(default: str = "all") -> Any:
    return typer.Option(
        default,
        "--channel",
        help="Notification channel(s) to target (email|slack|all or comma separated).",
    )


def _log_toggle() -> Any:
    return typer.Option(True, help="Write pipeline logs to reports/<asof>/run.log.")


def _resolve_log_path(config: Config, as_of_date: date, enabled: bool) -> Path | None:
    if not enabled:
        return None
    reports_dir = config.paths.reports / as_of_date.strftime("%Y-%m-%d")
    return reports_dir / "run.log"


def _print_pipeline_summary(summary: PipelineSummary | None) -> None:
    if summary is None:
        return

    state = "[green]SUCCESS[/green]" if summary.success else "[red]FAILED[/red]"
    console.print(
        f"{state} pipeline for [bold]{summary.as_of}[/bold] in {summary.duration:.2f}s"
    )

    if summary.steps:
        table = Table("step", "status", "duration", "details")
        for record in summary.steps:
            step_label = record.name.replace("_", " ")
            table.add_row(
                step_label,
                record.status,
                f"{record.duration:.2f}s",
                record.details or "—",
            )
        console.print(table)

    if summary.manifest:
        manifest_table = Table("artifact", "location")
        for key, value in sorted(summary.manifest.items()):
            manifest_table.add_row(key, value or "—")
        console.print("Manifest:")
        console.print(manifest_table)


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


@app.command("steps")
def steps() -> None:
    """List orchestrated pipeline steps and their primary artifacts."""

    table = Table("command", "description", "artifacts")
    for command, description, artifacts in _PIPELINE_STEPS:
        table.add_row(command, description, artifacts)
    console.print(table)


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


def _parse_as_of(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:  # pragma: no cover - defensive
        console.print(f"[red]Invalid as-of date:[/] {value}")
        raise typer.Exit(code=1) from exc


def _format_metric(value: Any) -> str:
    if value is None:
        return "—"
    if isinstance(value, Real) and not isinstance(value, bool):
        numeric = float(value)
        if math.isnan(numeric):
            return "nan"
        if math.isinf(numeric):
            return "∞" if numeric > 0 else "-∞"
        if abs(numeric) >= 1.0:
            return f"{numeric:,.2f}"
        return f"{numeric:.4f}"
    return str(value)


def _render_backtest_metrics(metrics: Mapping[str, Any]) -> None:
    fields = (
        "start",
        "end",
        "trading_days",
        "initial_cash",
        "final_equity",
        "total_return",
        "cagr",
        "volatility",
        "sharpe",
        "sortino",
        "max_drawdown",
        "hit_rate",
        "turnover_total",
        "turnover_average",
        "rebalance_events",
        "trades_executed",
    )
    table = Table("metric", "value")
    for field in fields:
        table.add_row(field, _format_metric(metrics.get(field)))
    console.print(table)


def _load_metrics_payload(path: Path) -> Mapping[str, Any]:
    target = path
    if target.is_dir():
        target = target / "metrics.json"
    if not target.is_file():
        console.print(f"[red]Metrics file not found:[/] {target}")
        raise typer.Exit(code=1)
    with target.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, Mapping):
        console.print("[red]Metrics payload must be a mapping.[/red]")
        raise typer.Exit(code=1)
    return payload


def _metrics_delta(baseline: Any, candidate: Any) -> Any:
    if (
        isinstance(baseline, Real)
        and isinstance(candidate, Real)
        and not isinstance(baseline, bool)
        and not isinstance(candidate, bool)
    ):
        return candidate - baseline
    return None


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

    as_of_date = _parse_as_of(as_of)

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


def _format_number(value: object) -> str:
    if value is None:
        return "—"
    if isinstance(value, Real):
        number = float(value)
    elif isinstance(value, str):
        try:
            number = float(value)
        except ValueError:
            return value
    else:
        return str(value)
    if math.isnan(number) or math.isinf(number):
        return str(number)
    return f"{number:.6f}".rstrip("0").rstrip(".")


def _load_signals_for_cli(
    config: Config,
    signals_path: Path | None,
    as_of_date: date,
    *,
    required: bool = True,
) -> pd.DataFrame | None:
    if signals_path is None:
        signals_path = (
            config.paths.reports / as_of_date.strftime("%Y-%m-%d") / "signals.parquet"
        )
    resolved = Path(signals_path)
    if not resolved.is_file():
        if required:
            console.print(f"[red]Signals file not found:[/] {resolved}")
            raise typer.Exit(code=1)
        return None
    try:
        frame = pd.read_parquet(resolved)
    except Exception as exc:  # pragma: no cover - defensive
        console.print(f"[red]Unable to read signals parquet:[/] {exc}")
        raise typer.Exit(code=1) from exc
    return frame


def _maybe_load_json(
    path: Path, *, required: bool, description: str
) -> tuple[Mapping[str, object] | None, Path | None]:
    if not path.is_file():
        if required:
            console.print(f"[red]{description} not found:[/] {path}")
            raise typer.Exit(code=1)
        return None, None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        console.print(f"[red]Failed to parse {description} JSON:[/] {exc}")
        raise typer.Exit(code=1) from exc
    if not isinstance(payload, Mapping):
        console.print(f"[red]{description} must be a JSON object.[/]")
        raise typer.Exit(code=1)
    return payload, path


def _print_rebalance_summary(
    result: RebalanceResult, *, max_targets: int | None = None
) -> None:
    console.print(f"[bold]{result.status}[/bold] for {result.as_of}")
    console.print(
        f"Turnover: {_format_number(result.turnover)} | Cash buffer: {_format_number(result.cash_buffer)}"
    )

    if result.targets:
        target_table = Table("symbol", "target_weight", "rationale")
        targets = list(result.targets)
        if max_targets is not None and max_targets >= 0:
            subset = targets[:max_targets]
        else:
            subset = targets
        for target in subset:
            target_table.add_row(
                target.symbol,
                _format_number(target.target_weight),
                target.rationale or "—",
            )
        console.print(target_table)
        if max_targets is not None and max_targets >= 0 and len(targets) > max_targets:
            console.print(
                f"[yellow]… {len(targets) - max_targets} additional targets omitted[/yellow]"
            )
    else:
        console.print("[yellow]No targets proposed.[/yellow]")

    if result.orders:
        order_table = Table("symbol", "side", "quantity", "notional")
        for order in result.orders:
            order_table.add_row(
                order.symbol,
                order.side,
                _format_number(order.quantity),
                _format_number(order.notional),
            )
        console.print(order_table)
    else:
        console.print("[yellow]No orders generated.[/yellow]")

    if result.notes:
        console.print("Notes:")
        for note in result.notes:
            console.print(f"  • {note}")


def _print_report_summary(result: ReportResult) -> None:
    payload = result.payload
    portfolio = payload.get("portfolio", {})
    actions = payload.get("actions", {})
    console.print(f"[bold]Report generated for {result.as_of}[/bold]")
    console.print(
        "Value: "
        + _format_number(portfolio.get("value"))
        + " | Cash: "
        + _format_number(portfolio.get("cash"))
        + " | Positions: "
        + str(len(portfolio.get("positions", [])))
    )
    console.print(
        "Orders: "
        + str(len(actions.get("orders", [])))
        + " | Status: "
        + str(actions.get("status", "—"))
        + " | Turnover: "
        + _format_number(actions.get("turnover"))
    )
    if result.notes:
        console.print("Notes:")
        for note in result.notes:
            console.print(f"  • {note}")


def _generate_report_result(
    config: Config,
    as_of_date: date,
    holdings: HoldingsSnapshot,
    *,
    holdings_path: Path,
    risk_path_option: Path | None,
    proposal_path_option: Path | None,
    signals_path_option: Path | None,
    include_pdf: bool,
) -> ReportResult:
    reports_dir = config.paths.reports / as_of_date.strftime("%Y-%m-%d")

    risk_candidate = risk_path_option or reports_dir / "risk_alerts.json"
    risk_payload, risk_artifact_path = _maybe_load_json(
        risk_candidate,
        required=risk_path_option is not None,
        description="Risk alerts",
    )

    proposal_candidate = proposal_path_option or reports_dir / "rebalance_proposal.json"
    proposal_payload, proposal_artifact_path = _maybe_load_json(
        proposal_candidate,
        required=proposal_path_option is not None,
        description="Rebalance proposal",
    )

    signals_candidate = (
        signals_path_option
        if signals_path_option is not None
        else reports_dir / "signals.parquet"
    )
    signals_frame = _load_signals_for_cli(
        config,
        signals_path_option,
        as_of_date,
        required=signals_path_option is not None,
    )
    signals_artifact_path = None
    if signals_frame is not None:
        candidate_path = Path(signals_candidate)
        if candidate_path.is_file():
            signals_artifact_path = candidate_path

    builder = ReportBuilder(config)
    return builder.build(
        as_of_date,
        holdings=holdings,
        holdings_path=holdings_path,
        risk_payload=risk_payload,
        risk_path=risk_artifact_path,
        proposal_payload=proposal_payload,
        proposal_path=proposal_artifact_path,
        signals=signals_frame,
        signals_path=signals_artifact_path,
        include_pdf=include_pdf,
    )


def _notify_channels(value: str) -> tuple[str, ...]:
    channels = [part.strip() for part in value.split(",") if part.strip()]
    if not channels:
        return ("all",)
    return tuple(channels)


def _print_notification_status(status: NotificationStatus, *, dry_run: bool) -> None:
    label = status.channel.capitalize()
    if status.delivered:
        console.print(f"[green]{label} notification ready.[/green]")
    else:
        message = status.details or "Delivery failed."
        console.print(f"[red]{label} notification failed:[/] {message}")
    if dry_run and status.details:
        console.print(status.details)


@notify_app.command("send")
def notify_send(
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
        ..., help="As-of date for the notification (YYYY-MM-DD)."
    ),
    channel: str = typer.Option(
        "all",
        "--channel",
        help="Which channels to use (email|slack|all or comma separated).",
    ),
    dry_run: bool = typer.Option(
        False, help="Render notifications without sending them."
    ),
) -> None:
    """Dispatch notifications referencing the generated report artifacts."""

    config = load_config(config_path)
    as_of_date = _parse_as_of(as_of)
    summary = load_report_summary(config, as_of_date)
    service = NotificationService()

    requested_channels = _notify_channels(channel)
    statuses: list[NotificationStatus] = []
    for group in requested_channels:
        statuses.extend(service.dispatch(summary, config, [group], dry_run=dry_run))

    success = any(status.delivered for status in statuses)
    for status in statuses:
        _print_notification_status(status, dry_run=dry_run)

    if not success:
        raise typer.Exit(code=1)


@notify_app.command("preview")
def notify_preview(
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
        ..., help="As-of date for the notification (YYYY-MM-DD)."
    ),
    channel: str = typer.Option(
        "all",
        "--channel",
        help="Which channel payload to preview (email|slack|all).",
    ),
) -> None:
    """Preview notification payloads without sending them."""

    config = load_config(config_path)
    as_of_date = _parse_as_of(as_of)
    summary = load_report_summary(config, as_of_date)
    service = NotificationService()

    requested_channels = _notify_channels(channel)
    statuses: list[NotificationStatus] = []
    for group in requested_channels:
        statuses.extend(service.dispatch(summary, config, [group], dry_run=True))

    for status in statuses:
        _print_notification_status(status, dry_run=True)

    if not any(status.delivered for status in statuses):
        raise typer.Exit(code=1)


@run_app.command("daily")
def run_daily(
    config_path: Path = _config_option(),  # noqa: B008 - Typer option factory
    holdings_path: Path = _holdings_option(),  # noqa: B008 - Typer option factory
    as_of: str = _asof_option("As-of date for the daily pipeline (YYYY-MM-DD)."),
    dry_run: bool = _dry_run_flag(
        "Skip actual notification delivery while still generating artifacts."
    ),
    force: bool = _force_flag(
        "Overwrite curated/report artifacts and ignore cadence checks."
    ),
    channel: str = _channel_option(),  # noqa: B008 - Typer option factory
    log_to_file: bool = _log_toggle(),  # noqa: B008 - Typer option factory
) -> None:
    """Run the daily workflow (pull → preprocess → risk → report → notify)."""

    config = load_config(config_path)
    as_of_date = _parse_as_of(as_of)
    provider = _resolve_provider(config.data.provider)
    holdings = load_holdings(holdings_path)
    channels = _notify_channels(channel)
    log_path = _resolve_log_path(config, as_of_date, log_to_file)

    summary: PipelineSummary | None = None
    try:
        with pipeline_logging(log_path):
            summary = run_daily_pipeline(
                config=config,
                provider=provider,
                as_of=as_of_date,
                holdings=holdings,
                holdings_path=holdings_path,
                dry_run=dry_run,
                force=force,
                channels=channels,
            )
    except PipelineExecutionError as exc:
        _print_pipeline_summary(exc.summary)
        console.print(f"[red]Pipeline failed at step[/red] {exc.step}: {exc}")
        raise typer.Exit(code=1) from exc

    _print_pipeline_summary(summary)


@run_app.command("rebalance")
def run_rebalance(
    config_path: Path = _config_option(),  # noqa: B008 - Typer option factory
    holdings_path: Path = _holdings_option(),  # noqa: B008 - Typer option factory
    as_of: str = _asof_option("As-of date for the rebalance pipeline (YYYY-MM-DD)."),
    dry_run: bool = _dry_run_flag(
        "Skip actual notification delivery while still generating artifacts."
    ),
    force: bool = _force_flag("Force proposal generation even if cadence is not met."),
    channel: str = _channel_option(),  # noqa: B008 - Typer option factory
    log_to_file: bool = _log_toggle(),  # noqa: B008 - Typer option factory
) -> None:
    """Run the rebalance workflow including signals and proposal generation."""

    config = load_config(config_path)
    as_of_date = _parse_as_of(as_of)
    provider = _resolve_provider(config.data.provider)
    holdings = load_holdings(holdings_path)
    channels = _notify_channels(channel)
    log_path = _resolve_log_path(config, as_of_date, log_to_file)

    summary: PipelineSummary | None = None
    try:
        with pipeline_logging(log_path):
            summary = run_rebalance_pipeline(
                config=config,
                provider=provider,
                as_of=as_of_date,
                holdings=holdings,
                holdings_path=holdings_path,
                dry_run=dry_run,
                force=force,
                channels=channels,
            )
    except PipelineExecutionError as exc:
        _print_pipeline_summary(exc.summary)
        console.print(f"[red]Pipeline failed at step[/red] {exc.step}: {exc}")
        raise typer.Exit(code=1) from exc

    _print_pipeline_summary(summary)


@backtest_app.command("run")
def backtest_run(
    config_path: Path = _config_option(),  # noqa: B008 - Typer option factory
    start: str = typer.Option(
        ...,
        "--start",
        help="Backtest start date (YYYY-MM-DD)",
        envvar="TS_BACKTEST_START",
    ),
    end: str = typer.Option(
        ..., "--end", help="Backtest end date (YYYY-MM-DD)", envvar="TS_BACKTEST_END"
    ),
    output_dir: Path = typer.Option(  # noqa: B008 - Typer option factory
        ..., "--output", help="Directory to store backtest artifacts"
    ),
    label: str | None = typer.Option(
        None,
        "--label",
        help="Optional scenario label stored alongside metrics.",
    ),
    dry_run: bool = _dry_run_flag(
        "Preview configuration without executing the backtest."
    ),
    no_chart: bool = typer.Option(
        False,
        "--no-chart",
        help="Disable Plotly equity/drawdown HTML output.",
    ),
) -> None:
    """Run the deterministic backtest harness."""

    config = load_config(config_path)
    start_date = _parse_as_of(start)
    end_date = _parse_as_of(end)
    engine = BacktestEngine(config)
    include_chart = False if no_chart else None

    result = engine.run(
        start=start_date,
        end=end_date,
        output_dir=output_dir,
        label=label,
        dry_run=dry_run,
        include_chart=include_chart,
    )

    _render_backtest_metrics(result.metrics)

    if dry_run:
        console.print("[yellow]Dry run complete — no artifacts were written.[/yellow]")
        return

    if result.manifest:
        manifest_table = Table("artifact", "location")
        for key, value in sorted(result.manifest.items()):
            manifest_table.add_row(key, value)
        console.print(manifest_table)

    final_equity = _format_metric(result.metrics.get("final_equity"))
    total_return = _format_metric(result.metrics.get("total_return"))
    console.print(
        f"[green]Backtest complete.[/green] Final equity {final_equity}, total return {total_return}."
    )
    if result.output_dir is not None:
        console.print(f"Artifacts available at: {result.output_dir}")


@backtest_app.command("compare")
def backtest_compare(
    baseline: Path = typer.Option(  # noqa: B008 - CLI option definition
        ...,
        "--baseline",
        exists=True,
        help="Baseline backtest directory or metrics.json",
    ),
    candidate: Path = typer.Option(  # noqa: B008 - CLI option definition
        ...,
        "--candidate",
        exists=True,
        help="Candidate backtest directory or metrics.json",
    ),
) -> None:
    """Compare backtest metric outputs and show deltas."""

    baseline_metrics = _load_metrics_payload(baseline)
    candidate_metrics = _load_metrics_payload(candidate)

    keys = sorted(set(baseline_metrics) | set(candidate_metrics))
    table = Table("metric", "baseline", "candidate", "delta")
    for key in keys:
        base_value = baseline_metrics.get(key)
        cand_value = candidate_metrics.get(key)
        delta_value = _metrics_delta(base_value, cand_value)
        table.add_row(
            key,
            _format_metric(base_value),
            _format_metric(cand_value),
            _format_metric(delta_value),
        )

    console.print(table)


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


@data_app.command("preprocess")
def data_preprocess(
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
        ..., help="As-of date for preprocessing (YYYY-MM-DD)."
    ),
    dry_run: bool = typer.Option(
        False, help="Validate inputs without writing outputs."
    ),
    force: bool = typer.Option(
        False, help="Overwrite curated outputs if they already exist."
    ),
) -> None:
    """Run the preprocessing pipeline for a given as-of date."""

    config = load_config(config_path)
    as_of_date = _parse_as_of(as_of)
    raw_dir = config.paths.data_raw / as_of_date.isoformat()
    curated_dir = config.paths.data_curated / as_of_date.isoformat()

    if not raw_dir.is_dir():
        console.print(f"[red]Raw data directory not found:[/] {raw_dir}")
        raise typer.Exit(code=1)

    if dry_run:
        files = sorted(raw_dir.glob("*.parquet"))
        console.print(f"Preprocess dry-run for [bold]{as_of_date}[/bold]")
        if files:
            table = Table("raw file")
            for file_path in files:
                table.add_row(file_path.name)
            console.print(table)
        else:
            console.print("[yellow]No parquet files located in raw directory.[/yellow]")
        return

    if curated_dir.exists() and not force and any(curated_dir.iterdir()):
        console.print(
            f"[red]Curated directory already populated:[/] {curated_dir}. Use --force to overwrite."
        )
        raise typer.Exit(code=1)

    preprocessor = Preprocessor(config)
    result = preprocessor.run(as_of_date)
    _print_preprocess_summary(result)


def _print_preprocess_summary(result: PreprocessResult) -> None:
    console.print(
        f"[green]Preprocessed symbols for[/green] {result.as_of}: {', '.join(result.symbols) or 'none'}"
    )
    if not result.artifacts:
        console.print("[yellow]No curated artifacts were produced.[/yellow]")
        return
    table = Table("symbol", "artifact")
    for symbol in result.symbols:
        path = result.artifacts.get(symbol)
        table.add_row(symbol, str(path) if path else "—")
    console.print(table)


@signals_app.command("build")
def signals_build(
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
        ..., help="As-of date for signals (YYYY-MM-DD)."
    ),
    window: int = typer.Option(252, help="Lookback window in rows for evaluation."),
    dry_run: bool = typer.Option(
        False, help="Evaluate without writing parquet output."
    ),
) -> None:
    """Generate strategy signals for the configured universe."""

    config = load_config(config_path)
    as_of_date = _parse_as_of(as_of)
    engine = StrategyEngine(config)

    try:
        result = engine.build(as_of_date, window=window, dry_run=dry_run)
    except Exception as exc:  # pragma: no cover - defensive
        console.print(f"[red]Signal build failed:[/] {exc}")
        raise typer.Exit(code=1) from exc

    frame = result.frame
    console.print(
        f"[green]Evaluated strategy for[/green] {as_of_date} — symbols processed: {len(frame)}"
    )
    console.print(
        f"Entry passes: {result.entry_count} | Exit passes: {result.exit_count}"
    )

    if frame.empty:
        console.print("[yellow]No signals were produced.[/yellow]")
    else:
        feature_columns = [
            column
            for column in frame.columns
            if column not in {"date", "symbol", "signal", "rank_score"}
        ]
        table = Table("symbol", "signal", "rank_score", *feature_columns)
        for row in frame.itertuples(index=False):
            feature_values = [
                _format_number(getattr(row, column)) for column in feature_columns
            ]
            table.add_row(
                str(row.symbol),
                str(row.signal),
                _format_number(row.rank_score),
                *feature_values,
            )
        console.print(table)

    if dry_run:
        console.print("[yellow]Dry run requested; no files written.[/yellow]")
    elif result.output_path:
        console.print(f"Signals written to: {result.output_path}")


@signals_app.command("explain")
def signals_explain(
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
    symbol: str = typer.Option(..., help="Ticker symbol to inspect."),
    as_of: str = typer.Option(..., help="As-of date for evaluation (YYYY-MM-DD)."),
    window: int = typer.Option(252, help="Lookback window in rows for evaluation."),
) -> None:
    """Explain strategy rule evaluations for a symbol."""

    config = load_config(config_path)
    as_of_date = _parse_as_of(as_of)
    engine = StrategyEngine(config)

    try:
        evaluation = engine.explain(symbol, as_of_date, window=window)
    except KeyError as exc:
        console.print(f"[red]Symbol not evaluated:[/] {symbol.upper()}")
        raise typer.Exit(code=1) from exc
    except Exception as exc:  # pragma: no cover - defensive
        console.print(f"[red]Unable to evaluate symbol:[/] {exc}")
        raise typer.Exit(code=1) from exc

    console.print(
        f"[bold]{evaluation.symbol}[/bold] on {as_of_date}: signal={evaluation.signal}"
    )
    console.print(
        f"Entry rule: {evaluation.entry_rule} | Exit rule: {evaluation.exit_rule} | Rank score: {_format_number(evaluation.rank_score)}"
    )

    if evaluation.features:
        feature_table = Table("feature", "value")
        for name, value in sorted(evaluation.features.items()):
            feature_table.add_row(name, _format_number(value))
        console.print(feature_table)

    if evaluation.indicators:
        indicator_table = Table("indicator", "value")
        for name, value in sorted(evaluation.indicators.items()):
            indicator_table.add_row(name, _format_number(value))
        console.print(indicator_table)


@rebalance_app.command("propose")
def rebalance_propose(
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
    holdings_path: Path = typer.Option(  # noqa: B008 - CLI option definition
        ...,
        "--holdings",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="Holdings snapshot JSON file.",
    ),
    as_of: str = typer.Option(..., help="As-of date for rebalance (YYYY-MM-DD)."),
    signals_path: Path | None = typer.Option(  # noqa: B008 - CLI option definition
        None,
        "--signals",
        help="Optional path to signals parquet (defaults to reports/<as_of>/signals.parquet).",
    ),
    force: bool = typer.Option(
        False, help="Ignore cadence checks and overwrite existing proposal."
    ),
) -> None:
    """Build a rebalance proposal and persist the resulting JSON artifact."""

    config = load_config(config_path)
    as_of_date = _parse_as_of(as_of)
    holdings = load_holdings(holdings_path)
    signals_frame = _load_signals_for_cli(config, signals_path, as_of_date)
    if signals_frame is None:  # pragma: no cover - defensive
        raise AssertionError("Signals frame must be available for rebalance proposal")
    engine = RebalanceEngine(config)

    try:
        result = engine.build(
            as_of_date,
            holdings=holdings,
            signals=signals_frame,
            dry_run=False,
            force=force,
        )
    except Exception as exc:  # pragma: no cover - defensive
        console.print(f"[red]Rebalance proposal failed:[/] {exc}")
        raise typer.Exit(code=1) from exc

    _print_rebalance_summary(result)
    if result.output_path:
        console.print(f"Proposal written to: {result.output_path}")


@rebalance_app.command("dry-run")
def rebalance_dry_run(
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
    holdings_path: Path = typer.Option(  # noqa: B008 - CLI option definition
        ...,
        "--holdings",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="Holdings snapshot JSON file.",
    ),
    as_of: str = typer.Option(..., help="As-of date for rebalance (YYYY-MM-DD)."),
    signals_path: Path | None = typer.Option(  # noqa: B008 - CLI option definition
        None,
        "--signals",
        help="Optional path to signals parquet (defaults to reports/<as_of>/signals.parquet).",
    ),
    max_candidates: int | None = typer.Option(
        10,
        help="Maximum targets to display in summary (set to 0 to hide targets).",
    ),
    force: bool = typer.Option(
        False, help="Ignore cadence checks for dry-run evaluation."
    ),
) -> None:
    """Evaluate rebalance logic without writing artifacts."""

    config = load_config(config_path)
    as_of_date = _parse_as_of(as_of)
    holdings = load_holdings(holdings_path)
    signals_frame = _load_signals_for_cli(config, signals_path, as_of_date)
    if signals_frame is None:  # pragma: no cover - defensive
        raise AssertionError("Signals frame must be available for rebalance dry run")
    engine = RebalanceEngine(config)

    try:
        result = engine.build(
            as_of_date,
            holdings=holdings,
            signals=signals_frame,
            dry_run=True,
            force=force,
        )
    except Exception as exc:  # pragma: no cover - defensive
        console.print(f"[red]Rebalance dry run failed:[/] {exc}")
        raise typer.Exit(code=1) from exc

    limit = None if max_candidates is None or max_candidates < 0 else max_candidates
    _print_rebalance_summary(result, max_targets=limit)


@report_app.command("build")
def report_build(
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
    holdings_path: Path = typer.Option(  # noqa: B008 - CLI option definition
        ...,
        "--holdings",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="Holdings snapshot JSON file.",
    ),
    as_of: str = typer.Option(..., help="As-of date for report (YYYY-MM-DD)."),
    risk_path: Path | None = typer.Option(  # noqa: B008 - CLI option definition
        None,
        "--risk",
        help="Optional path to risk_alerts.json (defaults to reports/<as_of>/risk_alerts.json).",
    ),
    proposal_path: Path | None = typer.Option(  # noqa: B008 - CLI option definition
        None,
        "--proposal",
        help="Optional path to rebalance_proposal.json (defaults to reports/<as_of>/rebalance_proposal.json).",
    ),
    signals_path: Path | None = typer.Option(  # noqa: B008 - CLI option definition
        None,
        "--signals",
        help="Optional path to signals parquet (defaults to reports/<as_of>/signals.parquet).",
    ),
    include_pdf: bool = typer.Option(  # noqa: B008 - CLI option definition
        False,
        "--include-pdf",
        help="Render a PDF copy when a renderer is available.",
    ),
) -> None:
    """Render the daily report and persist HTML/JSON artifacts."""

    config = load_config(config_path)
    as_of_date = _parse_as_of(as_of)

    try:
        holdings = load_holdings(holdings_path)
    except Exception as exc:  # pragma: no cover - defensive
        console.print(f"[red]Failed to load holdings:[/] {exc}")
        raise typer.Exit(code=1) from exc

    try:
        result = _generate_report_result(
            config,
            as_of_date,
            holdings,
            holdings_path=holdings_path,
            risk_path_option=risk_path,
            proposal_path_option=proposal_path,
            signals_path_option=signals_path,
            include_pdf=include_pdf,
        )
    except Exception as exc:  # pragma: no cover - defensive
        console.print(f"[red]Report generation failed:[/] {exc}")
        raise typer.Exit(code=1) from exc

    _print_report_summary(result)

    if result.html_path:
        console.print(f"HTML report: {result.html_path}")
    if result.json_path:
        console.print(f"JSON report: {result.json_path}")
    if include_pdf:
        if result.pdf_path:
            console.print(f"PDF report: {result.pdf_path}")
        else:
            console.print("[yellow]PDF not generated; see notes above.[/yellow]")


@report_app.command("preview")
def report_preview(
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
    holdings_path: Path = typer.Option(  # noqa: B008 - CLI option definition
        ...,
        "--holdings",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="Holdings snapshot JSON file.",
    ),
    as_of: str = typer.Option(..., help="As-of date for report (YYYY-MM-DD)."),
    risk_path: Path | None = typer.Option(  # noqa: B008 - CLI option definition
        None,
        "--risk",
        help="Optional path to risk_alerts.json (defaults to reports/<as_of>/risk_alerts.json).",
    ),
    proposal_path: Path | None = typer.Option(  # noqa: B008 - CLI option definition
        None,
        "--proposal",
        help="Optional path to rebalance_proposal.json (defaults to reports/<as_of>/rebalance_proposal.json).",
    ),
    signals_path: Path | None = typer.Option(  # noqa: B008 - CLI option definition
        None,
        "--signals",
        help="Optional path to signals parquet (defaults to reports/<as_of>/signals.parquet).",
    ),
    open_browser: bool = typer.Option(  # noqa: B008 - CLI option definition
        False,
        "--open/--no-open",
        help="Open the rendered HTML in the default browser.",
    ),
) -> None:
    """Build the report artifacts and optionally open the HTML preview."""

    config = load_config(config_path)
    as_of_date = _parse_as_of(as_of)

    try:
        holdings = load_holdings(holdings_path)
    except Exception as exc:  # pragma: no cover - defensive
        console.print(f"[red]Failed to load holdings:[/] {exc}")
        raise typer.Exit(code=1) from exc

    try:
        result = _generate_report_result(
            config,
            as_of_date,
            holdings,
            holdings_path=holdings_path,
            risk_path_option=risk_path,
            proposal_path_option=proposal_path,
            signals_path_option=signals_path,
            include_pdf=False,
        )
    except Exception as exc:  # pragma: no cover - defensive
        console.print(f"[red]Report preview failed:[/] {exc}")
        raise typer.Exit(code=1) from exc

    _print_report_summary(result)

    if result.html_path:
        console.print(f"HTML report: {result.html_path}")
        if open_browser:
            try:
                webbrowser.open(result.html_path.as_uri())
            except Exception as exc:  # pragma: no cover - best effort
                console.print(f"[yellow]Unable to open browser:[/] {exc}")
    elif open_browser:
        console.print("[yellow]No HTML artifact available to open.[/yellow]")

    if result.json_path:
        console.print(f"JSON report: {result.json_path}")


@risk_app.command("evaluate")
def risk_evaluate(
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
    holdings_path: Path = typer.Option(  # noqa: B008 - CLI option definition
        ...,
        "--holdings",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="Holdings snapshot JSON file.",
    ),
    as_of: str = typer.Option(  # noqa: B008 - CLI option definition
        ..., help="As-of date for risk evaluation (YYYY-MM-DD)."
    ),
    dry_run: bool = typer.Option(
        False, help="Evaluate without writing risk_alerts.json."
    ),
) -> None:
    """Run crash/drawdown checks and market filter evaluation."""

    config = load_config(config_path)
    as_of_date = _parse_as_of(as_of)

    try:
        holdings = load_holdings(holdings_path)
    except Exception as exc:  # pragma: no cover - defensive
        console.print(f"[red]Failed to load holdings:[/] {exc}")
        raise typer.Exit(code=1) from exc

    engine = RiskEngine(config)

    try:
        result = engine.build(as_of_date, holdings, dry_run=dry_run)
    except Exception as exc:  # pragma: no cover - defensive
        console.print(f"[red]Risk evaluation failed:[/] {exc}")
        raise typer.Exit(code=1) from exc

    console.print(
        f"[green]Evaluated risk for[/green] {as_of_date} — alerts: {len(result.alerts)}"
    )
    console.print(f"Market state: {result.market_state}")

    if result.benchmark:
        if result.market_filter_pass is None:
            status = "UNKNOWN"
        else:
            status = "PASS" if result.market_filter_pass else "FAIL"
        console.print(f"Market filter [{result.benchmark}] status: {status}")

    if result.alerts:
        table = Table("symbol", "type", "value", "threshold")
        for alert in result.alerts:
            table.add_row(
                alert.symbol,
                alert.alert_type,
                _format_number(alert.value),
                _format_number(alert.threshold),
            )
        console.print(table)
    else:
        console.print("[yellow]No risk alerts triggered.[/yellow]")

    if dry_run:
        console.print("[yellow]Dry run requested; no files written.[/yellow]")
    elif result.output_path:
        console.print(f"Risk alerts written to: {result.output_path}")


@risk_app.command("explain")
def risk_explain(
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
    holdings_path: Path = typer.Option(  # noqa: B008 - CLI option definition
        ...,
        "--holdings",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="Holdings snapshot JSON file.",
    ),
    symbol: str = typer.Option(..., help="Ticker symbol to inspect."),
    as_of: str = typer.Option(..., help="As-of date for evaluation (YYYY-MM-DD)."),
) -> None:
    """Explain crash/drawdown evaluation for a holding."""

    config = load_config(config_path)
    as_of_date = _parse_as_of(as_of)

    try:
        holdings = load_holdings(holdings_path)
    except Exception as exc:  # pragma: no cover - defensive
        console.print(f"[red]Failed to load holdings:[/] {exc}")
        raise typer.Exit(code=1) from exc

    engine = RiskEngine(config)

    try:
        evaluation = engine.explain(symbol, as_of_date, holdings)
    except KeyError as exc:
        console.print(f"[red]Symbol not evaluated:[/] {symbol.upper()}")
        raise typer.Exit(code=1) from exc
    except Exception as exc:  # pragma: no cover - defensive
        console.print(f"[red]Unable to explain risk evaluation:[/] {exc}")
        raise typer.Exit(code=1) from exc

    console.print(
        f"[bold]{evaluation.symbol}[/bold] on {as_of_date}: "
        f"crash_triggered={evaluation.crash_triggered} | "
        f"drawdown_triggered={evaluation.drawdown_triggered}"
    )

    table = Table("metric", "value")
    table.add_row("daily_return", _format_number(evaluation.daily_return))
    table.add_row("crash_threshold", _format_number(evaluation.crash_threshold))
    table.add_row("drawdown", _format_number(evaluation.drawdown))
    table.add_row("drawdown_threshold", _format_number(evaluation.drawdown_threshold))
    table.add_row("close", _format_number(evaluation.close))
    table.add_row("rolling_peak", _format_number(evaluation.rolling_peak))
    console.print(table)


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
