"""Pipeline orchestration utilities for the trading-system CLI."""

from __future__ import annotations

import contextlib
import logging
import sys
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from trading_system.config import Config
from trading_system.data import DataProvider, DataRunMeta, run_data_pull
from trading_system.notify import (
    NotificationService,
    NotificationStatus,
    load_report_summary,
)
from trading_system.observability.logging import (
    StructuredJsonFormatter,
    StructuredLoggerAdapter,
)
from trading_system.observability.manifest import (
    ArtifactSpec,
    ManifestBuilder,
    ManifestWriteResult,
)
from trading_system.preprocess import Preprocessor, PreprocessResult
from trading_system.rebalance import RebalanceEngine, RebalanceResult
from trading_system.report import ReportBuilder, ReportResult
from trading_system.risk import HoldingsSnapshot, RiskEngine, RiskResult
from trading_system.signals import StrategyEngine, StrategyResult

__all__ = [
    "PipelineExecutionError",
    "PipelineSummary",
    "PipelineStep",
    "StepOutcome",
    "pipeline_logging",
    "run_daily_pipeline",
    "run_rebalance_pipeline",
]


@dataclass(slots=True)
class StepOutcome:
    """Normalized return value from an orchestration step."""

    status: str = "completed"
    details: str | None = None
    artifacts: dict[str, str] = field(default_factory=dict)
    manifest_entries: tuple[ArtifactSpec, ...] = ()


@dataclass(slots=True)
class PipelineStep:
    """Record of a pipeline step execution."""

    name: str
    status: str
    duration: float
    details: str | None = None
    artifacts: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class PipelineSummary:
    """Aggregated summary for an orchestrated pipeline run."""

    as_of: date
    success: bool
    duration: float
    steps: list[PipelineStep]
    manifest: dict[str, str]


class PipelineExecutionError(RuntimeError):
    """Raised when a pipeline step fails in the orchestrator."""

    def __init__(
        self,
        step: str,
        message: str,
        *,
        summary: PipelineSummary | None = None,
        original: Exception | None = None,
    ) -> None:
        super().__init__(message)
        self.step = step
        self.summary = summary
        self.original = original


class _PipelineStepError(RuntimeError):
    """Internal signal used to annotate a failing pipeline step."""

    def __init__(self, step: str, error: Exception) -> None:
        super().__init__(str(error))
        self.step = step
        self.error = error


@contextlib.contextmanager
def pipeline_logging(
    log_path: Path | None, *, context: dict[str, Any] | None = None
) -> Iterator[logging.LoggerAdapter[logging.Logger]]:
    """Configure structured logging for pipeline execution."""

    root_logger = logging.getLogger()
    previous_level = root_logger.level
    previous_handlers = root_logger.handlers[:]

    for handler in previous_handlers:
        root_logger.removeHandler(handler)

    formatter = StructuredJsonFormatter()

    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)

    file_handler: logging.Handler | None = None
    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    root_logger.setLevel(logging.INFO)

    adapter = StructuredLoggerAdapter(
        logging.getLogger("trading_system.pipeline"), context or {}
    )

    try:
        yield adapter
    finally:
        root_logger.removeHandler(stream_handler)
        stream_handler.close()

        if file_handler is not None:
            root_logger.removeHandler(file_handler)
            file_handler.close()

        for handler in previous_handlers:
            root_logger.addHandler(handler)
        root_logger.setLevel(previous_level)


def run_daily_pipeline(
    *,
    config: Config,
    provider: DataProvider,
    as_of: date,
    holdings: HoldingsSnapshot,
    holdings_path: Path,
    config_path: Path | None,
    dry_run: bool,
    force: bool,
    channels: Iterable[str],
    log_path: Path | None,
) -> PipelineSummary:
    """Execute the daily pipeline (pull → preprocess → risk → report → notify)."""

    runner = _PipelineRunner(
        config=config,
        provider=provider,
        as_of=as_of,
        holdings=holdings,
        holdings_path=holdings_path,
        config_path=config_path,
        dry_run=dry_run,
        force=force,
        channels=tuple(channels),
        include_signals=False,
        include_rebalance=False,
        pipeline_name="daily",
        log_path=log_path,
    )
    return runner.run()


def run_rebalance_pipeline(
    *,
    config: Config,
    provider: DataProvider,
    as_of: date,
    holdings: HoldingsSnapshot,
    holdings_path: Path,
    config_path: Path | None,
    dry_run: bool,
    force: bool,
    channels: Iterable[str],
    log_path: Path | None,
) -> PipelineSummary:
    """Execute the rebalance pipeline including signals and proposal generation."""

    runner = _PipelineRunner(
        config=config,
        provider=provider,
        as_of=as_of,
        holdings=holdings,
        holdings_path=holdings_path,
        config_path=config_path,
        dry_run=dry_run,
        force=force,
        channels=tuple(channels),
        include_signals=True,
        include_rebalance=True,
        pipeline_name="rebalance",
        log_path=log_path,
    )
    return runner.run()


class _PipelineRunner:
    """Internal helper that orchestrates pipeline steps and aggregates results."""

    def __init__(
        self,
        *,
        config: Config,
        provider: DataProvider,
        as_of: date,
        holdings: HoldingsSnapshot,
        holdings_path: Path,
        config_path: Path | None,
        dry_run: bool,
        force: bool,
        channels: tuple[str, ...],
        include_signals: bool,
        include_rebalance: bool,
        pipeline_name: str,
        log_path: Path | None,
    ) -> None:
        self._config = config
        self._provider = provider
        self._as_of = as_of
        self._as_of_str = as_of.strftime("%Y-%m-%d")
        self._holdings = holdings
        self._holdings_path = holdings_path
        self._config_path = config_path
        self._dry_run = dry_run
        self._force = force
        self._channels = channels if channels else ("all",)
        self._include_signals = include_signals
        self._include_rebalance = include_rebalance
        self._logger = logging.getLogger("trading_system.pipeline")
        self._pipeline_name = pipeline_name
        self._log_path = log_path

        self._data_meta: DataRunMeta | None = None
        self._preprocess_result: PreprocessResult | None = None
        self._strategy_result: StrategyResult | None = None
        self._risk_result: RiskResult | None = None
        self._rebalance_result: RebalanceResult | None = None
        self._report_result: ReportResult | None = None
        self._notification_statuses: tuple[NotificationStatus, ...] | None = None

        reports_dir = self._config.paths.reports / self._as_of_str
        self._manifest_builder = ManifestBuilder(
            pipeline=pipeline_name,
            as_of=self._as_of,
            reports_dir=reports_dir,
            config_path=config_path,
            holdings_path=holdings_path,
            log_path=log_path,
        )

    def run(self) -> PipelineSummary:
        steps: list[PipelineStep] = []
        start_perf = time.perf_counter()
        run_started_at = datetime.now(UTC)
        manifest_result: ManifestWriteResult | None = None

        self._logger.info(
            "pipeline_start",
            extra={
                "event": "pipeline_start",
                "pipeline": self._pipeline_name,
                "as_of": self._as_of_str,
            },
        )

        try:
            self._execute_step("data_pull", steps, self._step_data_pull)
            self._execute_step("data_preprocess", steps, self._step_preprocess)
            if self._include_signals:
                self._execute_step("signals_build", steps, self._step_signals)
            self._execute_step("risk_evaluate", steps, self._step_risk)
            if self._include_rebalance:
                self._execute_step("rebalance_propose", steps, self._step_rebalance)
            self._execute_step("report_build", steps, self._step_report)
            self._execute_step("notify_send", steps, self._step_notify)
        except _PipelineStepError as exc:
            duration = time.perf_counter() - start_perf
            completed_at = datetime.now(UTC)
            self._logger.error(
                "pipeline_failed",
                extra={
                    "event": "pipeline_end",
                    "pipeline": self._pipeline_name,
                    "as_of": self._as_of_str,
                    "status": "failed",
                    "duration": duration,
                },
            )
            manifest_result = self._manifest_builder.finalize(
                started_at=run_started_at,
                completed_at=completed_at,
                success=False,
            )
            summary = PipelineSummary(
                as_of=self._as_of,
                success=False,
                duration=duration,
                steps=steps,
                manifest=manifest_result.summary if manifest_result else {},
            )
            raise PipelineExecutionError(
                exc.step, str(exc), summary=summary, original=exc.error
            ) from exc.error

        duration = time.perf_counter() - start_perf
        completed_at = datetime.now(UTC)
        self._logger.info(
            "pipeline_completed",
            extra={
                "event": "pipeline_end",
                "pipeline": self._pipeline_name,
                "as_of": self._as_of_str,
                "status": "completed",
                "duration": duration,
            },
        )
        manifest_result = self._manifest_builder.finalize(
            started_at=run_started_at,
            completed_at=completed_at,
            success=True,
        )
        return PipelineSummary(
            as_of=self._as_of,
            success=True,
            duration=duration,
            steps=steps,
            manifest=manifest_result.summary,
        )

    def _execute_step(
        self,
        name: str,
        steps: list[PipelineStep],
        func: Callable[[], StepOutcome],
    ) -> StepOutcome:
        step_started_at = datetime.now(UTC)
        step_start = time.perf_counter()
        self._logger.info(
            "step_start",
            extra={
                "event": "step_start",
                "step": name,
                "as_of": self._as_of_str,
            },
        )
        try:
            outcome = func()
        except Exception as exc:
            duration = time.perf_counter() - step_start
            completed_at = datetime.now(UTC)
            steps.append(
                PipelineStep(
                    name=name,
                    status="failed",
                    duration=duration,
                    details=str(exc),
                )
            )
            self._manifest_builder.add_step(
                name=name,
                status="failed",
                started_at=step_started_at,
                completed_at=completed_at,
                duration_seconds=duration,
                details=str(exc),
                artifacts=(),
            )
            self._logger.error(
                "step_failed",
                extra={
                    "event": "step_end",
                    "step": name,
                    "status": "failed",
                    "duration": duration,
                    "error": str(exc),
                },
            )
            raise _PipelineStepError(name, exc) from exc

        duration = time.perf_counter() - step_start
        completed_at = datetime.now(UTC)
        status = outcome.status or "completed"
        steps.append(
            PipelineStep(
                name=name,
                status=status,
                duration=duration,
                details=outcome.details,
                artifacts=outcome.artifacts,
            )
        )
        self._manifest_builder.add_step(
            name=name,
            status=status,
            started_at=step_started_at,
            completed_at=completed_at,
            duration_seconds=duration,
            details=outcome.details,
            artifacts=outcome.manifest_entries,
        )
        self._logger.info(
            "step_completed",
            extra={
                "event": "step_end",
                "step": name,
                "status": status,
                "duration": duration,
            },
        )
        return outcome

    # ---- individual steps -------------------------------------------------

    def _step_data_pull(self) -> StepOutcome:
        self._logger.info("Pulling raw data for %s", self._as_of_str)
        meta = run_data_pull(
            self._config,
            self._provider,
            as_of=self._as_of,
            include_benchmark=True,
        )
        self._data_meta = meta
        details = (
            f"symbols={len(meta.symbols)} window={meta.start}→{meta.end}"
            if meta.start and meta.end
            else f"symbols={len(meta.symbols)}"
        )
        return StepOutcome(
            details=details,
            artifacts={"raw_directory": str(meta.directory)},
            manifest_entries=(
                ArtifactSpec(
                    key="data_raw",
                    path=meta.directory,
                    kind="directory",
                    description="Raw data pull",
                ),
            ),
        )

    def _step_preprocess(self) -> StepOutcome:
        curated_dir = self._config.paths.data_curated / self._as_of_str
        if curated_dir.exists() and not self._force and any(curated_dir.iterdir()):
            raise RuntimeError(
                f"Curated directory already populated: {curated_dir}. Use --force to overwrite."
            )

        self._logger.info("Preprocessing raw data into curated artifacts")
        preprocessor = Preprocessor(self._config)
        result = preprocessor.run(self._as_of)
        self._preprocess_result = result
        curated_path = self._config.paths.data_curated / self._as_of_str
        return StepOutcome(
            details=f"symbols={len(result.symbols)}",
            artifacts={"curated_directory": str(curated_path)},
            manifest_entries=(
                ArtifactSpec(
                    key="data_curated",
                    path=curated_path,
                    kind="directory",
                    description="Curated datasets",
                ),
            ),
        )

    def _step_signals(self) -> StepOutcome:
        self._logger.info("Generating strategy signals")
        engine = StrategyEngine(self._config)
        result = engine.build(self._as_of, window=252, dry_run=False)
        self._strategy_result = result
        manifest_entries: tuple[ArtifactSpec, ...] = ()
        artifacts: dict[str, str] = {}
        if result.output_path is not None:
            artifacts["signals_parquet"] = str(result.output_path)
            manifest_entries = (
                ArtifactSpec(
                    key="signals",
                    path=result.output_path,
                    kind="file",
                    row_count=len(result.frame),
                    description="Strategy signals",
                ),
            )
        details = f"records={len(result.frame)} entries={result.entry_count} exits={result.exit_count}"
        return StepOutcome(
            details=details,
            artifacts=artifacts,
            manifest_entries=manifest_entries,
        )

    def _step_risk(self) -> StepOutcome:
        self._logger.info("Evaluating risk rules")
        engine = RiskEngine(self._config)
        result = engine.build(self._as_of, self._holdings, dry_run=False)
        self._risk_result = result
        artifacts: dict[str, str] = {}
        manifest_entries: tuple[ArtifactSpec, ...] = ()
        if result.output_path is not None:
            artifacts["risk_alerts"] = str(result.output_path)
            manifest_entries = (
                ArtifactSpec(
                    key="risk_alerts",
                    path=result.output_path,
                    kind="file",
                    description="Risk alert payload",
                    row_count=len(result.alerts),
                ),
            )
        details = f"alerts={len(result.alerts)} market_state={result.market_state}"
        return StepOutcome(
            details=details,
            artifacts=artifacts,
            manifest_entries=manifest_entries,
        )

    def _step_rebalance(self) -> StepOutcome:
        if self._strategy_result is None:
            raise RuntimeError("Signals must be generated before rebalance step")

        self._logger.info("Constructing rebalance proposal")
        engine = RebalanceEngine(self._config)
        result = engine.build(
            self._as_of,
            holdings=self._holdings,
            signals=self._strategy_result.frame,
            dry_run=False,
            force=self._force,
        )
        self._rebalance_result = result

        artifacts: dict[str, str] = {}
        manifest_entries: tuple[ArtifactSpec, ...] = ()
        if result.output_path is not None:
            artifacts["proposal"] = str(result.output_path)
            manifest_entries = (
                ArtifactSpec(
                    key="rebalance_proposal",
                    path=result.output_path,
                    kind="file",
                    description="Rebalance proposal",
                    row_count=len(result.targets) + len(result.orders),
                ),
            )

        details = f"status={result.status} targets={len(result.targets)} orders={len(result.orders)}"
        return StepOutcome(
            details=details,
            artifacts=artifacts,
            manifest_entries=manifest_entries,
        )

    def _step_report(self) -> StepOutcome:
        self._logger.info("Rendering daily report")
        builder = ReportBuilder(self._config)

        risk_payload: dict[str, Any] | None = None
        risk_path: Path | None = None
        if self._risk_result is not None:
            risk_payload = _risk_payload(self._risk_result)
            risk_path = self._risk_result.output_path

        proposal_payload: dict[str, Any] | None = None
        proposal_path: Path | None = None
        if self._rebalance_result is not None:
            proposal_payload = _rebalance_payload(self._rebalance_result)
            proposal_path = self._rebalance_result.output_path

        signals_frame = self._strategy_result.frame if self._strategy_result else None
        signals_path = (
            self._strategy_result.output_path if self._strategy_result else None
        )

        result = builder.build(
            self._as_of,
            holdings=self._holdings,
            holdings_path=self._holdings_path,
            risk_payload=risk_payload,
            risk_path=risk_path,
            proposal_payload=proposal_payload,
            proposal_path=proposal_path,
            signals=signals_frame,
            signals_path=signals_path,
            include_pdf=False,
            dry_run=False,
        )
        self._report_result = result

        artifacts: dict[str, str] = {}
        manifest_entries: list[ArtifactSpec] = []
        if result.json_path is not None:
            artifacts["report_json"] = str(result.json_path)
            manifest_entries.append(
                ArtifactSpec(
                    key="report_json",
                    path=result.json_path,
                    kind="file",
                    description="Daily report payload",
                )
            )
        if result.html_path is not None:
            artifacts["report_html"] = str(result.html_path)

        details = f"orders={len(result.payload.get('actions', {}).get('orders', []))}"
        return StepOutcome(
            details=details,
            artifacts=artifacts,
            manifest_entries=tuple(manifest_entries),
        )

    def _step_notify(self) -> StepOutcome:
        if self._report_result is None:
            raise RuntimeError("Report must be generated before notifications")

        if self._dry_run:
            self._logger.info(
                "Dispatching notifications in dry-run mode for channels=%s",
                ",".join(self._channels),
            )
        else:
            self._logger.info(
                "Dispatching notifications for channels=%s",
                ",".join(self._channels),
            )

        summary = load_report_summary(self._config, self._as_of)
        service = NotificationService()
        statuses = service.dispatch(
            summary, self._config, list(self._channels), dry_run=self._dry_run
        )
        self._notification_statuses = tuple(statuses)

        delivered = sum(1 for status in statuses if status.delivered)
        details = f"channels={len(statuses)} delivered={delivered}" + (
            " (dry-run)" if self._dry_run else ""
        )
        return StepOutcome(details=details)


def _risk_payload(result: RiskResult) -> dict[str, Any]:
    alerts = [
        {
            "symbol": alert.symbol,
            "type": alert.alert_type,
            "value": alert.value,
            "threshold": alert.threshold,
            "reason": alert.reason,
        }
        for alert in result.alerts
    ]

    market_filter: dict[str, Any] | None = None
    if result.benchmark:
        market_filter = {
            "benchmark": result.benchmark,
            "passed": result.market_filter_pass,
            "rule": None,
        }

    payload: dict[str, Any] = {
        "date": result.as_of.isoformat(),
        "evaluated_at": result.evaluated_at.astimezone(UTC).isoformat(),
        "market_state": result.market_state,
        "alerts": alerts,
    }

    if market_filter is not None:
        payload["market_filter"] = market_filter

    return payload


def _rebalance_payload(result: RebalanceResult) -> dict[str, Any]:
    targets = [
        {
            "symbol": target.symbol,
            "target_weight": target.target_weight,
            "rationale": target.rationale,
        }
        for target in result.targets
    ]
    orders = [
        {
            "symbol": order.symbol,
            "side": order.side,
            "quantity": order.quantity,
            "notional": order.notional,
        }
        for order in result.orders
    ]

    return {
        "status": result.status,
        "cash_buffer": result.cash_buffer,
        "turnover": result.turnover,
        "targets": targets,
        "orders": orders,
        "notes": list(result.notes),
    }
