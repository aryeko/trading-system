"""Tests covering the pipeline orchestrator utilities."""

# mypy: disable-error-code=no-untyped-def
# mypy: disable-error-code=var-annotated

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from trading_system.config import load_config
from trading_system.data import DataRunMeta
from trading_system.data.provider import DataProvider
from trading_system.notify import NotificationStatus
from trading_system.orchestrator import (
    PipelineExecutionError,
    PipelineSummary,
    run_daily_pipeline,
    run_rebalance_pipeline,
)
from trading_system.preprocess import PreprocessResult
from trading_system.rebalance import RebalanceResult
from trading_system.report import ManifestEntry, ReportResult
from trading_system.risk import RiskResult, load_holdings
from trading_system.signals import StrategyResult


class DummyProvider(DataProvider):
    def get_bars(
        self, universe: Sequence[str], start: date | datetime, end: date | datetime
    ) -> pd.DataFrame:
        return pd.DataFrame()

    def get_benchmark(
        self, symbol: str, start: date | datetime, end: date | datetime
    ) -> pd.DataFrame:
        return pd.DataFrame()


DUMMY_PROVIDER = DummyProvider()


def _write_config(tmp_path: Path) -> Path:
    config_text = f"""
base_ccy: USD
calendar: NYSE
data:
  provider: yahoo
  lookback_days: 30
universe:
  tickers: [AAPL]
strategy:
  type: trend_follow
  entry: "close > sma_100"
  exit: "close < sma_100"
  rank: momentum_63d
risk:
  crash_threshold_pct: -0.08
  drawdown_threshold_pct: -0.20
  market_filter:
    benchmark: SPY
    rule: "close > sma_200"
rebalance:
  cadence: monthly
  max_positions: 5
  equal_weight: true
  min_weight: 0.05
  cash_buffer: 0.05
notify:
  email: ops@example.com
  slack_webhook: https://hooks.slack.test/ABC
paths:
  data_raw: {tmp_path}/data/raw
  data_curated: {tmp_path}/data/curated
  reports: {tmp_path}/reports
"""
    config_path = tmp_path / "config.yml"
    config_path.write_text(config_text, encoding="utf-8")
    return config_path


def _write_holdings(tmp_path: Path) -> Path:
    holdings_payload = {
        "as_of_date": "2024-05-01",
        "base_ccy": "USD",
        "cash": 1000.0,
        "positions": [
            {"symbol": "AAPL", "qty": 10, "cost_basis": 100.0},
        ],
    }
    holdings_path = tmp_path / "holdings.json"
    holdings_path.write_text(json.dumps(holdings_payload), encoding="utf-8")
    return holdings_path


def _stub_bindings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run_data_pull(
        config, provider, *, as_of, run_at=None, include_benchmark=True
    ):
        directory = config.paths.data_raw / as_of.strftime("%Y-%m-%d")
        directory.mkdir(parents=True, exist_ok=True)
        return DataRunMeta(
            directory=directory,
            timestamp=datetime.now(UTC),
            symbols=("AAPL",),
            last_bar_date=as_of,
            start=as_of,
            end=as_of,
            benchmark=None,
        )

    class FakePreprocessor:
        def __init__(self, config) -> None:
            self._config = config

        def run(self, as_of):
            as_of_date = pd.Timestamp(as_of).date()
            curated_dir = self._config.paths.data_curated / as_of_date.strftime(
                "%Y-%m-%d"
            )
            curated_dir.mkdir(parents=True, exist_ok=True)
            output_path = curated_dir / "AAPL.parquet"
            output_path.touch(exist_ok=True)
            return PreprocessResult(
                as_of=as_of_date,
                symbols=("AAPL",),
                artifacts={"AAPL": output_path},
            )

    class FakeStrategyEngine:
        def __init__(self, config) -> None:
            self._config = config

        def build(self, as_of, window=None, dry_run=False):
            as_of_date = pd.Timestamp(as_of).date()
            frame = pd.DataFrame(
                {
                    "date": [pd.Timestamp(as_of_date)],
                    "symbol": ["AAPL"],
                    "signal": ["BUY"],
                    "rank_score": [1.0],
                }
            )
            output_dir = self._config.paths.reports / as_of_date.strftime("%Y-%m-%d")
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / "signals.parquet"
            output_path.touch(exist_ok=True)
            return StrategyResult(
                as_of=as_of_date,
                frame=frame,
                evaluations={},
                entry_count=1,
                exit_count=0,
                output_path=output_path,
            )

    class FakeRiskEngine:
        def __init__(self, config) -> None:
            self._config = config

        def build(self, as_of, holdings, dry_run=False):
            as_of_date = pd.Timestamp(as_of).date()
            reports_dir = self._config.paths.reports / as_of_date.strftime("%Y-%m-%d")
            reports_dir.mkdir(parents=True, exist_ok=True)
            output_path = reports_dir / "risk_alerts.json"
            payload: dict[str, Any] = {
                "date": as_of_date.isoformat(),
                "evaluated_at": datetime.now(UTC).isoformat(),
                "market_state": "RISK_ON",
                "alerts": [],
            }
            output_path.write_text(json.dumps(payload), encoding="utf-8")
            return RiskResult(
                as_of=as_of_date,
                evaluated_at=datetime.now(UTC),
                market_state="RISK_ON",
                alerts=tuple(),
                evaluations={},
                market_filter_pass=True,
                benchmark="SPY",
                output_path=output_path,
            )

    class FakeRebalanceEngine:
        def __init__(self, config) -> None:
            self._config = config

        def build(
            self,
            as_of,
            *,
            holdings,
            signals,
            dry_run=False,
            force=False,
        ):
            as_of_date = pd.Timestamp(as_of).date()
            reports_dir = self._config.paths.reports / as_of_date.strftime("%Y-%m-%d")
            reports_dir.mkdir(parents=True, exist_ok=True)
            output_path = reports_dir / "rebalance_proposal.json"
            payload: dict[str, Any] = {
                "status": "READY",
                "targets": [],
                "orders": [],
                "notes": [],
            }
            output_path.write_text(json.dumps(payload), encoding="utf-8")
            return RebalanceResult(
                as_of=as_of_date,
                status="READY",
                cash_buffer=0.05,
                turnover=0.1,
                targets=tuple(),
                orders=tuple(),
                notes=tuple(),
                output_path=output_path,
            )

    class FakeReportBuilder:
        def __init__(self, config) -> None:
            self._config = config

        def build(
            self,
            as_of,
            *,
            holdings,
            holdings_path,
            risk_payload=None,
            risk_path=None,
            proposal_payload=None,
            proposal_path=None,
            signals=None,
            signals_path=None,
            include_pdf=False,
            dry_run=False,
        ):
            as_of_date = pd.Timestamp(as_of).date()
            reports_dir = self._config.paths.reports / as_of_date.strftime("%Y-%m-%d")
            reports_dir.mkdir(parents=True, exist_ok=True)
            html_path = reports_dir / "daily_report.html"
            html_path.write_text("<html></html>", encoding="utf-8")
            json_path = reports_dir / "daily_report.json"
            payload: dict[str, Any] = {
                "as_of": as_of_date.isoformat(),
                "generated_at": datetime.now(UTC).isoformat(),
                "base_currency": "USD",
                "risk": risk_payload or {"market_state": "RISK_ON", "alerts": []},
                "actions": proposal_payload
                or {"orders": [], "exits": [], "status": "READY", "turnover": 0.0},
                "notes": [],
            }
            json_path.write_text(json.dumps(payload), encoding="utf-8")
            manifest = {"report_json": ManifestEntry(path=str(json_path), sha256=None)}
            return ReportResult(
                as_of=as_of_date,
                generated_at=datetime.now(UTC),
                html_path=html_path,
                json_path=json_path,
                pdf_path=None,
                payload=payload,
                manifest=manifest,
                notes=tuple(),
            )

    class FakeNotificationService:
        def dispatch(self, summary, config, channels, dry_run=False):
            return tuple(
                NotificationStatus(
                    channel=channel,
                    delivered=True,
                    details="dry-run" if dry_run else "sent",
                )
                for channel in channels
            )

    monkeypatch.setattr("trading_system.orchestrator.run_data_pull", fake_run_data_pull)
    monkeypatch.setattr("trading_system.orchestrator.Preprocessor", FakePreprocessor)
    monkeypatch.setattr(
        "trading_system.orchestrator.StrategyEngine", FakeStrategyEngine
    )
    monkeypatch.setattr("trading_system.orchestrator.RiskEngine", FakeRiskEngine)
    monkeypatch.setattr(
        "trading_system.orchestrator.RebalanceEngine", FakeRebalanceEngine
    )
    monkeypatch.setattr("trading_system.orchestrator.ReportBuilder", FakeReportBuilder)
    monkeypatch.setattr(
        "trading_system.orchestrator.NotificationService", FakeNotificationService
    )


def test_run_daily_pipeline_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = _write_config(tmp_path)
    holdings_path = _write_holdings(tmp_path)
    config = load_config(config_path)
    holdings = load_holdings(holdings_path)

    _stub_bindings(tmp_path, monkeypatch)

    summary = run_daily_pipeline(
        config=config,
        provider=DUMMY_PROVIDER,
        as_of=date(2024, 5, 2),
        holdings=holdings,
        holdings_path=holdings_path,
        dry_run=True,
        force=False,
        channels=["email"],
    )

    assert isinstance(summary, PipelineSummary)
    assert summary.success is True
    assert "report_json" in summary.manifest
    assert any(step.name == "notify_send" for step in summary.steps)


def test_run_rebalance_pipeline_generates_proposal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = _write_config(tmp_path)
    holdings_path = _write_holdings(tmp_path)
    config = load_config(config_path)
    holdings = load_holdings(holdings_path)

    _stub_bindings(tmp_path, monkeypatch)

    summary = run_rebalance_pipeline(
        config=config,
        provider=DUMMY_PROVIDER,
        as_of=date(2024, 5, 3),
        holdings=holdings,
        holdings_path=holdings_path,
        dry_run=False,
        force=True,
        channels=["all"],
    )

    assert summary.success is True
    assert "rebalance_proposal" in summary.manifest
    proposal_path = Path(summary.manifest["rebalance_proposal"])
    assert proposal_path.name == "rebalance_proposal.json"


def test_pipeline_failure_propagates_step(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = _write_config(tmp_path)
    holdings_path = _write_holdings(tmp_path)
    config = load_config(config_path)
    holdings = load_holdings(holdings_path)

    _stub_bindings(tmp_path, monkeypatch)

    class FailingPreprocessor:
        def __init__(self, config) -> None:
            self._config = config

        def run(self, as_of):  # noqa: ANN001 - test stub
            raise RuntimeError("boom")

    monkeypatch.setattr("trading_system.orchestrator.Preprocessor", FailingPreprocessor)

    with pytest.raises(PipelineExecutionError) as excinfo:
        run_daily_pipeline(
            config=config,
            provider=DUMMY_PROVIDER,
            as_of=date(2024, 5, 4),
            holdings=holdings,
            holdings_path=holdings_path,
            dry_run=False,
            force=False,
            channels=["email"],
        )

    error = excinfo.value
    assert error.step == "data_preprocess"
    assert error.summary is not None
    assert error.summary.success is False
    assert any(step.status == "failed" for step in error.summary.steps)
