# Copyright (c) 2025 Arye Kogan
# SPDX-License-Identifier: MIT

"""Risk engine for crash/drawdown alerts and market filter evaluation."""

from __future__ import annotations

import json
import logging
import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_system.config import Config
from trading_system.rules import RuleEvaluator

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class Position:
    """Position held in the portfolio."""

    symbol: str
    qty: float
    cost_basis: float | None = None


@dataclass(slots=True)
class HoldingsSnapshot:
    """Current portfolio snapshot used for risk evaluation."""

    as_of_date: date | None
    positions: tuple[Position, ...]
    cash: float | None = None
    base_ccy: str | None = None

    @property
    def symbols(self) -> tuple[str, ...]:
        return tuple(position.symbol for position in self.positions)


@dataclass(slots=True)
class RiskAlert:
    """Alert generated for a holding when a threshold is breached."""

    symbol: str
    alert_type: str
    value: float
    threshold: float
    reason: str


@dataclass(slots=True)
class SymbolRiskEvaluation:
    """Detailed metrics for a holding used in risk decisions."""

    symbol: str
    daily_return: float
    drawdown: float
    crash_threshold: float
    drawdown_threshold: float
    crash_triggered: bool
    drawdown_triggered: bool
    close: float | None
    rolling_peak: float | None


@dataclass(slots=True)
class RiskResult:
    """Aggregated output from a risk evaluation run."""

    as_of: date
    evaluated_at: datetime
    market_state: str
    alerts: tuple[RiskAlert, ...]
    evaluations: dict[str, SymbolRiskEvaluation]
    market_filter_pass: bool | None
    benchmark: str | None
    output_path: Path | None = None


class RiskEngine:
    """Compute crash/drawdown alerts and evaluate the market filter."""

    def __init__(
        self, config: Config, *, clock: Callable[[], datetime] | None = None
    ) -> None:
        self._config = config
        self._curated_base = config.paths.data_curated
        self._reports_base = config.paths.reports
        self._crash_threshold = config.risk.crash_threshold_pct
        self._drawdown_threshold = config.risk.drawdown_threshold_pct
        self._market_filter = config.risk.market_filter
        self._clock = clock or (lambda: datetime.now(UTC))

    def evaluate(
        self, as_of: date | str | pd.Timestamp, holdings: HoldingsSnapshot
    ) -> RiskResult:
        """Evaluate risk rules for the provided holdings."""

        as_of_ts = _normalize_timestamp(as_of)
        as_of_date = as_of_ts.date()
        curated_dir = self._curated_base / as_of_ts.strftime("%Y-%m-%d")
        if not curated_dir.is_dir():
            raise FileNotFoundError(f"Curated data directory not found: {curated_dir}")

        alerts: list[RiskAlert] = []
        evaluations: dict[str, SymbolRiskEvaluation] = {}

        for position in sorted(holdings.positions, key=lambda item: item.symbol):
            symbol = position.symbol
            frame = self._load_symbol_frame(curated_dir, symbol, as_of_ts)
            if frame is None:
                logger.warning(
                    "Curated dataset missing for %s in %s", symbol, curated_dir
                )
                continue

            latest = frame.iloc[-1]
            daily_return = _safe_float(latest.get("ret_1d"))
            crash_triggered = _is_triggered(daily_return, self._crash_threshold)

            close = _safe_float(latest.get("close"))
            rolling_peak = _safe_float(latest.get("rolling_peak"))
            drawdown = _compute_drawdown(close, rolling_peak)
            drawdown_triggered = _is_triggered(drawdown, self._drawdown_threshold)

            if crash_triggered:
                alerts.append(
                    RiskAlert(
                        symbol=symbol,
                        alert_type="CRASH",
                        value=daily_return,
                        threshold=self._crash_threshold,
                        reason=f"Daily return {daily_return:.4f} <= crash threshold {self._crash_threshold:.4f}",
                    )
                )
            if drawdown_triggered:
                alerts.append(
                    RiskAlert(
                        symbol=symbol,
                        alert_type="DRAWDOWN",
                        value=drawdown,
                        threshold=self._drawdown_threshold,
                        reason=f"Drawdown {drawdown:.4f} <= threshold {self._drawdown_threshold:.4f}",
                    )
                )

            evaluations[symbol] = SymbolRiskEvaluation(
                symbol=symbol,
                daily_return=daily_return,
                drawdown=drawdown,
                crash_threshold=self._crash_threshold,
                drawdown_threshold=self._drawdown_threshold,
                crash_triggered=crash_triggered,
                drawdown_triggered=drawdown_triggered,
                close=None if math.isnan(close) else close,
                rolling_peak=None if math.isnan(rolling_peak) else rolling_peak,
            )

        alerts.sort(key=lambda alert: (alert.symbol, alert.alert_type))

        market_state, market_pass = self._evaluate_market_filter(curated_dir, as_of_ts)
        evaluated_at = self._clock()

        logger.info(
            "Risk evaluation for %s generated %d alerts (market_state=%s)",
            as_of_date,
            len(alerts),
            market_state,
        )

        return RiskResult(
            as_of=as_of_date,
            evaluated_at=evaluated_at,
            market_state=market_state,
            alerts=tuple(alerts),
            evaluations=evaluations,
            market_filter_pass=market_pass,
            benchmark=(
                self._market_filter.benchmark.upper() if self._market_filter else None
            ),
        )

    def build(
        self,
        as_of: date | str | pd.Timestamp,
        holdings: HoldingsSnapshot,
        *,
        dry_run: bool = False,
    ) -> RiskResult:
        """Evaluate risk rules and persist alerts to JSON unless ``dry_run``."""

        result = self.evaluate(as_of, holdings)
        if dry_run:
            return result

        as_of_str = result.as_of.strftime("%Y-%m-%d")
        output_dir = self._reports_base / as_of_str
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "risk_alerts.json"

        payload = self._serialize_result(result)
        output_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
        )
        result.output_path = output_path
        logger.info("Risk alerts written to %s", output_path)
        return result

    def explain(
        self,
        symbol: str,
        as_of: date | str | pd.Timestamp,
        holdings: HoldingsSnapshot,
    ) -> SymbolRiskEvaluation:
        """Return evaluation details for ``symbol`` on ``as_of``."""

        result = self.evaluate(as_of, holdings)
        symbol_upper = symbol.upper()
        evaluation = result.evaluations.get(symbol_upper)
        if evaluation is None:
            raise KeyError(f"Symbol {symbol_upper} not evaluated.")
        return evaluation

    def _load_symbol_frame(
        self, curated_dir: Path, symbol: str, as_of: pd.Timestamp
    ) -> pd.DataFrame | None:
        path = curated_dir / f"{symbol.upper()}.parquet"
        if not path.is_file():
            return None
        data = pd.read_parquet(path)
        if data.empty:
            return None
        data["date"] = pd.to_datetime(data["date"], utc=False)
        data = data.sort_values("date")
        data = data[data["date"] <= as_of]
        if data.empty:
            return None
        data = data.set_index("date")
        return data

    def _evaluate_market_filter(
        self, curated_dir: Path, as_of: pd.Timestamp
    ) -> tuple[str, bool | None]:
        if not self._market_filter:
            return "RISK_ON", None

        benchmark_symbol = self._market_filter.benchmark.upper()
        frame = self._load_symbol_frame(curated_dir, benchmark_symbol, as_of)
        if frame is None:
            logger.warning(
                "Benchmark data missing for market filter: %s", benchmark_symbol
            )
            return "RISK_OFF", None

        evaluator = RuleEvaluator(self._market_filter.rule)
        series = evaluator.evaluate(frame)
        passed = bool(series.iloc[-1]) if not series.empty else False
        market_state = "RISK_ON" if passed else "RISK_OFF"
        return market_state, passed

    def _serialize_result(self, result: RiskResult) -> dict[str, Any]:
        alerts_payload = [
            {
                "symbol": alert.symbol,
                "type": alert.alert_type,
                "value": alert.value,
                "threshold": alert.threshold,
                "reason": alert.reason,
            }
            for alert in result.alerts
        ]

        payload: dict[str, Any] = {
            "date": result.as_of.isoformat(),
            "evaluated_at": result.evaluated_at.isoformat(),
            "market_state": result.market_state,
            "alerts": alerts_payload,
        }

        if result.benchmark:
            payload["market_filter"] = {
                "benchmark": result.benchmark,
                "passed": result.market_filter_pass,
                "rule": self._market_filter.rule if self._market_filter else None,
            }
        return payload


def load_holdings(path: str | Path) -> HoldingsSnapshot:
    """Load a holdings snapshot from ``path``."""

    holdings_path = Path(path)
    if not holdings_path.is_file():
        raise FileNotFoundError(f"Holdings file not found: {holdings_path}")

    payload = json.loads(holdings_path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("Holdings file must contain a JSON object.")

    raw_positions = payload.get("positions", [])
    if not isinstance(raw_positions, list):
        raise ValueError("Holdings 'positions' must be a list.")

    positions: list[Position] = []
    for raw in raw_positions:
        if not isinstance(raw, Mapping):
            raise ValueError("Each position must be an object.")
        symbol_raw = str(raw.get("symbol", "")).strip()
        if not symbol_raw:
            raise ValueError("Position missing symbol.")
        qty = float(raw.get("qty", 0.0))
        cost_basis = raw.get("cost_basis")
        cost = float(cost_basis) if cost_basis is not None else None
        positions.append(Position(symbol=symbol_raw.upper(), qty=qty, cost_basis=cost))

    positions.sort(key=lambda item: item.symbol)

    as_of_raw = payload.get("as_of_date")
    as_of_date = date.fromisoformat(as_of_raw) if as_of_raw else None

    cash_raw = payload.get("cash")
    cash = float(cash_raw) if cash_raw is not None else None

    base_ccy_raw = payload.get("base_ccy")
    base_ccy = str(base_ccy_raw) if base_ccy_raw is not None else None

    return HoldingsSnapshot(
        as_of_date=as_of_date,
        positions=tuple(positions),
        cash=cash,
        base_ccy=base_ccy,
    )


def _normalize_timestamp(value: date | str | pd.Timestamp) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert(None)
    return timestamp.normalize()


def _safe_float(value: Any) -> float:
    if value is None:
        return math.nan
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan


def _compute_drawdown(close: float, peak: float) -> float:
    if math.isnan(close) or math.isnan(peak) or peak == 0:
        return math.nan
    return close / peak - 1.0


def _is_triggered(value: float, threshold: float) -> bool:
    if math.isnan(value):
        return False
    return value <= threshold


__all__ = [
    "HoldingsSnapshot",
    "Position",
    "RiskAlert",
    "RiskEngine",
    "RiskResult",
    "SymbolRiskEvaluation",
    "load_holdings",
]
