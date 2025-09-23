# Copyright (c) 2025 Arye Kogan
# SPDX-License-Identifier: MIT

"""Deterministic backtesting harness built on live trading components."""

from __future__ import annotations

import json
import logging
import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go

from trading_system.config import BacktestConfig, Config
from trading_system.rebalance import RebalanceEngine, RebalanceOrder
from trading_system.risk import HoldingsSnapshot, Position
from trading_system.signals import StrategyEngine

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class BacktestTrade:
    """Executed trade captured during a backtest run."""

    date: date
    symbol: str
    side: str
    quantity: float
    price: float
    fill_price: float
    commission: float
    slippage_cost: float

    @property
    def notional(self) -> float:
        return round(abs(self.quantity) * self.fill_price, 8)


@dataclass(slots=True)
class BacktestResult:
    """Aggregated artefacts and metrics produced by the engine."""

    start: date
    end: date
    metrics: dict[str, Any]
    equity_curve: pd.DataFrame
    trades: pd.DataFrame
    manifest: dict[str, str]
    output_dir: Path | None = None
    metrics_path: Path | None = None
    equity_path: Path | None = None
    trades_path: Path | None = None
    chart_path: Path | None = None


@dataclass(slots=True)
class _PortfolioState:
    cash: float
    positions: dict[str, float] = field(default_factory=dict)

    def snapshot(self, *, as_of: date, base_ccy: str) -> HoldingsSnapshot:
        positions_snapshot = tuple(
            Position(symbol=symbol, qty=qty)
            for symbol, qty in sorted(self.positions.items())
            if abs(qty) > 1e-9
        )
        return HoldingsSnapshot(
            as_of_date=as_of,
            positions=positions_snapshot,
            cash=self.cash,
            base_ccy=base_ccy,
        )


class BacktestEngine:
    """Simulate historical performance using live strategy components."""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._strategy = StrategyEngine(config)
        self._rebalance = RebalanceEngine(config)
        self._backtest_cfg = config.backtest or BacktestConfig()

    def run(
        self,
        *,
        start: date | str | pd.Timestamp,
        end: date | str | pd.Timestamp,
        output_dir: Path,
        label: str | None = None,
        dry_run: bool = False,
        include_chart: bool | None = None,
    ) -> BacktestResult:
        """Execute the backtest and optionally persist artefacts."""

        start_ts = _normalize_date(start)
        end_ts = _normalize_date(end)
        if end_ts < start_ts:
            raise ValueError("Backtest end date must be on or after the start date")

        include_chart = (
            self._backtest_cfg.include_chart if include_chart is None else include_chart
        )

        output_dir = output_dir.resolve()
        if not dry_run:
            output_dir.mkdir(parents=True, exist_ok=True)

        trading_days = pd.bdate_range(start=start_ts, end=end_ts)
        if trading_days.empty:
            raise ValueError("No trading days found between start and end dates")

        state = _PortfolioState(cash=float(self._backtest_cfg.initial_cash))
        equity_records: list[dict[str, Any]] = []
        trades: list[BacktestTrade] = []
        peak_equity = state.cash
        previous_equity = state.cash
        turnover_total = 0.0
        rebalance_events = 0

        for current_ts in trading_days:
            as_of_date = current_ts.date()
            strategy_result = self._strategy.evaluate(current_ts)
            price_map = _extract_prices(strategy_result.evaluations)
            _assert_price_coverage(price_map, state.positions.keys())

            force_rebalance = not state.positions
            rebalance_result = self._rebalance.evaluate(
                current_ts,
                holdings=state.snapshot(
                    as_of=as_of_date, base_ccy=self._config.base_ccy
                ),
                signals=strategy_result.frame,
                force=force_rebalance,
            )

            if rebalance_result.orders:
                day_trades = self._execute_orders(
                    as_of=as_of_date,
                    orders=rebalance_result.orders,
                    price_map=price_map,
                    state=state,
                )
                trades.extend(day_trades)
                turnover_total += float(rebalance_result.turnover)
                rebalance_events += 1

            equity = _portfolio_equity(state, price_map)
            daily_return = (
                0.0 if not equity_records else (equity / previous_equity) - 1.0
            )
            peak_equity = max(peak_equity, equity)
            drawdown = (equity / peak_equity) - 1.0 if peak_equity > 0 else 0.0
            previous_equity = equity

            equity_records.append(
                {
                    "date": as_of_date.isoformat(),
                    "equity": _round(equity),
                    "cash": _round(state.cash),
                    "daily_return": _round(daily_return),
                    "drawdown": _round(drawdown),
                }
            )

        equity_frame = pd.DataFrame(equity_records)
        trades_frame = _trades_frame(trades)
        metrics = self._compute_metrics(
            equity_frame=equity_frame,
            turnover_total=turnover_total,
            rebalance_events=rebalance_events,
            trades_count=len(trades),
            label=label,
        )

        metrics_path = output_dir / "metrics.json"
        equity_path = output_dir / "equity_curve.csv"
        trades_path = output_dir / "trades.csv"
        chart_path = output_dir / "equity_curve.html" if include_chart else None
        manifest: dict[str, str] = {}

        if not dry_run:
            _write_json(metrics_path, metrics)
            equity_frame.to_csv(equity_path, index=False)
            trades_frame.to_csv(trades_path, index=False)
            manifest.update(
                {
                    "metrics": str(metrics_path),
                    "equity_curve": str(equity_path),
                    "trades": str(trades_path),
                }
            )

            if include_chart:
                chart_path = _write_chart(
                    path=chart_path,
                    equity_frame=equity_frame,
                    label=label,
                )
                manifest["equity_curve_chart"] = str(chart_path)

            manifest_path = output_dir / "manifest.json"
            manifest_with_self = {**manifest, "manifest": str(manifest_path)}
            _write_json(manifest_path, manifest_with_self)
            manifest = manifest_with_self

        logger.info(
            "Backtest completed: %s to %s | final_equity=%s | total_return=%.4f",
            metrics["start"],
            metrics["end"],
            metrics["final_equity"],
            metrics["total_return"],
        )

        return BacktestResult(
            start=start_ts.date(),
            end=end_ts.date(),
            metrics=metrics,
            equity_curve=equity_frame,
            trades=trades_frame,
            manifest=manifest,
            output_dir=None if dry_run else output_dir,
            metrics_path=None if dry_run else metrics_path,
            equity_path=None if dry_run else equity_path,
            trades_path=None if dry_run else trades_path,
            chart_path=None if dry_run else chart_path,
        )

    def _compute_metrics(
        self,
        *,
        equity_frame: pd.DataFrame,
        turnover_total: float,
        rebalance_events: int,
        trades_count: int,
        label: str | None,
    ) -> dict[str, Any]:
        initial_cash = float(self._backtest_cfg.initial_cash)
        trading_days_per_year = max(int(self._backtest_cfg.trading_days_per_year), 1)
        annual_rf = float(self._backtest_cfg.annual_risk_free_rate)

        trading_days = int(equity_frame.shape[0])
        final_equity = (
            float(equity_frame["equity"].iloc[-1]) if trading_days else initial_cash
        )
        total_return = (final_equity / initial_cash) - 1.0 if initial_cash else 0.0
        years = trading_days / trading_days_per_year
        if years > 0 and final_equity > 0 and initial_cash > 0:
            cagr = (final_equity / initial_cash) ** (1.0 / years) - 1.0
        else:
            cagr = 0.0

        returns = equity_frame["daily_return"].to_numpy(copy=True)
        if returns.size:
            returns = returns[1:]  # drop the artificial first zero return
        mean_return = returns.mean() if returns.size else 0.0
        std_return = returns.std(ddof=0) if returns.size else 0.0
        volatility = std_return * math.sqrt(trading_days_per_year)
        rf_daily = (1.0 + annual_rf) ** (1.0 / trading_days_per_year) - 1.0
        excess_returns = returns - rf_daily if returns.size else returns
        sharpe = (
            (excess_returns.mean() / std_return) * math.sqrt(trading_days_per_year)
            if std_return > 0
            else 0.0
        )
        downside = returns[returns < 0]
        downside_std = downside.std(ddof=0) if downside.size else 0.0
        sortino = (
            (mean_return - rf_daily) / downside_std * math.sqrt(trading_days_per_year)
            if downside_std > 0
            else float("inf") if returns.size and (returns >= 0).all() else 0.0
        )

        max_drawdown = float(equity_frame["drawdown"].min()) if trading_days else 0.0
        positive_days = (returns > 0).sum() if returns.size else 0
        hit_rate = positive_days / returns.size if returns.size else 0.0
        turnover_average = (
            turnover_total / rebalance_events if rebalance_events else 0.0
        )

        metrics: dict[str, Any] = {
            "start": equity_frame["date"].iloc[0] if trading_days else None,
            "end": equity_frame["date"].iloc[-1] if trading_days else None,
            "trading_days": trading_days,
            "initial_cash": _round(initial_cash),
            "final_equity": _round(final_equity),
            "total_return": _round(total_return),
            "cagr": _round(cagr),
            "volatility": _round(volatility),
            "sharpe": _round(sharpe),
            "sortino": _round(sortino),
            "max_drawdown": _round(max_drawdown),
            "hit_rate": _round(hit_rate),
            "turnover_total": _round(turnover_total),
            "turnover_average": _round(turnover_average),
            "rebalance_events": rebalance_events,
            "trades_executed": trades_count,
            "annual_risk_free_rate": _round(annual_rf),
        }
        if label:
            metrics["label"] = label
        return metrics

    def _execute_orders(
        self,
        *,
        as_of: date,
        orders: Iterable[RebalanceOrder],
        price_map: Mapping[str, float],
        state: _PortfolioState,
    ) -> list[BacktestTrade]:
        slippage_pct = float(self._backtest_cfg.slippage_pct)
        commission = float(self._backtest_cfg.commission_per_trade)
        trades: list[BacktestTrade] = []

        for order in sorted(orders, key=lambda item: item.symbol):
            price = price_map.get(order.symbol)
            if price is None:
                raise ValueError(f"Missing price for symbol {order.symbol} on {as_of}")
            quantity = float(order.quantity)
            if abs(quantity) < 1e-9:
                continue

            if order.side == "BUY":
                fill_price = price * (1.0 + slippage_pct)
                state.cash -= quantity * fill_price
                state.cash -= commission
                state.positions[order.symbol] = (
                    state.positions.get(order.symbol, 0.0) + quantity
                )
                slippage_cost = (fill_price - price) * quantity
            elif order.side == "SELL":
                fill_price = price * (1.0 - slippage_pct)
                state.cash += quantity * fill_price
                state.cash -= commission
                state.positions[order.symbol] = (
                    state.positions.get(order.symbol, 0.0) - quantity
                )
                if abs(state.positions[order.symbol]) < 1e-9:
                    del state.positions[order.symbol]
                slippage_cost = (price - fill_price) * quantity
            else:  # pragma: no cover - defensive
                raise ValueError(f"Unsupported side {order.side}")

            trades.append(
                BacktestTrade(
                    date=as_of,
                    symbol=order.symbol,
                    side=order.side,
                    quantity=_round(quantity),
                    price=_round(price),
                    fill_price=_round(fill_price),
                    commission=_round(commission),
                    slippage_cost=_round(slippage_cost),
                )
            )

        if state.cash < -1e-6:
            logger.warning(
                "Portfolio cash negative after trades on %s: %.6f",
                as_of,
                state.cash,
            )

        return trades


def _portfolio_equity(state: _PortfolioState, price_map: Mapping[str, float]) -> float:
    equity = state.cash
    for symbol, qty in state.positions.items():
        price = price_map.get(symbol)
        if price is None:
            raise ValueError(f"Missing price for symbol {symbol}")
        equity += qty * price
    return float(equity)


def _extract_prices(
    evaluations: Mapping[str, Any]
) -> dict[str, float]:  # Mapping[str, SymbolEvaluation]
    prices: dict[str, float] = {}
    for symbol, evaluation in evaluations.items():
        indicators = getattr(evaluation, "indicators", {})
        close = indicators.get("close")
        if close is None or math.isnan(close):
            raise ValueError(f"Curated data missing closing price for {symbol}")
        prices[symbol] = float(close)
    return prices


def _assert_price_coverage(
    price_map: Mapping[str, float], symbols: Iterable[str]
) -> None:
    missing = [symbol for symbol in symbols if symbol not in price_map]
    if missing:
        raise ValueError(
            "Missing prices for positions: " + ", ".join(sorted(set(missing)))
        )


def _trades_frame(trades: list[BacktestTrade]) -> pd.DataFrame:
    if not trades:
        return pd.DataFrame(
            columns=[
                "date",
                "symbol",
                "side",
                "quantity",
                "price",
                "fill_price",
                "commission",
                "slippage_cost",
                "notional",
            ]
        )

    data = [
        {
            "date": trade.date.isoformat(),
            "symbol": trade.symbol,
            "side": trade.side,
            "quantity": trade.quantity,
            "price": trade.price,
            "fill_price": trade.fill_price,
            "commission": trade.commission,
            "slippage_cost": trade.slippage_cost,
            "notional": _round(trade.notional),
        }
        for trade in trades
    ]
    return pd.DataFrame(data)


def _normalize_date(value: date | str | pd.Timestamp) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert(None)
    return timestamp.normalize()


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def _write_chart(
    *, path: Path | None, equity_frame: pd.DataFrame, label: str | None
) -> Path:
    assert path is not None
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=equity_frame["date"],
            y=equity_frame["equity"],
            name="Equity",
            mode="lines",
        )
    )
    figure.add_trace(
        go.Scatter(
            x=equity_frame["date"],
            y=equity_frame["drawdown"],
            name="Drawdown",
            mode="lines",
            yaxis="y2",
        )
    )
    figure.update_layout(
        title=f"Backtest Equity Curve{f' – {label}' if label else ''}",
        yaxis=dict(title="Equity"),
        yaxis2=dict(title="Drawdown", overlaying="y", side="right"),
        xaxis=dict(title="Date"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    figure.write_html(
        path,
        include_plotlyjs="cdn",
        full_html=True,
        div_id="equity_curve_chart",
    )
    return path


def _round(value: float, digits: int = 8) -> float:
    return round(float(value), digits)


__all__ = ["BacktestEngine", "BacktestResult", "BacktestTrade"]
