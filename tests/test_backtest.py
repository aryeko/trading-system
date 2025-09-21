from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from typer.testing import CliRunner

from trading_system.backtest import BacktestEngine
from trading_system.cli import app
from trading_system.config import Config, load_config

CONFIG_TEMPLATE = """
base_ccy: USD
calendar: NYSE
data:
  provider: yahoo
  lookback_days: 420
universe:
  tickers: [{tickers}]
strategy:
  type: trend_follow
  entry: "close > sma_100"
  exit: "close < sma_100"
  rank: momentum_63d
risk:
  crash_threshold_pct: -0.08
  drawdown_threshold_pct: -0.20
rebalance:
  cadence: monthly
  max_positions: 4
  equal_weight: true
  min_weight: 0.05
  cash_buffer: 0.05
notify:
  email: ops@example.com
  slack_webhook: https://hooks.slack.test/XYZ
paths:
  data_raw: {base}/data/raw
  data_curated: {base}/data/curated
  reports: {base}/reports
backtest:
  initial_cash: 100000
  slippage_pct: 0.001
  commission_per_trade: 1.0
  annual_risk_free_rate: 0.0
  include_chart: true
  trading_days_per_year: 252
"""


def _write_config(tmp_path: Path, tickers: list[str]) -> Path:
    config_text = CONFIG_TEMPLATE.format(tickers=", ".join(tickers), base=tmp_path)
    config_path = tmp_path / "config.yml"
    config_path.write_text(config_text, encoding="utf-8")
    return config_path


def _build_history_frames(
    dates: pd.DatetimeIndex, tickers: list[str]
) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    for index, symbol in enumerate(tickers):
        upward = index % 2 == 0
        slope = 0.6 if upward else -0.4
        base = 100 + index * 5
        prices = base + slope * np.arange(len(dates))
        series = pd.Series(prices, index=dates, dtype=float)
        values = series.to_numpy(copy=True)
        sma_offset = -1.0 if upward else 1.0
        frame = pd.DataFrame(
            {
                "date": dates,
                "symbol": symbol,
                "open": values,
                "high": values,
                "low": values,
                "close": values,
                "volume": np.full(len(series), 1_000),
                "adj_close": values,
                "sma_100": values + sma_offset,
                "sma_200": values + sma_offset,
                "ret_1d": series.pct_change().fillna(0.0).values,
                "ret_20d": series.pct_change(20).fillna(0.0).values,
                "rolling_peak": series.cummax().values,
            }
        )
        frames[symbol] = frame
    return frames


def _write_curated_history(
    config: Config,
    frames: dict[str, pd.DataFrame],
    as_of_dates: pd.DatetimeIndex,
) -> None:
    for as_of in as_of_dates:
        curated_dir = config.paths.data_curated / as_of.strftime("%Y-%m-%d")
        curated_dir.mkdir(parents=True, exist_ok=True)
        for symbol, frame in frames.items():
            filtered = frame[frame["date"] <= as_of]
            filtered.to_parquet(curated_dir / f"{symbol}.parquet", index=False)


def test_backtest_engine_produces_deterministic_metrics(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, ["AAA", "BBB"])
    config = load_config(config_path)
    start = pd.Timestamp("2024-01-02")
    end = pd.Timestamp("2024-04-30")
    history_dates = pd.bdate_range(end=end, periods=120)
    frames = _build_history_frames(history_dates, ["AAA", "BBB"])
    as_of_dates = pd.bdate_range(start=start, end=end)
    _write_curated_history(config, frames, as_of_dates)

    engine = BacktestEngine(config)
    output_dir = config.paths.reports / "backtests" / "demo"
    result = engine.run(
        start=start.date(),
        end=end.date(),
        output_dir=output_dir,
        label="demo",
        include_chart=False,
    )

    metrics = result.metrics
    assert metrics["label"] == "demo"
    assert metrics["trading_days"] == len(as_of_dates)
    assert metrics["final_equity"] > metrics["initial_cash"] * 0.9
    assert not result.equity_curve.empty
    assert (output_dir / "metrics.json").exists()
    assert (output_dir / "equity_curve.csv").exists()
    assert (output_dir / "trades.csv").exists()

    dry_run_metrics = engine.run(
        start=start.date(),
        end=end.date(),
        output_dir=output_dir,
        label="demo",
        dry_run=True,
        include_chart=False,
    ).metrics
    assert dry_run_metrics == metrics


def test_backtest_cli_run_and_compare(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, ["AAA"])
    config = load_config(config_path)
    start = pd.Timestamp("2024-02-01")
    end = pd.Timestamp("2024-03-15")
    history_dates = pd.bdate_range(end=end, periods=80)
    frames = _build_history_frames(history_dates, ["AAA"])
    as_of_dates = pd.bdate_range(start=start, end=end)
    _write_curated_history(config, frames, as_of_dates)

    runner = CliRunner()
    base_dir = config.paths.reports / "backtests" / "base"
    result = runner.invoke(
        app,
        [
            "backtest",
            "run",
            "--config",
            str(config_path),
            "--start",
            start.strftime("%Y-%m-%d"),
            "--end",
            end.strftime("%Y-%m-%d"),
            "--output",
            str(base_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert (base_dir / "metrics.json").exists()
    assert (base_dir / "equity_curve.html").exists()

    candidate_dir = config.paths.reports / "backtests" / "candidate"
    result_candidate = runner.invoke(
        app,
        [
            "backtest",
            "run",
            "--config",
            str(config_path),
            "--start",
            start.strftime("%Y-%m-%d"),
            "--end",
            end.strftime("%Y-%m-%d"),
            "--output",
            str(candidate_dir),
            "--label",
            "candidate",
            "--no-chart",
        ],
    )
    assert result_candidate.exit_code == 0, result_candidate.output
    assert (candidate_dir / "metrics.json").exists()
    assert not (candidate_dir / "equity_curve.html").exists()

    compare = runner.invoke(
        app,
        [
            "backtest",
            "compare",
            "--baseline",
            str(base_dir),
            "--candidate",
            str(candidate_dir),
        ],
    )
    assert compare.exit_code == 0, compare.output
    assert "delta" in compare.output
