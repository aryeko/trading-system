from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from trading_system.config import load_config
from trading_system.signals import StrategyEngine

CONFIG_TEMPLATE = """
base_ccy: USD
calendar: NYSE
data:
  provider: yahoo
  lookback_days: 30
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
  max_positions: 5
notify:
  email: ops@example.com
paths:
  data_raw: data/raw
  data_curated: data/curated
  reports: reports
"""


def _write_config(tmp_path: Path, tickers: list[str]) -> Path:
    config_text = CONFIG_TEMPLATE.format(tickers=", ".join(tickers))
    config_path = tmp_path / "config.yml"
    config_path.write_text(config_text, encoding="utf-8")
    return config_path


def _make_curated_frame(
    dates: pd.DatetimeIndex,
    close_values: np.ndarray,
    symbol: str,
    *,
    sma_offset: float,
) -> pd.DataFrame:
    series = pd.Series(close_values, index=dates)
    values = series.to_numpy(dtype=float, copy=True)
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
    return frame


def _write_curated(
    config_path: Path,
    as_of: pd.Timestamp,
    frames: dict[str, pd.DataFrame],
) -> Path:
    config = load_config(config_path)
    curated_dir = config.paths.data_curated / as_of.strftime("%Y-%m-%d")
    curated_dir.mkdir(parents=True, exist_ok=True)
    for symbol, frame in frames.items():
        frame.to_parquet(curated_dir / f"{symbol}.parquet", index=False)
    return curated_dir


def test_strategy_engine_generates_signals_and_features(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    config_path = _write_config(tmp_path, ["AAPL", "MSFT"])
    as_of = pd.Timestamp("2024-05-02")
    dates = pd.bdate_range(end=as_of, periods=80)

    upward_prices = np.linspace(100, 140, len(dates))
    downward_prices = np.linspace(140, 90, len(dates))

    frames = {
        "AAPL": _make_curated_frame(dates, upward_prices, "AAPL", sma_offset=-1.0),
        "MSFT": _make_curated_frame(dates, downward_prices, "MSFT", sma_offset=1.0),
    }
    _write_curated(config_path, as_of, frames)

    config = load_config(config_path)
    engine = StrategyEngine(config)

    with caplog.at_level(logging.INFO):
        result = engine.evaluate(as_of)

    assert result.entry_count == 1
    assert result.exit_count == 1
    frame = result.frame
    assert set(frame.columns).issuperset(
        {"date", "symbol", "signal", "rank_score", "momentum_63d"}
    )

    aapl_row = frame.loc[frame["symbol"] == "AAPL"].iloc[0]
    aapl_eval = result.evaluations["AAPL"]
    assert aapl_row["signal"] == "BUY"
    assert aapl_eval.entry_rule is True
    assert aapl_eval.exit_rule is False

    expected_momentum = (
        frames["AAPL"]["close"].iloc[-1] / frames["AAPL"]["close"].iloc[-64] - 1.0
    )
    assert aapl_row["rank_score"] == pytest.approx(expected_momentum)

    msft_eval = result.evaluations["MSFT"]
    assert msft_eval.signal == "EXIT"
    assert any(
        "processed 2 symbols (1 entry, 1 exit)" in record.message
        for record in caplog.records
    )


def test_strategy_engine_tie_breaks_alphabetically(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, ["AAA", "AAC"])
    as_of = pd.Timestamp("2024-06-03")
    dates = pd.bdate_range(end=as_of, periods=70)
    prices = np.linspace(50, 100, len(dates))
    frames = {
        "AAA": _make_curated_frame(dates, prices, "AAA", sma_offset=-1.0),
        "AAC": _make_curated_frame(dates, prices, "AAC", sma_offset=-1.0),
    }
    _write_curated(config_path, as_of, frames)

    config = load_config(config_path)
    engine = StrategyEngine(config)
    result = engine.evaluate(as_of)

    order = result.frame["symbol"].tolist()
    assert order == ["AAA", "AAC"]
    assert result.frame["rank_score"].iloc[0] == pytest.approx(
        result.frame["rank_score"].iloc[1]
    )


def test_strategy_engine_build_persists_signals(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, ["AAPL"])
    as_of = pd.Timestamp("2024-05-10")
    dates = pd.bdate_range(end=as_of, periods=65)
    prices = np.linspace(90, 120, len(dates))
    frame = _make_curated_frame(dates, prices, "AAPL", sma_offset=-1.0)
    _write_curated(config_path, as_of, {"AAPL": frame})

    config = load_config(config_path)
    engine = StrategyEngine(config)
    result = engine.build(as_of, dry_run=False)

    output_path = config.paths.reports / as_of.strftime("%Y-%m-%d") / "signals.parquet"
    assert output_path.exists()
    assert result.output_path == output_path
    stored = pd.read_parquet(output_path)
    assert list(stored.columns[:4]) == ["date", "symbol", "signal", "rank_score"]


def test_strategy_engine_build_dry_run_skips_write(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, ["AAPL"])
    as_of = pd.Timestamp("2024-05-10")
    dates = pd.bdate_range(end=as_of, periods=65)
    prices = np.linspace(90, 120, len(dates))
    frame = _make_curated_frame(dates, prices, "AAPL", sma_offset=-1.0)
    _write_curated(config_path, as_of, {"AAPL": frame})

    config = load_config(config_path)
    engine = StrategyEngine(config)
    result = engine.build(as_of, dry_run=True)

    output_path = config.paths.reports / as_of.strftime("%Y-%m-%d") / "signals.parquet"
    assert not output_path.exists()
    assert result.output_path is None
