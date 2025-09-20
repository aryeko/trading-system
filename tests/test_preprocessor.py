"""Tests for the preprocessing module."""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from trading_system.config import load_config
from trading_system.preprocess import CANONICAL_COLUMNS, Preprocessor, PreprocessResult

CONFIG_TEMPLATE = """
base_ccy: USD
calendar: NYSE
data:
  provider: eodhd
  adjust: splits_dividends
universe:
  tickers: [AAPL]
strategy:
  type: trend_follow
  entry: "close > sma_100"
  exit: "close < sma_100"
risk:
  crash_threshold_pct: -0.08
  drawdown_threshold_pct: -0.2
rebalance:
  cadence: monthly
  max_positions: 5
notify:
  email: ops@example.com
preprocess:
  forward_fill_limit: {forward_fill_limit}
  rolling_peak_window: {rolling_peak_window}
paths:
  data_raw: data/raw
  data_curated: data/curated
  reports: reports
"""


def load_test_config(
    tmp_path: Path, *, forward_fill_limit: int, rolling_peak_window: int
) -> tuple[Preprocessor, Path, Path]:
    """Create a configuration file and return a preprocessor with paths."""

    config_text = CONFIG_TEMPLATE.format(
        forward_fill_limit=forward_fill_limit,
        rolling_peak_window=rolling_peak_window,
    )
    config_path = tmp_path / "config.yml"
    config_path.write_text(config_text, encoding="utf-8")

    config = load_config(config_path)
    preprocessor = Preprocessor(config)
    return preprocessor, config.paths.data_raw, config.paths.data_curated


def test_preprocessor_curates_and_derives_features(tmp_path: Path) -> None:
    """Raw bars are aligned to calendar and indicators are derived."""

    preprocessor, raw_base, curated_base = load_test_config(
        tmp_path, forward_fill_limit=1, rolling_peak_window=5
    )

    as_of = pd.Timestamp("2024-02-05")
    raw_dir = raw_base / as_of.strftime("%Y-%m-%d")
    raw_dir.mkdir(parents=True, exist_ok=True)

    calendar = pd.bdate_range("2024-01-02", as_of)
    missing_date = calendar[10]
    raw_dates = calendar.delete(10)

    base_prices = pd.Series(100 + np.arange(len(calendar)) * 2.0, index=calendar)

    frame = pd.DataFrame(
        {
            "date": raw_dates,
            "symbol": "AAPL",
            "open": base_prices.loc[raw_dates] - 1.0,
            "high": base_prices.loc[raw_dates] + 1.0,
            "low": base_prices.loc[raw_dates] - 2.0,
            "close": base_prices.loc[raw_dates],
            "volume": 1_000 + np.arange(len(raw_dates)),
            "adj_close": base_prices.loc[raw_dates] * 0.5,
        }
    )
    frame.to_parquet(raw_dir / "AAPL.parquet", index=False)

    result = preprocessor.run(as_of)

    assert isinstance(result, PreprocessResult)
    assert result.as_of == as_of.normalize().date()
    assert result.symbols == ("AAPL",)
    assert "AAPL" in result.artifacts

    curated = pd.read_parquet(result.artifacts["AAPL"])

    assert list(curated.columns) == CANONICAL_COLUMNS
    assert len(curated) == len(calendar)

    gap_row = curated.loc[curated["date"] == missing_date]
    prev_row = curated.loc[curated["date"] == missing_date - pd.tseries.offsets.BDay()]
    assert not gap_row.empty
    assert gap_row["close"].iloc[0] == pytest.approx(prev_row["close"].iloc[0])

    last_row = curated.iloc[-1]
    expected_close = base_prices.loc[as_of] * 0.5
    assert last_row["close"] == pytest.approx(expected_close)
    assert last_row["adj_close"] == pytest.approx(expected_close)

    previous_close = curated.iloc[-2]["close"]
    expected_ret_1d = (last_row["close"] / previous_close) - 1.0
    assert last_row["ret_1d"] == pytest.approx(expected_ret_1d)

    twentieth_prior = curated.iloc[-21]["close"]
    expected_ret_20d = (last_row["close"] / twentieth_prior) - 1.0
    assert last_row["ret_20d"] == pytest.approx(expected_ret_20d)

    close_series = curated["close"]
    expected_peak = close_series.iloc[-5:].max()
    assert last_row["rolling_peak"] == pytest.approx(expected_peak)


def test_preprocessor_logs_when_gap_exceeds_limit(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Gaps beyond the forward-fill tolerance emit a warning."""

    preprocessor, raw_base, curated_base = load_test_config(
        tmp_path, forward_fill_limit=0, rolling_peak_window=5
    )

    as_of = pd.Timestamp("2024-01-10")
    raw_dir = raw_base / as_of.strftime("%Y-%m-%d")
    raw_dir.mkdir(parents=True, exist_ok=True)

    calendar = pd.bdate_range("2024-01-02", as_of)
    raw_dates = calendar.delete(3)
    raw_dates = raw_dates.delete(3)

    frame = pd.DataFrame(
        {
            "date": raw_dates,
            "symbol": "AAPL",
            "open": 100 + np.arange(len(raw_dates)),
            "high": 101 + np.arange(len(raw_dates)),
            "low": 99 + np.arange(len(raw_dates)),
            "close": 100 + np.arange(len(raw_dates)),
            "volume": 500 + np.arange(len(raw_dates)),
            "adj_close": 100 + np.arange(len(raw_dates)),
        }
    )
    frame.to_parquet(raw_dir / "AAPL.parquet", index=False)

    with caplog.at_level(logging.WARNING):
        preprocessor.run(as_of)

    warnings = [
        record.message for record in caplog.records if record.levelno == logging.WARNING
    ]
    assert any("Missing close data for AAPL" in message for message in warnings)

    curated = pd.read_parquet(
        curated_base / as_of.strftime("%Y-%m-%d") / "AAPL.parquet"
    )
    missing_rows = curated["close"].isna()
    assert missing_rows.sum() == 2
