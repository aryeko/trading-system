"""Tests for the configuration loader."""

from __future__ import annotations

from pathlib import Path

import pytest

from trading_system.config import Config, load_config

SAMPLE_CONFIG = """
base_ccy: USD
calendar: NYSE
data:
  provider: eodhd
  lookback_days: 420
universe:
  tickers: [AAPL, MSFT, NVDA]
strategy:
  type: trend_follow
  entry: "close > sma_100"
  exit: "close < sma_100"
  rank: "momentum_63d"
risk:
  crash_threshold_pct: -0.08
  drawdown_threshold_pct: -0.2
  market_filter:
    benchmark: SPY
    rule: "close > sma_200"
rebalance:
  cadence: monthly
  max_positions: 8
  equal_weight: true
  min_weight: 0.05
  cash_buffer: 0.05
notify:
  email: ops@example.com
preprocess:
  forward_fill_limit: 2
  rolling_peak_window: 126
paths:
  data_raw: data/raw
  data_curated: data/curated
  reports: reports
"""


def write_config(tmp_path: Path, text: str) -> Path:
    config_path = tmp_path / "config.yml"
    config_path.write_text(text, encoding="utf-8")
    return config_path


def test_load_config_creates_directories(tmp_path: Path) -> None:
    """Successful load returns Config instance and creates directories."""

    config_path = write_config(tmp_path, SAMPLE_CONFIG)

    config = load_config(config_path)

    assert isinstance(config, Config)
    expected_raw = (tmp_path / "data" / "raw").resolve()
    expected_curated = (tmp_path / "data" / "curated").resolve()
    expected_reports = (tmp_path / "reports").resolve()

    assert config.paths.data_raw == expected_raw
    assert config.paths.data_curated == expected_curated
    assert config.paths.reports == expected_reports

    assert config.preprocess is not None
    assert config.preprocess.forward_fill_limit == 2
    assert config.preprocess.rolling_peak_window == 126

    for directory in config.paths.directories:
        assert directory.is_dir()

    # Idempotent on repeated calls.
    config_repeat = load_config(config_path)
    assert config_repeat.paths.data_raw == expected_raw


def test_missing_required_key_raises(tmp_path: Path) -> None:
    """Missing required top-level keys raise a clear error."""

    config_text = """
calendar: NYSE
paths:
  data_raw: data/raw
  data_curated: data/curated
  reports: reports
"""
    config_path = write_config(tmp_path, config_text)

    with pytest.raises(Exception) as exc_info:
        load_config(config_path)

    message = str(exc_info.value)
    assert "base_ccy" in message or "paths" in message
