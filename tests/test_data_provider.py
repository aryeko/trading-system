"""Tests for data provider interfaces and persistence."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from trading_system.config import load_config
from trading_system.data import (
    BARS_COLUMN_ORDER,
    RawDataWriter,
    YahooDataProvider,
    run_data_pull,
)
from trading_system.data.provider import (
    DataProvider,
    DataUnavailableError,
    ensure_bars_frame,
)

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
paths:
  data_raw: data/raw
  data_curated: data/curated
  reports: reports
"""


def write_config(tmp_path: Path, text: str) -> Path:
    config_path = tmp_path / "config.yml"
    config_path.write_text(text, encoding="utf-8")
    return config_path


def sample_bars(symbol: str) -> str:
    return (
        "Date,Open,High,Low,Close,Adj Close,Volume\n"
        "2024-05-01,100,110,95,108,108,12345\n"
        "2024-05-02,109,112,107,111,111,15000\n"
    )


def build_frame(symbol: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-05-01", "2024-05-02"]),
            "symbol": [symbol, symbol],
            "open": [100.0, 109.0],
            "high": [110.0, 112.0],
            "low": [95.0, 107.0],
            "close": [108.0, 111.0],
            "adj_close": [108.0, 111.0],
            "volume": [12345, 15000],
        }
    )


def test_yahoo_provider_parses_csv(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = YahooDataProvider()
    monkeypatch.setattr(
        provider,
        "_download_csv",
        lambda symbol, start, end: sample_bars(symbol),
        raising=False,
    )

    bars = provider.get_bars(["AAPL"], start=date(2024, 5, 1), end=date(2024, 5, 2))

    assert list(bars.columns) == list(BARS_COLUMN_ORDER)
    assert bars["symbol"].unique().tolist() == ["AAPL"]
    assert bars["open"].iloc[0] == pytest.approx(100.0)
    assert pd.Timestamp("2024-05-01") == bars["date"].iloc[0]


def test_yahoo_provider_skips_missing_symbols(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    provider = YahooDataProvider()

    def fake_download(symbol: str, start: date, end: date) -> str:
        if symbol == "MSFT":
            raise DataUnavailableError(symbol)
        return sample_bars(symbol)

    monkeypatch.setattr(provider, "_download_csv", fake_download, raising=False)

    with caplog.at_level("WARNING"):
        bars = provider.get_bars(
            ["AAPL", "MSFT"], start=date(2024, 5, 1), end=date(2024, 5, 2)
        )

    assert bars["symbol"].unique().tolist() == ["AAPL"]
    assert "MSFT" in caplog.text


def test_raw_data_writer_persists_files_and_meta(tmp_path: Path) -> None:
    bars = pd.concat([build_frame("AAPL"), build_frame("MSFT")], ignore_index=True)
    benchmark = build_frame("SPY")
    writer = RawDataWriter(tmp_path)

    as_of = date(2024, 5, 2)
    result = writer.persist(
        as_of=as_of,
        bars=bars,
        start=date(2024, 4, 1),
        end=as_of,
        benchmark_symbol="SPY",
        benchmark_frame=benchmark,
    )

    run_dir = tmp_path / as_of.isoformat()
    assert run_dir.is_dir()
    for symbol in ("AAPL", "MSFT"):
        path = run_dir / f"{symbol}.parquet"
        assert path.is_file()
        restored = pd.read_parquet(path)
        pd.testing.assert_frame_equal(restored, ensure_bars_frame(build_frame(symbol)))

    benchmark_path = run_dir / "benchmark_SPY.parquet"
    assert benchmark_path.is_file()

    meta_path = run_dir / "meta_run.json"
    assert meta_path.is_file()
    first_meta = meta_path.read_text(encoding="utf-8")

    # Re-run to confirm deterministic output for the same as_of date.
    writer.persist(
        as_of=as_of,
        bars=bars,
        start=date(2024, 4, 1),
        end=as_of,
        benchmark_symbol="SPY",
        benchmark_frame=benchmark,
    )
    second_meta = meta_path.read_text(encoding="utf-8")
    assert first_meta == second_meta

    payload = json.loads(first_meta)
    assert payload["symbols"] == ["AAPL", "MSFT"]
    assert payload["benchmark"] == "SPY"
    assert payload["last_bar_date"] == "2024-05-02"
    assert result.directory == run_dir


class StubProvider(DataProvider):
    def __init__(self, bars: pd.DataFrame, benchmark: pd.DataFrame) -> None:
        self._bars = bars
        self._benchmark = benchmark
        self.bar_requests: list[tuple[tuple[str, ...], date, date]] = []
        self.benchmark_requests: list[tuple[str, date, date]] = []

    def get_bars(
        self,
        universe: Sequence[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        self.bar_requests.append((tuple(universe), start, end))
        return self._bars

    def get_benchmark(self, symbol: str, start: date, end: date) -> pd.DataFrame:
        self.benchmark_requests.append((symbol, start, end))
        return self._benchmark


def test_run_data_pull_writes_artifacts(tmp_path: Path) -> None:
    config_path = write_config(tmp_path, SAMPLE_CONFIG)
    config = load_config(config_path)

    bars = pd.concat([build_frame("AAPL"), build_frame("MSFT")], ignore_index=True)
    benchmark = build_frame("SPY")

    provider = StubProvider(bars=bars, benchmark=benchmark)
    as_of = date(2024, 5, 2)
    run_timestamp = datetime(2024, 5, 2, tzinfo=timezone.utc)

    result = run_data_pull(
        config,
        provider,
        as_of=as_of,
        run_at=run_timestamp,
    )

    expected_start = as_of - timedelta(days=config.data.lookback_days or 0)
    assert provider.bar_requests == [
        (tuple(config.universe.tickers), expected_start, as_of)
    ]
    assert provider.benchmark_requests == [
        (config.risk.market_filter.benchmark, expected_start, as_of)
    ]

    run_dir = config.paths.data_raw / as_of.isoformat()
    assert run_dir == result.directory
    assert (run_dir / "AAPL.parquet").exists()
    assert (run_dir / "benchmark_SPY.parquet").exists()

    with (run_dir / "meta_run.json").open("r", encoding="utf-8") as handle:
        meta = json.load(handle)

    assert meta["timestamp"] == run_timestamp.isoformat()
    assert meta["symbols"] == ["AAPL", "MSFT"]
    assert meta["benchmark"] == "SPY"
