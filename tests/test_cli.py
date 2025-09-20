"""Tests for the trading system CLI."""

import json
from collections.abc import Sequence
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from trading_system import __version__
from trading_system.cli import app
from trading_system.data.provider import DataProvider

runner = CliRunner()

PREPROCESS_CONFIG = """
base_ccy: USD
calendar: NYSE
data:
  provider: yahoo
  lookback_days: 1
universe:
  tickers: [AAPL]
strategy:
  type: trend_follow
  entry: "close > sma_100"
  exit: "close < sma_100"
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
preprocess:
  forward_fill_limit: 1
  rolling_peak_window: 5
"""


def _write_preprocess_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "config.yml"
    config_path.write_text(PREPROCESS_CONFIG, encoding="utf-8")
    return config_path


def test_cli_help_lists_commands() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "version" in result.stdout
    assert "info" in result.stdout


def test_version_command_outputs_package_version() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert __version__ in result.stdout


def test_doctor_command_honors_environment_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TS_DOCTOR_REQUIRED", "python")

    result = runner.invoke(app, ["doctor"])

    assert result.exit_code == 0
    assert "python" in result.stdout


def test_config_new_generates_template(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yml"

    result = runner.invoke(app, ["config", "new", "--path", str(config_path)])

    assert result.exit_code == 0
    assert config_path.is_file()
    contents = config_path.read_text(encoding="utf-8")
    assert "base_ccy" in contents


def test_config_inspect_prints_summary(tmp_path: Path) -> None:
    config_text = """
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
    config_path = tmp_path / "config.yml"
    config_path.write_text(config_text, encoding="utf-8")

    result = runner.invoke(app, ["config", "inspect", "--path", str(config_path)])

    assert result.exit_code == 0
    assert "Inspecting configuration:" in result.stdout
    assert "provider=yahoo" in result.stdout


def test_data_providers_lists_registry() -> None:
    result = runner.invoke(app, ["data", "providers"])

    assert result.exit_code == 0
    assert "yahoo" in result.stdout


def test_data_pull_command_uses_stub_provider(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_text = """
base_ccy: USD
calendar: NYSE
data:
  provider: stub
  lookback_days: 1
universe:
  tickers: [AAPL, MSFT]
strategy:
  type: trend_follow
  entry: "close > sma_100"
  exit: "close < sma_100"
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

    config_path = tmp_path / "config.yml"
    config_path.write_text(config_text, encoding="utf-8")

    dates = pd.to_datetime(["2024-05-01", "2024-05-02"])
    bars = pd.concat(
        [
            pd.DataFrame(
                {
                    "date": dates,
                    "symbol": symbol,
                    "open": [100.0, 101.0],
                    "high": [101.0, 102.0],
                    "low": [99.0, 100.0],
                    "close": [100.5, 101.5],
                    "adj_close": [100.5, 101.5],
                    "volume": [1_000, 1_100],
                }
            )
            for symbol in ("AAPL", "MSFT")
        ],
        ignore_index=True,
    )

    class StubCliProvider(DataProvider):
        def get_bars(
            self,
            universe: Sequence[str],
            start: date,
            end: date,
        ) -> pd.DataFrame:
            return bars

        def get_benchmark(self, symbol: str, start: date, end: date) -> pd.DataFrame:
            raise NotImplementedError

    from trading_system import cli as cli_module

    monkeypatch.setitem(
        cli_module.DATA_PROVIDER_FACTORIES,
        "stub",
        lambda: StubCliProvider(),
    )

    result = runner.invoke(
        app,
        [
            "data",
            "pull",
            "--config",
            str(config_path),
            "--as-of",
            "2024-05-02",
            "--provider",
            "stub",
            "--skip-benchmark",
        ],
    )

    assert result.exit_code == 0

    run_dir = config_path.parent / "data" / "raw" / "2024-05-02"
    assert (run_dir / "AAPL.parquet").exists()
    assert (run_dir / "MSFT.parquet").exists()


def test_data_inspect_summarizes_run(tmp_path: Path) -> None:
    run_dir = tmp_path / "data" / "raw" / "2024-05-02"
    run_dir.mkdir(parents=True, exist_ok=True)

    meta_payload = {
        "timestamp": "2024-05-02T18:00:00+00:00",
        "symbols": ["AAPL"],
        "start": "2024-04-01",
        "end": "2024-05-02",
        "last_bar_date": "2024-05-02",
    }
    (run_dir / "meta_run.json").write_text(
        json.dumps(meta_payload, indent=2), encoding="utf-8"
    )

    pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-05-01"]),
            "symbol": ["AAPL"],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "adj_close": [100.5],
            "volume": [1_000],
        }
    ).to_parquet(run_dir / "AAPL.parquet", index=False)

    result = runner.invoke(app, ["data", "inspect", "--run", str(run_dir)])

    assert result.exit_code == 0
    assert "AAPL" in result.stdout
    assert "timestamp" in result.stdout


def test_data_preprocess_dry_run_lists_symbols(tmp_path: Path) -> None:
    config_path = _write_preprocess_config(tmp_path)
    run_dir = config_path.parent / "data" / "raw" / "2024-05-02"
    run_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-05-01", "2024-05-02"]),
            "symbol": ["AAPL", "AAPL"],
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, 101.5],
            "adj_close": [100.5, 101.5],
            "volume": [1_000, 1_100],
        }
    ).to_parquet(run_dir / "AAPL.parquet", index=False)

    result = runner.invoke(
        app,
        [
            "data",
            "preprocess",
            "--config",
            str(config_path),
            "--as-of",
            "2024-05-02",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    assert "AAPL.parquet" in result.stdout


def test_data_preprocess_writes_curated_outputs(tmp_path: Path) -> None:
    config_path = _write_preprocess_config(tmp_path)
    run_dir = config_path.parent / "data" / "raw" / "2024-05-02"
    run_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-05-01", "2024-05-02"]),
            "symbol": ["AAPL", "AAPL"],
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, 101.5],
            "adj_close": [100.5, 101.5],
            "volume": [1_000, 1_100],
        }
    ).to_parquet(run_dir / "AAPL.parquet", index=False)

    result = runner.invoke(
        app,
        [
            "data",
            "preprocess",
            "--config",
            str(config_path),
            "--as-of",
            "2024-05-02",
        ],
    )

    curated_path = (
        config_path.parent / "data" / "curated" / "2024-05-02" / "AAPL.parquet"
    )
    assert result.exit_code == 0
    assert curated_path.exists()
