"""Tests for the trading system CLI."""

from pathlib import Path

import pytest
from typer.testing import CliRunner

from trading_system import __version__
from trading_system.cli import app

runner = CliRunner()


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
