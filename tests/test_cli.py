"""Tests for the trading system CLI."""

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
