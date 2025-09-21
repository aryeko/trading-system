from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from email.message import EmailMessage
from pathlib import Path
from smtplib import SMTP
from types import SimpleNamespace, TracebackType
from typing import Any, cast

import pytest
from requests import Response

from trading_system.config import Config, load_config
from trading_system.notify import (
    EmailChannel,
    NotificationError,
    NotificationService,
    SlackChannel,
    load_report_summary,
)

REPORT_PAYLOAD: dict[str, Any] = {
    "as_of": "2024-05-02",
    "generated_at": "2024-05-02T22:30:00+00:00",
    "base_currency": "USD",
    "risk": {
        "market_state": "RISK_OFF",
        "alerts": [
            {
                "symbol": "AAPL",
                "type": "CRASH",
                "reason": "Daily return -0.0850 <= threshold -0.0800",
                "value": -0.085,
                "threshold": -0.08,
            }
        ],
    },
    "actions": {
        "orders": [
            {
                "symbol": "MSFT",
                "side": "BUY",
                "quantity": 10,
                "notional": 3500.0,
            }
        ],
        "exits": ["TSLA"],
        "status": "READY",
        "turnover": 0.12,
    },
    "notes": ["PDF skipped due to missing renderer"],
}


CONFIG_TEMPLATE = """
base_ccy: USD
calendar: NYSE
data:
  provider: yahoo
  lookback_days: 30
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
  slack_webhook: https://hooks.slack.test/ABC
paths:
  data_raw: data/raw
  data_curated: data/curated
  reports: reports
"""


@pytest.fixture
def config(tmp_path: Path) -> Config:
    config_path = tmp_path / "config.yml"
    config_path.write_text(CONFIG_TEMPLATE, encoding="utf-8")
    return load_config(config_path)


def _write_report(config: Config, payload: Mapping[str, Any]) -> Path:
    report_dir = config.paths.reports / "2024-05-02"
    report_dir.mkdir(parents=True, exist_ok=True)
    json_path = report_dir / "daily_report.json"
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    (report_dir / "daily_report.html").write_text("<html></html>", encoding="utf-8")
    (report_dir / "daily_report.pdf").write_bytes(b"%PDF-1.4")
    return json_path


def test_load_report_summary_extracts_sections(config: Config) -> None:
    _write_report(config, REPORT_PAYLOAD)

    summary = load_report_summary(config, "2024-05-02")

    assert summary.as_of.isoformat() == "2024-05-02"
    assert summary.market_state == "RISK_OFF"
    assert summary.base_currency == "USD"
    assert summary.html_path is not None
    assert summary.pdf_path is not None
    assert len(summary.alerts) == 1
    alert = summary.alerts[0]
    assert alert.symbol == "AAPL"
    assert pytest.approx(alert.value or 0.0, rel=1e-6) == -0.085
    assert summary.orders[0].symbol == "MSFT"
    assert summary.exits == ("TSLA",)
    assert summary.actions_status == "READY"
    assert pytest.approx(summary.turnover or 0.0, rel=1e-6) == 0.12


def test_email_channel_dry_run_returns_message(config: Config) -> None:
    _write_report(config, REPORT_PAYLOAD)
    summary = load_report_summary(config, "2024-05-02")

    channel = EmailChannel()
    status = channel.send(summary, "ops@example.com", dry_run=True)

    assert status.channel == "email"
    assert status.delivered is True
    assert status.details is not None
    assert "RISK_OFF" in status.details
    assert "daily_report.html" in status.details


def test_email_channel_sends_via_smtp(
    monkeypatch: pytest.MonkeyPatch, config: Config
) -> None:
    _write_report(config, REPORT_PAYLOAD)
    summary = load_report_summary(config, "2024-05-02")

    class DummySMTP:
        def __init__(self, host: str, port: int) -> None:
            self.host = host
            self.port = port
            self.messages: list[EmailMessage] = []
            self.tls_started = False
            self.logged_in: tuple[str, str] | None = None

        def __enter__(self) -> DummySMTP:
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> None:
            return None

        def ehlo(self) -> None:
            return None

        def starttls(self) -> None:
            self.tls_started = True

        def login(self, username: str, password: str) -> None:
            self.logged_in = (username, password)

        def send_message(self, message: EmailMessage) -> None:
            self.messages.append(message)

    smtp_instances: list[DummySMTP] = []

    def factory(host: str, port: int) -> DummySMTP:
        instance = DummySMTP(host, port)
        smtp_instances.append(instance)
        return instance

    monkeypatch.setenv("TS_EMAIL_SENDER", "alerts@example.com")
    monkeypatch.setenv("TS_SMTP_HOST", "smtp.example.com")
    monkeypatch.setenv("TS_SMTP_PORT", "2525")
    monkeypatch.setenv("TS_SMTP_USERNAME", "user")
    monkeypatch.setenv("TS_SMTP_PASSWORD", "secret")
    monkeypatch.setenv("TS_SMTP_STARTTLS", "true")

    channel = EmailChannel(smtp_factory=cast(Callable[[str, int], SMTP], factory))
    status = channel.send(summary, "ops@example.com", dry_run=False)

    assert status.delivered is True
    assert smtp_instances
    smtp = smtp_instances[0]
    assert smtp.host == "smtp.example.com"
    assert smtp.port == 2525
    assert smtp.tls_started is True
    assert smtp.logged_in == ("user", "secret")
    assert smtp.messages and "RISK_OFF" in smtp.messages[0].get_content()


def test_slack_channel_payload_contains_sections(config: Config) -> None:
    _write_report(config, REPORT_PAYLOAD)
    summary = load_report_summary(config, "2024-05-02")

    channel = SlackChannel()
    payload = channel.build_payload(summary)

    assert "blocks" in payload
    first_block = payload["blocks"][0]
    assert "2024-05-02" in first_block["text"]["text"]
    assert any(
        "MSFT" in block["text"]["text"]
        for block in payload["blocks"]
        if block["type"] == "section"
    )


def test_slack_channel_send_raises_on_error(config: Config) -> None:
    _write_report(config, REPORT_PAYLOAD)
    summary = load_report_summary(config, "2024-05-02")

    def poster(_: str, __: Mapping[str, Any]) -> Response:
        return cast(Response, SimpleNamespace(status_code=400))

    channel = SlackChannel(http_post=poster)

    with pytest.raises(NotificationError):
        channel.send(summary, "https://hooks.slack.test/ABC", dry_run=False)


def test_notification_service_reports_missing_channels(config: Config) -> None:
    _write_report(config, REPORT_PAYLOAD)
    summary = load_report_summary(config, "2024-05-02")

    config.notify.slack_webhook = None

    service = NotificationService()
    statuses = service.dispatch(summary, config, ["slack", "email"], dry_run=True)

    lookup = {status.channel: status for status in statuses}
    assert lookup["email"].delivered is True
    assert lookup["slack"].delivered is False
    assert "Slack webhook" in (lookup["slack"].details or "")
