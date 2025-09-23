# Copyright (c) 2025 Arye Kogan
# SPDX-License-Identifier: MIT

"""Notification delivery utilities for daily reports."""

from __future__ import annotations

import json
import logging
import os
import smtplib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests import Response

from trading_system.config import Config

logger = logging.getLogger(__name__)


class NotificationError(RuntimeError):
    """Raised when a notification transport cannot complete."""


@dataclass(slots=True)
class AlertSummary:
    """Reduced representation of a triggered risk alert."""

    symbol: str
    alert_type: str
    reason: str
    value: float | None
    threshold: float | None


@dataclass(slots=True)
class OrderSummary:
    """Description of a proposed trade action."""

    symbol: str
    side: str
    quantity: float | None
    notional: float | None


@dataclass(slots=True)
class ReportSummary:
    """Key facts extracted from a persisted daily report."""

    as_of: date
    generated_at: datetime
    base_currency: str
    market_state: str
    alerts: tuple[AlertSummary, ...]
    orders: tuple[OrderSummary, ...]
    exits: tuple[str, ...]
    actions_status: str
    turnover: float | None
    notes: tuple[str, ...]
    report_dir: Path
    json_path: Path
    html_path: Path | None
    pdf_path: Path | None

    @property
    def has_alerts(self) -> bool:
        return bool(self.alerts)

    @property
    def has_orders(self) -> bool:
        return bool(self.orders)


@dataclass(slots=True)
class NotificationStatus:
    """Result of attempting to deliver a notification."""

    channel: str
    delivered: bool
    details: str | None = None


def _normalize_date(value: date | str) -> date:
    if isinstance(value, date):
        return value
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert(None)
    return timestamp.date()


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def _safe_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    raise ValueError(f"Unable to parse datetime value: {value!r}")


def load_report_summary(config: Config, as_of: date | str) -> ReportSummary:
    """Load the persisted report payload for ``as_of`` and summarise it."""

    as_of_date = _normalize_date(as_of)
    report_dir = config.paths.reports / as_of_date.isoformat()
    json_path = report_dir / "daily_report.json"
    if not json_path.is_file():
        raise FileNotFoundError(f"Report JSON not found: {json_path}")

    payload = json.loads(json_path.read_text(encoding="utf-8"))

    risk_payload = payload.get("risk") if isinstance(payload, dict) else None
    actions_payload = payload.get("actions") if isinstance(payload, dict) else None

    alerts: list[AlertSummary] = []
    if isinstance(risk_payload, dict):
        for raw in risk_payload.get("alerts", []):
            if not isinstance(raw, dict):
                continue
            alerts.append(
                AlertSummary(
                    symbol=str(raw.get("symbol", "")),
                    alert_type=str(raw.get("type", "")),
                    reason=str(raw.get("reason", "")),
                    value=_safe_float(raw.get("value")),
                    threshold=_safe_float(raw.get("threshold")),
                )
            )
    alerts.sort(key=lambda item: (item.symbol, item.alert_type))

    orders: list[OrderSummary] = []
    exits: list[str] = []
    actions_status = "UNKNOWN"
    turnover: float | None = None
    if isinstance(actions_payload, dict):
        for raw in actions_payload.get("orders", []):
            if not isinstance(raw, dict):
                continue
            orders.append(
                OrderSummary(
                    symbol=str(raw.get("symbol", "")),
                    side=str(raw.get("side", "")),
                    quantity=_safe_float(raw.get("quantity")),
                    notional=_safe_float(raw.get("notional")),
                )
            )
        orders.sort(key=lambda item: item.symbol)
        exits = [
            str(symbol)
            for symbol in actions_payload.get("exits", [])
            if isinstance(symbol, str)
        ]
        exits.sort()
        actions_status = str(actions_payload.get("status", "UNKNOWN"))
        turnover = _safe_float(actions_payload.get("turnover"))

    generated_raw = payload.get("generated_at")
    generated_at = _safe_datetime(generated_raw) if generated_raw else datetime.now(UTC)

    base_currency = str(payload.get("base_currency", config.base_ccy))
    market_state = "UNKNOWN"
    if isinstance(risk_payload, dict):
        market_state = str(risk_payload.get("market_state", "UNKNOWN")) or "UNKNOWN"

    notes_tuple = tuple(
        str(note) for note in payload.get("notes", []) if isinstance(note, str)
    )

    html_candidate = report_dir / "daily_report.html"
    html_path: Path | None = html_candidate if html_candidate.is_file() else None

    pdf_candidate = report_dir / "daily_report.pdf"
    pdf_path: Path | None = pdf_candidate if pdf_candidate.is_file() else None

    return ReportSummary(
        as_of=as_of_date,
        generated_at=generated_at,
        base_currency=base_currency,
        market_state=market_state or "UNKNOWN",
        alerts=tuple(alerts),
        orders=tuple(orders),
        exits=tuple(exits),
        actions_status=actions_status,
        turnover=turnover,
        notes=notes_tuple,
        report_dir=report_dir,
        json_path=json_path,
        html_path=html_path,
        pdf_path=pdf_path,
    )


def _bool_env(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off"}:
        return False
    return default


SMTPFactory = Callable[[str, int], smtplib.SMTP]


class EmailChannel:
    """SMTP-backed notification channel."""

    def __init__(self, *, smtp_factory: SMTPFactory | None = None) -> None:
        self._smtp_factory = smtp_factory or (
            lambda host, port: smtplib.SMTP(host, port, timeout=10)
        )

    def compose_message(
        self, summary: ReportSummary, recipient: str, *, sender: str
    ) -> EmailMessage:
        subject = f"[{summary.as_of.isoformat()}][{summary.market_state}] Daily report summary"
        message = EmailMessage()
        message["Subject"] = subject
        message["To"] = recipient
        message["From"] = sender
        body = self._build_body(summary)
        message.set_content(body)
        return message

    def _build_body(self, summary: ReportSummary) -> str:
        lines = [
            f"Daily report for {summary.as_of.isoformat()} (market state: {summary.market_state}).",
            f"Generated at {summary.generated_at.isoformat()} UTC.",
            "",
        ]

        if summary.alerts:
            lines.append("Risk alerts:")
            for alert in summary.alerts:
                value = f" value={alert.value:.4f}" if alert.value is not None else ""
                threshold = (
                    f" threshold={alert.threshold:.4f}"
                    if alert.threshold is not None
                    else ""
                )
                lines.append(
                    f"- {alert.symbol} [{alert.alert_type}]{value}{threshold}: {alert.reason}"
                )
        else:
            lines.append("No risk alerts triggered.")

        lines.append("")

        if summary.orders:
            lines.append("Proposed orders:")
            for order in summary.orders:
                qty = f"{order.quantity:.2f}" if order.quantity is not None else "?"
                notional = (
                    f" @ {order.notional:.2f} {summary.base_currency}"
                    if order.notional is not None
                    else ""
                )
                lines.append(f"- {order.side} {qty} {order.symbol}{notional}")
        else:
            lines.append("No new orders proposed.")

        if summary.exits:
            lines.append("")
            lines.append("Exit candidates: " + ", ".join(summary.exits))

        lines.append("")
        lines.append(f"Proposal status: {summary.actions_status}")
        if summary.turnover is not None:
            lines.append(f"Turnover: {summary.turnover:.2%}")

        if summary.notes:
            lines.append("")
            lines.append("Notes:")
            for note in summary.notes:
                lines.append(f"- {note}")

        lines.append("")
        lines.append("Artifacts:")
        lines.append(f"- JSON: {summary.json_path}")
        if summary.html_path is not None:
            lines.append(f"- HTML: {summary.html_path}")
        if summary.pdf_path is not None:
            lines.append(f"- PDF: {summary.pdf_path}")

        return "\n".join(lines)

    def send(
        self,
        summary: ReportSummary,
        recipient: str,
        *,
        dry_run: bool = False,
    ) -> NotificationStatus:
        sender = os.environ.get("TS_EMAIL_SENDER")
        if not sender:
            if dry_run:
                sender = "dry-run@localhost"
            else:
                raise NotificationError(
                    "TS_EMAIL_SENDER environment variable not configured."
                )

        message = self.compose_message(summary, recipient, sender=sender)

        if dry_run:
            body = message.get_content()
            header_lines = [
                f"Subject: {message['Subject']}",
                f"To: {message['To']}",
                f"From: {message['From']}",
                "",
            ]
            preview = "\n".join(header_lines) + body
            return NotificationStatus("email", True, preview)

        host = os.environ.get("TS_SMTP_HOST")
        if not host:
            raise NotificationError("TS_SMTP_HOST environment variable not configured.")
        port_text = os.environ.get("TS_SMTP_PORT", "587")
        try:
            port = int(port_text)
        except ValueError as exc:
            raise NotificationError("TS_SMTP_PORT must be an integer.") from exc

        username = os.environ.get("TS_SMTP_USERNAME")
        password = os.environ.get("TS_SMTP_PASSWORD")
        if (username and not password) or (password and not username):
            raise NotificationError(
                "Both TS_SMTP_USERNAME and TS_SMTP_PASSWORD must be set for authentication."
            )

        use_starttls = _bool_env("TS_SMTP_STARTTLS", True)

        try:
            with self._smtp_factory(host, port) as client:
                client.ehlo()
                if use_starttls:
                    client.starttls()
                    client.ehlo()
                if username and password:
                    client.login(username, password)
                client.send_message(message)
        except Exception as exc:  # pragma: no cover - smtplib specific failures
            logger.exception("Failed to send email notification: %s", exc)
            raise NotificationError(str(exc)) from exc

        return NotificationStatus("email", True, None)


HTTPPoster = Callable[[str, dict[str, Any]], Response]


class SlackChannel:
    """Slack webhook notification channel."""

    def __init__(self, *, http_post: HTTPPoster | None = None) -> None:
        self._http_post = http_post or self._default_post

    def build_payload(self, summary: ReportSummary) -> dict[str, Any]:
        header_text = (
            f"Daily Report — {summary.as_of.isoformat()} ({summary.market_state})"
        )
        blocks: list[dict[str, Any]] = [
            {"type": "section", "text": {"type": "mrkdwn", "text": header_text}},
        ]

        if summary.alerts:
            alert_lines = ["*Risk alerts*"]
            for alert in summary.alerts:
                values: list[str] = []
                if alert.value is not None:
                    values.append(f"value={alert.value:.4f}")
                if alert.threshold is not None:
                    values.append(f"threshold={alert.threshold:.4f}")
                details = f" ({', '.join(values)})" if values else ""
                alert_lines.append(
                    f"• `{alert.symbol}` {alert.alert_type}{details}\n{alert.reason}"
                )
            blocks.append(
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "\n".join(alert_lines)},
                }
            )
        else:
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "No risk alerts triggered.",
                    },
                }
            )

        if summary.orders:
            order_lines = ["*Proposed orders*"]
            for order in summary.orders:
                qty = f"{order.quantity:.2f}" if order.quantity is not None else "?"
                notional = (
                    f" ({order.notional:.2f} {summary.base_currency})"
                    if order.notional is not None
                    else ""
                )
                order_lines.append(f"• {order.side} {qty} `{order.symbol}`{notional}")
            blocks.append(
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "\n".join(order_lines)},
                }
            )
        else:
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "No orders proposed.",
                    },
                }
            )

        if summary.exits:
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*Exit candidates:* " + ", ".join(summary.exits),
                    },
                }
            )

        artifact_lines = [f"JSON: {summary.json_path}"]
        if summary.html_path is not None:
            artifact_lines.append(f"HTML: {summary.html_path}")
        if summary.pdf_path is not None:
            artifact_lines.append(f"PDF: {summary.pdf_path}")

        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Artifacts*\n" + "\n".join(artifact_lines),
                },
            }
        )

        if summary.notes:
            note_lines = ["*Notes*"]
            note_lines.extend(f"• {note}" for note in summary.notes)
            blocks.append(
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "\n".join(note_lines)},
                }
            )

        return {"blocks": blocks}

    def send(
        self,
        summary: ReportSummary,
        webhook: str,
        *,
        dry_run: bool = False,
    ) -> NotificationStatus:
        payload = self.build_payload(summary)
        if dry_run:
            pretty = json.dumps(payload, indent=2, sort_keys=True)
            return NotificationStatus("slack", True, pretty)

        try:
            response = self._http_post(webhook, payload)
        except requests.RequestException as exc:  # pragma: no cover - network failure
            logger.exception("Slack webhook request failed: %s", exc)
            raise NotificationError(str(exc)) from exc

        if response.status_code >= 300:
            raise NotificationError(
                f"Slack webhook returned unexpected status: {response.status_code}"
            )

        return NotificationStatus("slack", True, None)

    @staticmethod
    def _default_post(url: str, payload: dict[str, Any]) -> Response:
        return requests.post(url, json=payload, timeout=10)


def _normalize_channels(channels: Sequence[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    for channel in channels:
        lowered = channel.lower()
        if lowered == "all":
            normalized.extend(["email", "slack"])
        elif lowered in {"email", "slack"}:
            normalized.append(lowered)
    seen: set[str] = set()
    ordered: list[str] = []
    for item in normalized:
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return tuple(ordered)


class NotificationService:
    """Coordinate notification delivery across channels."""

    def __init__(
        self,
        *,
        email_channel: EmailChannel | None = None,
        slack_channel: SlackChannel | None = None,
    ) -> None:
        self._email_channel = email_channel or EmailChannel()
        self._slack_channel = slack_channel or SlackChannel()

    def dispatch(
        self,
        summary: ReportSummary,
        config: Config,
        channels: Sequence[str],
        *,
        dry_run: bool = False,
    ) -> tuple[NotificationStatus, ...]:
        requested = _normalize_channels(channels)
        if not requested:
            return (NotificationStatus("none", False, "No channels requested"),)

        statuses: list[NotificationStatus] = []
        for channel in requested:
            if channel == "email":
                recipient = config.notify.email
                if not recipient:
                    statuses.append(
                        NotificationStatus(
                            "email",
                            False,
                            "Email recipient not configured in notify.email",
                        )
                    )
                    continue
                try:
                    status = self._email_channel.send(
                        summary, recipient, dry_run=dry_run
                    )
                except NotificationError as exc:
                    statuses.append(NotificationStatus("email", False, str(exc)))
                else:
                    statuses.append(status)
            elif channel == "slack":
                webhook = config.notify.slack_webhook
                if not webhook:
                    statuses.append(
                        NotificationStatus(
                            "slack",
                            False,
                            "Slack webhook not configured in notify.slack_webhook",
                        )
                    )
                    continue
                try:
                    status = self._slack_channel.send(summary, webhook, dry_run=dry_run)
                except NotificationError as exc:
                    statuses.append(NotificationStatus("slack", False, str(exc)))
                else:
                    statuses.append(status)
        return tuple(statuses)


__all__ = [
    "AlertSummary",
    "EmailChannel",
    "NotificationError",
    "NotificationService",
    "NotificationStatus",
    "OrderSummary",
    "ReportSummary",
    "SlackChannel",
    "load_report_summary",
]
