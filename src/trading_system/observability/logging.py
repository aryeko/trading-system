"""Structured logging utilities for trading-system pipelines."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

_LOG_DEFAULT_FIELDS: tuple[str, ...] = (
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
)


class StructuredJsonFormatter(logging.Formatter):
    """Serialize log records as JSON payloads suitable for ingestion."""

    def format(self, record: logging.LogRecord) -> str:  # noqa: D401 - override
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        extras = {
            key: value
            for key, value in record.__dict__.items()
            if key not in _LOG_DEFAULT_FIELDS and not key.startswith("_")
        }
        if extras:
            payload.update(extras)

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        if record.stack_info:
            payload["stack_info"] = self.formatStack(record.stack_info)

        return json.dumps(payload, sort_keys=True)


class StructuredLoggerAdapter(logging.LoggerAdapter[logging.Logger]):
    """Merge contextual metadata into structured log records."""

    def process(
        self, msg: str, kwargs: Mapping[str, Any] | None
    ) -> tuple[str, dict[str, Any]]:
        extra = dict(self.extra) if self.extra else {}
        provided = dict((kwargs or {}).get("extra") or {})
        extra.update(provided)
        payload = dict(kwargs or {})
        if extra:
            payload["extra"] = extra
        return msg, payload


__all__ = ["StructuredJsonFormatter", "StructuredLoggerAdapter"]
