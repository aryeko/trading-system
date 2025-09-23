# Copyright (c) 2025 Arye Kogan
# SPDX-License-Identifier: MIT

"""Persistence utilities for raw market data pulls."""

from __future__ import annotations

import json
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any

import pandas as pd

from trading_system.data.provider import BARS_COLUMN_ORDER, ensure_bars_frame


@dataclass(frozen=True, slots=True)
class DataRunMeta:
    """Metadata describing a persisted data pull."""

    directory: Path
    timestamp: datetime
    symbols: tuple[str, ...]
    last_bar_date: date | None
    start: date | None
    end: date | None
    benchmark: str | None


class RawDataWriter:
    """Persist raw bars and associated metadata."""

    def __init__(self, root: Path) -> None:
        self._root = root

    def persist(
        self,
        *,
        as_of: date,
        bars: pd.DataFrame,
        start: date | None = None,
        end: date | None = None,
        benchmark_symbol: str | None = None,
        benchmark_frame: pd.DataFrame | None = None,
        run_at: datetime | None = None,
    ) -> DataRunMeta:
        normalized_bars = ensure_bars_frame(bars) if not bars.empty else bars
        if benchmark_frame is not None and not benchmark_frame.empty:
            normalized_benchmark = ensure_bars_frame(benchmark_frame)
        else:
            normalized_benchmark = None

        run_directory = self._root / as_of.isoformat()
        run_directory.mkdir(parents=True, exist_ok=True)

        if not normalized_bars.empty:
            for symbol, frame in normalized_bars.groupby("symbol", sort=True):
                output_path = run_directory / f"{symbol}.parquet"
                _write_parquet_atomic(frame, output_path)

        if normalized_benchmark is not None and benchmark_symbol is not None:
            benchmark_path = run_directory / f"benchmark_{benchmark_symbol}.parquet"
            _write_parquet_atomic(normalized_benchmark, benchmark_path)

        run_timestamp = run_at or datetime.combine(as_of, time.min, tzinfo=UTC)

        last_bar = None
        date_series = normalized_bars.get("date")
        if date_series is not None and not normalized_bars.empty:
            last_bar_value = pd.to_datetime(date_series, utc=False).max()
            if pd.notna(last_bar_value):
                last_bar = pd.Timestamp(last_bar_value).date()

        symbol_series = normalized_bars.get("symbol")
        symbols_list = (
            sorted(str(symbol) for symbol in symbol_series.unique())
            if symbol_series is not None
            else []
        )

        meta_payload: dict[str, Any] = {
            "timestamp": run_timestamp.isoformat(),
            "symbols": symbols_list,
            "last_bar_date": last_bar.isoformat() if last_bar else None,
            "start": start.isoformat() if start else None,
            "end": end.isoformat() if end else None,
        }
        if benchmark_symbol is not None:
            meta_payload["benchmark"] = benchmark_symbol

        _write_json_atomic(meta_payload, run_directory / "meta_run.json")

        return DataRunMeta(
            directory=run_directory,
            timestamp=run_timestamp,
            symbols=tuple(meta_payload["symbols"]),
            last_bar_date=last_bar,
            start=start,
            end=end,
            benchmark=benchmark_symbol,
        )


def _write_parquet_atomic(frame: pd.DataFrame, path: Path) -> None:
    def _writer(temp_path: Path) -> None:
        ordered = frame.loc[:, list(BARS_COLUMN_ORDER)]
        ordered.to_parquet(temp_path, index=False)

    _atomic_write(path, _writer)


def _write_json_atomic(payload: dict[str, Any], path: Path) -> None:
    def _writer(temp_path: Path) -> None:
        text = json.dumps(payload, indent=2, sort_keys=True)
        temp_path.write_text(text, encoding="utf-8")

    _atomic_write(path, _writer)


def _atomic_write(path: Path, writer: Callable[[Path], None]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        delete=False, dir=str(path.parent), suffix=".tmp"
    ) as handle:
        temp_path = Path(handle.name)

    try:
        writer(temp_path)
        temp_path.replace(path)
    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)


__all__ = ["DataRunMeta", "RawDataWriter"]
