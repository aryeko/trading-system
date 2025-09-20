"""Yahoo Finance adapter implementing :class:`DataProvider`."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import date, datetime, timedelta
from typing import Final, cast

import pandas as pd
import yfinance as yf

from trading_system.data.provider import (
    BARS_COLUMN_ORDER,
    DataProvider,
    DataUnavailableError,
    empty_bars_frame,
    ensure_bars_frame,
)

LOGGER = logging.getLogger(__name__)

_DEFAULT_INTERVAL: Final[str] = "1d"


class YahooDataProvider(DataProvider):
    """Fetch daily bars from Yahoo Finance via yfinance."""

    def __init__(
        self,
        *,
        interval: str = _DEFAULT_INTERVAL,
    ) -> None:
        self._interval = interval

    def get_bars(
        self,
        universe: Sequence[str],
        start: date | datetime,
        end: date | datetime,
    ) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for symbol in universe:
            try:
                frame = self._fetch_symbol(symbol, start=start, end=end)
            except DataUnavailableError:
                LOGGER.warning("Symbol %s unavailable from Yahoo; skipping.", symbol)
                continue
            frames.append(frame)

        if not frames:
            return empty_bars_frame()

        combined = pd.concat(frames, ignore_index=True)
        return ensure_bars_frame(combined)

    def get_benchmark(
        self,
        symbol: str,
        start: date | datetime,
        end: date | datetime,
    ) -> pd.DataFrame:
        try:
            frame = self._fetch_symbol(symbol, start=start, end=end)
        except DataUnavailableError:
            LOGGER.warning(
                "Benchmark symbol %s unavailable from Yahoo; skipping.", symbol
            )
            return empty_bars_frame()
        return ensure_bars_frame(frame)

    def _fetch_symbol(
        self,
        symbol: str,
        *,
        start: date | datetime,
        end: date | datetime,
    ) -> pd.DataFrame:
        history = self._download_history(symbol, start=start, end=end)
        if history.empty:
            raise DataUnavailableError(symbol)

        frame = history.reset_index(names="Date")

        expected_columns = {
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }

        missing = [column for column in expected_columns if column not in frame.columns]
        if missing:
            raise DataUnavailableError(
                symbol,
                message=f"Missing expected columns from Yahoo response: {missing}",
            )

        frame = frame.loc[:, list(expected_columns.keys())]
        frame = frame.rename(columns=expected_columns)
        frame.loc[:, "symbol"] = symbol
        frame = frame.loc[:, list(BARS_COLUMN_ORDER)]

        return ensure_bars_frame(frame)

    def _download_history(
        self,
        symbol: str,
        *,
        start: date | datetime,
        end: date | datetime,
    ) -> pd.DataFrame:
        yf_start = _to_datetime(start)
        yf_end = _to_datetime(end) + timedelta(days=1)

        try:
            history_raw = yf.download(
                symbol,
                start=yf_start,
                end=yf_end,
                interval=self._interval,
                auto_adjust=False,
                progress=False,
                actions=True,
            )
            history = cast(pd.DataFrame, history_raw)
        except Exception as exc:  # pragma: no cover - network error surface
            raise DataUnavailableError(symbol, message=str(exc)) from exc

        # yfinance may return a Series when a single column is requested; ensure DataFrame.
        if isinstance(history, pd.Series):
            history = history.to_frame().T

        return history


def _to_datetime(value: date | datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.combine(value, datetime.min.time())


__all__ = ["YahooDataProvider"]
