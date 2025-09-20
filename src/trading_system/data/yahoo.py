"""Yahoo Finance adapter implementing :class:`DataProvider`."""

from __future__ import annotations

import io
import logging
from collections.abc import Sequence
from datetime import date, datetime, timedelta
from typing import Final, cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

from trading_system.data.provider import (
    BARS_COLUMN_ORDER,
    DataProvider,
    DataUnavailableError,
    empty_bars_frame,
    ensure_bars_frame,
)

LOGGER = logging.getLogger(__name__)

_BASE_URL: Final[str] = "https://query1.finance.yahoo.com/v7/finance/download"
_DEFAULT_USER_AGENT: Final[str] = "trading-system/1.0"


class YahooDataProvider(DataProvider):
    """Fetch daily bars from Yahoo Finance CSV endpoint."""

    def __init__(
        self,
        *,
        user_agent: str = _DEFAULT_USER_AGENT,
        timeout: float = 10.0,
    ) -> None:
        self._user_agent = user_agent
        self._timeout = timeout

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
        csv_data = self._download_csv(symbol, start=start, end=end)
        if not csv_data.strip():
            raise DataUnavailableError(symbol)

        buffer = io.StringIO(csv_data)
        frame = pd.read_csv(buffer)

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

    def _download_csv(
        self,
        symbol: str,
        *,
        start: date | datetime,
        end: date | datetime,
    ) -> str:
        query = _build_query(start=start, end=end)
        url = f"{_BASE_URL}/{symbol}?{urlencode(query)}"
        request = Request(url, headers={"User-Agent": self._user_agent})

        try:
            with urlopen(request, timeout=self._timeout) as response:  # noqa: S310
                payload = cast(bytes, response.read())
        except (HTTPError, URLError) as error:
            raise DataUnavailableError(symbol, message=str(error)) from error

        return payload.decode("utf-8")


def _to_datetime(value: date | datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.combine(value, datetime.min.time())


def _build_query(*, start: date | datetime, end: date | datetime) -> dict[str, str]:
    start_dt = _to_datetime(start)
    end_dt = _to_datetime(end) + timedelta(days=1)

    period1 = int(start_dt.timestamp())
    period2 = int(end_dt.timestamp())

    return {
        "period1": str(period1),
        "period2": str(period2),
        "interval": "1d",
        "events": "history",
        "includeAdjustedClose": "true",
    }


__all__ = ["YahooDataProvider"]
