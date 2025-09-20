"""Interfaces and helpers for raw market data providers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import ClassVar, Final

import pandas as pd

BARS_COLUMN_ORDER: Final[tuple[str, ...]] = (
    "date",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
)


def _column_dtype(column: str) -> pd.api.extensions.ExtensionDtype | str:
    """Return the canonical dtype for ``column``."""

    if column == "date":
        return "datetime64[ns]"
    if column == "symbol":
        return pd.StringDtype()
    if column == "volume":
        return "Int64"
    return "float64"


def empty_bars_frame() -> pd.DataFrame:
    """Return an empty bars frame with canonical schema."""

    return pd.DataFrame(
        {col: pd.Series(dtype=_column_dtype(col)) for col in BARS_COLUMN_ORDER}
    )


def ensure_bars_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate and normalize a raw bars frame."""

    missing = [column for column in BARS_COLUMN_ORDER if column not in frame.columns]
    if missing:
        raise ValueError(f"Frame missing required columns: {missing}")

    result = frame.copy(deep=True)
    result = result.loc[:, list(BARS_COLUMN_ORDER)]

    dates = pd.to_datetime(result["date"], utc=False, errors="coerce")
    if dates.isna().any():
        raise ValueError("Invalid dates encountered in bars frame.")
    result.loc[:, "date"] = dates.dt.tz_localize(None)

    for column in ("open", "high", "low", "close", "adj_close"):
        numeric = pd.to_numeric(result[column], errors="coerce")
        if numeric.isna().any():
            raise ValueError(f"Invalid numeric values for column '{column}'.")
        result.loc[:, column] = numeric.astype("float64")

    volumes = pd.to_numeric(result["volume"], errors="coerce")
    if volumes.isna().any():
        raise ValueError("Invalid numeric values for column 'volume'.")
    result.loc[:, "volume"] = volumes.astype("Int64")

    result.loc[:, "symbol"] = result["symbol"].astype(pd.StringDtype())

    result.sort_values(["symbol", "date"], inplace=True)
    result.reset_index(drop=True, inplace=True)

    return result


class DataUnavailableError(RuntimeError):
    """Raised when a provider cannot return bars for a symbol."""

    def __init__(self, symbol: str, message: str | None = None) -> None:
        self.symbol = symbol
        final_message = message or f"Data for symbol '{symbol}' is unavailable."
        super().__init__(final_message)


@dataclass(frozen=True, slots=True)
class ProviderRequest:
    """Represents a data request issued to a provider."""

    universe: tuple[str, ...]
    start: date | datetime
    end: date | datetime


class DataProvider(ABC):
    """Abstract interface for raw market data providers."""

    required_columns: ClassVar[tuple[str, ...]] = BARS_COLUMN_ORDER

    @abstractmethod
    def get_bars(
        self,
        universe: Sequence[str],
        start: date | datetime,
        end: date | datetime,
    ) -> pd.DataFrame:
        """Return OHLCV bars for the requested ``universe``."""

    def get_benchmark(
        self,
        symbol: str,
        start: date | datetime,
        end: date | datetime,
    ) -> pd.DataFrame:
        """Return benchmark series for ``symbol``.

        Providers may raise ``NotImplementedError`` if benchmark retrieval is
        not supported.
        """

        raise NotImplementedError


__all__ = [
    "BARS_COLUMN_ORDER",
    "DataProvider",
    "DataUnavailableError",
    "ProviderRequest",
    "ensure_bars_frame",
    "empty_bars_frame",
]
