"""High-level orchestration for raw data pulls."""

from __future__ import annotations

from datetime import date, datetime, timedelta

from trading_system.config import Config
from trading_system.data.provider import DataProvider
from trading_system.data.storage import DataRunMeta, RawDataWriter


def run_data_pull(
    config: Config,
    provider: DataProvider,
    *,
    as_of: date,
    run_at: datetime | None = None,
    include_benchmark: bool = True,
) -> DataRunMeta:
    """Execute a data pull for ``as_of`` using ``provider`` and persist artifacts."""

    lookback_days = config.data.lookback_days or 0
    start_date = as_of - timedelta(days=lookback_days)

    bars = provider.get_bars(config.universe.tickers, start=start_date, end=as_of)

    benchmark_frame = None
    benchmark_symbol: str | None = None
    if include_benchmark and config.risk.market_filter is not None:
        benchmark_symbol = config.risk.market_filter.benchmark
        if benchmark_symbol:
            try:
                benchmark_frame = provider.get_benchmark(
                    benchmark_symbol, start=start_date, end=as_of
                )
            except NotImplementedError:
                benchmark_frame = None
    elif not include_benchmark:
        benchmark_symbol = None

    writer = RawDataWriter(config.paths.data_raw)
    result = writer.persist(
        as_of=as_of,
        bars=bars,
        start=start_date,
        end=as_of,
        benchmark_symbol=benchmark_symbol,
        benchmark_frame=benchmark_frame,
        run_at=run_at,
    )
    return result


__all__ = ["run_data_pull"]
