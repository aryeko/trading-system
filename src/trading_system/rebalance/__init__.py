# Copyright (c) 2025 Arye Kogan
# SPDX-License-Identifier: MIT

"""Portfolio rebalancer converting signals into target weights and orders."""

from __future__ import annotations

import json
import logging
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from trading_system.config import Config
from trading_system.risk import HoldingsSnapshot, Position

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class RebalanceTarget:
    """Desired allocation for a given symbol."""

    symbol: str
    target_weight: float
    rationale: str | None = None


@dataclass(slots=True)
class RebalanceOrder:
    """Order intent derived from target weights."""

    symbol: str
    side: str
    quantity: float
    notional: float


@dataclass(slots=True)
class RebalanceResult:
    """Result returned by :class:`RebalanceEngine`."""

    as_of: date
    status: str
    cash_buffer: float
    turnover: float
    targets: tuple[RebalanceTarget, ...]
    orders: tuple[RebalanceOrder, ...]
    notes: tuple[str, ...]
    output_path: Path | None = None


@dataclass(slots=True)
class _Candidate:
    symbol: str
    signal: str
    rank_score: float
    price: float
    rationale: str
    is_existing: bool


class RebalanceEngine:
    """Construct rebalance proposals based on signals and holdings."""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._curated_base = config.paths.data_curated
        self._reports_base = config.paths.reports
        cadence_raw = (config.rebalance.cadence or "").strip().lower()
        if not cadence_raw:
            raise ValueError("Rebalance cadence must be configured")
        self._cadence = cadence_raw
        self._max_positions = int(config.rebalance.max_positions)
        self._equal_weight = (
            True
            if config.rebalance.equal_weight is None
            else bool(config.rebalance.equal_weight)
        )
        self._min_weight = float(config.rebalance.min_weight or 0.0)
        self._cash_buffer = float(config.rebalance.cash_buffer or 0.0)
        self._turnover_cap = (
            float(config.rebalance.turnover_cap_pct)
            if config.rebalance.turnover_cap_pct is not None
            else None
        )

    def evaluate(
        self,
        as_of: date | str | pd.Timestamp,
        *,
        holdings: HoldingsSnapshot,
        signals: pd.DataFrame,
        force: bool = False,
    ) -> RebalanceResult:
        """Evaluate rebalance logic for ``as_of`` without writing artifacts."""

        as_of_ts = _normalize_timestamp(as_of)
        as_of_date = as_of_ts.date()
        notes: list[str] = []

        if not force and not _is_rebalance_day(as_of_ts, self._cadence):
            notes.append(f"Cadence {self._cadence} not met on {as_of_date}")
            return RebalanceResult(
                as_of=as_of_date,
                status="NO_REBALANCE",
                cash_buffer=self._cash_buffer,
                turnover=0.0,
                targets=(),
                orders=(),
                notes=tuple(notes),
            )

        curated_dir = self._curated_base / as_of_ts.strftime("%Y-%m-%d")
        if not curated_dir.is_dir():
            raise FileNotFoundError(f"Curated data directory not found: {curated_dir}")

        frame = _prepare_signals(signals, as_of_ts)
        if frame.empty:
            notes.append("No signals available for rebalance date")
            return RebalanceResult(
                as_of=as_of_date,
                status="NO_CANDIDATES",
                cash_buffer=self._cash_buffer,
                turnover=0.0,
                targets=(),
                orders=(),
                notes=tuple(notes),
            )

        current_positions: dict[str, Position] = {
            position.symbol: position for position in holdings.positions
        }

        price_map = _load_price_map(
            curated_dir,
            symbols=sorted(set(frame["symbol"].unique()) | set(current_positions)),
            as_of=as_of_ts,
        )

        candidates = _collect_candidates(
            frame, current_positions, price_map, equal_weight=self._equal_weight
        )
        exit_symbols = _collect_exit_symbols(frame, current_positions)

        available_weight = max(0.0, 1.0 - self._cash_buffer)
        max_allowed = _max_positions_by_min_weight(
            available_weight, self._min_weight, self._max_positions
        )
        if max_allowed == 0:
            notes.append(
                "Cash buffer and min_weight configuration leave no capacity for targets"
            )
            orders = _exit_orders(exit_symbols, current_positions, price_map)
            return RebalanceResult(
                as_of=as_of_date,
                status="NO_CAPACITY",
                cash_buffer=self._cash_buffer,
                turnover=0.0,
                targets=(),
                orders=orders,
                notes=tuple(notes),
            )

        selected = candidates[:max_allowed]
        selected = _enforce_min_weight(
            selected, available_weight, self._min_weight, notes
        )

        proposal = _build_proposal(
            selected=selected,
            exit_symbols=exit_symbols,
            current_positions=current_positions,
            holdings_cash=holdings.cash or 0.0,
            price_map=price_map,
            available_weight=available_weight,
            equal_weight=self._equal_weight,
        )

        if self._turnover_cap is not None and proposal.turnover > self._turnover_cap:
            proposal = _reduce_turnover(
                selected,
                exit_symbols,
                current_positions,
                holdings.cash or 0.0,
                price_map,
                available_weight,
                equal_weight=self._equal_weight,
                cap=self._turnover_cap,
                notes=notes,
            )

        combined_notes = tuple(notes + list(proposal.notes))
        return RebalanceResult(
            as_of=as_of_date,
            status=proposal.status,
            cash_buffer=self._cash_buffer,
            turnover=proposal.turnover,
            targets=tuple(proposal.targets),
            orders=tuple(proposal.orders),
            notes=combined_notes,
        )

    def build(
        self,
        as_of: date | str | pd.Timestamp,
        *,
        holdings: HoldingsSnapshot,
        signals: pd.DataFrame,
        dry_run: bool = False,
        force: bool = False,
    ) -> RebalanceResult:
        """Evaluate rebalance and persist proposal unless ``dry_run``."""

        result = self.evaluate(as_of, holdings=holdings, signals=signals, force=force)
        if dry_run:
            return result

        output_dir = self._reports_base / result.as_of.strftime("%Y-%m-%d")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "rebalance_proposal.json"
        payload = _serialize_result(result)
        output_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
        )
        result.output_path = output_path
        logger.info("Rebalance proposal written to %s", output_path)
        return result


@dataclass(slots=True)
class _Proposal:
    status: str
    targets: list[RebalanceTarget]
    orders: list[RebalanceOrder]
    turnover: float
    notes: list[str]


def _normalize_timestamp(value: date | str | pd.Timestamp) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert(None)
    return timestamp.normalize()


def _is_rebalance_day(as_of: pd.Timestamp, cadence: str) -> bool:
    if cadence == "monthly":
        return as_of == (as_of + pd.offsets.BMonthEnd(0))
    if cadence == "weekly":
        return as_of.weekday() == 4  # Friday
    raise ValueError(f"Unsupported rebalance cadence: {cadence}")


def _prepare_signals(frame: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
    if "symbol" not in frame.columns or "signal" not in frame.columns:
        raise ValueError("Signals frame must contain 'symbol' and 'signal' columns")
    working = frame.copy()
    if "date" in working.columns:
        working["date"] = pd.to_datetime(working["date"]).dt.normalize()
        working = working[working["date"] == as_of]
    working["symbol"] = working["symbol"].astype(str).str.upper()
    if "rank_score" not in working.columns:
        working["rank_score"] = 0.0
    working = working.sort_values(["rank_score", "symbol"], ascending=[False, True])
    working = working.reset_index(drop=True)
    return working


def _load_price_map(
    curated_dir: Path, symbols: Sequence[str], as_of: pd.Timestamp
) -> dict[str, float]:
    prices: dict[str, float] = {}
    for symbol in symbols:
        prices[symbol] = _load_price(curated_dir, symbol, as_of)
    return prices


def _load_price(curated_dir: Path, symbol: str, as_of: pd.Timestamp) -> float:
    path = curated_dir / f"{symbol}.parquet"
    if not path.is_file():
        raise FileNotFoundError(
            f"Curated dataset missing for {symbol} in {curated_dir}"
        )
    data = pd.read_parquet(path)
    if data.empty:
        raise ValueError(f"Curated dataset for {symbol} is empty")
    data["date"] = pd.to_datetime(data["date"]).dt.normalize()
    data = data[data["date"] <= as_of]
    if data.empty:
        raise ValueError(f"No data for {symbol} on or before {as_of.date()}")
    latest = data.iloc[-1]
    price = float(latest.get("close", math.nan))
    if math.isnan(price) or price <= 0:
        raise ValueError(f"Invalid close price for {symbol} on {as_of.date()}")
    return price


def _collect_candidates(
    frame: pd.DataFrame,
    current_positions: Mapping[str, Position],
    price_map: Mapping[str, float],
    *,
    equal_weight: bool,
) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    current_symbols = set(current_positions)
    for _, row in frame.iterrows():
        symbol = str(row["symbol"]).upper()
        signal = str(row["signal"]).upper()
        if signal == "EXIT":
            continue
        price = price_map.get(symbol)
        if price is None:
            continue
        rank_score = float(row.get("rank_score", 0.0))
        rationale = "BUY signal" if signal == "BUY" else "Maintain position"
        if not equal_weight and rank_score <= 0 and symbol not in current_symbols:
            # avoid allocating new positions with non-positive scores
            continue
        candidates.append(
            _Candidate(
                symbol=symbol,
                signal=signal,
                rank_score=rank_score,
                price=price,
                rationale=rationale,
                is_existing=symbol in current_symbols,
            )
        )
    candidates.sort(key=lambda item: (-item.rank_score, item.symbol))
    return candidates


def _collect_exit_symbols(
    frame: pd.DataFrame, current_positions: Mapping[str, Position]
) -> set[str]:
    exits: set[str] = set()
    current_symbols = set(current_positions)
    for _, row in frame.iterrows():
        symbol = str(row["symbol"]).upper()
        signal = str(row["signal"]).upper()
        if signal == "EXIT" and symbol in current_symbols:
            exits.add(symbol)
    return exits


def _max_positions_by_min_weight(
    available_weight: float, min_weight: float, max_positions: int
) -> int:
    if max_positions <= 0:
        return 0
    if min_weight <= 0:
        return max_positions
    allowed = int(math.floor((available_weight + 1e-9) / min_weight))
    return max(0, min(max_positions, allowed))


def _enforce_min_weight(
    selected: list[_Candidate],
    available_weight: float,
    min_weight: float,
    notes: list[str],
) -> list[_Candidate]:
    if not selected or min_weight <= 0:
        return selected
    while selected:
        per_weight = available_weight / len(selected)
        if per_weight + 1e-9 >= min_weight:
            break
        removed = selected.pop()
        notes.append(f"Removed {removed.symbol} to satisfy min_weight={min_weight:.4f}")
    return selected


def _build_proposal(
    *,
    selected: Sequence[_Candidate],
    exit_symbols: set[str],
    current_positions: Mapping[str, Position],
    holdings_cash: float,
    price_map: Mapping[str, float],
    available_weight: float,
    equal_weight: bool,
) -> _Proposal:
    targets: list[RebalanceTarget] = []
    notes: list[str] = []

    if selected:
        weights = _compute_weights(selected, available_weight, equal_weight)
        if equal_weight:
            allocation_mode = "equal-weight"
        else:
            allocation_mode = (
                "score-weight"
                if any(weight != weights[0] for weight in weights[1:])
                else "equal-weight"
            )
        for candidate, weight in zip(selected, weights, strict=False):
            targets.append(
                RebalanceTarget(
                    symbol=candidate.symbol,
                    target_weight=weight,
                    rationale=candidate.rationale,
                )
            )
        notes.append(
            f"Selected {len(selected)} symbols with {allocation_mode} allocation"
        )
    else:
        notes.append("No candidates selected for allocation")

    for symbol in sorted(exit_symbols):
        targets.append(
            RebalanceTarget(
                symbol=symbol,
                target_weight=0.0,
                rationale="Exit signal triggered",
            )
        )

    targets.sort(key=lambda item: (-item.target_weight, item.symbol))

    orders, turnover = _orders_and_turnover(
        current_positions=current_positions,
        holdings_cash=holdings_cash,
        price_map=price_map,
        targets=targets,
    )

    status = "REBALANCE" if targets or orders else "NO_CANDIDATES"
    return _Proposal(
        status=status, targets=targets, orders=orders, turnover=turnover, notes=notes
    )


def _compute_weights(
    selected: Sequence[_Candidate], available_weight: float, equal_weight: bool
) -> list[float]:
    if not selected:
        return []
    if equal_weight or len(selected) == 1:
        per_weight = available_weight / len(selected)
        return [per_weight for _ in selected]
    scores = [max(candidate.rank_score, 0.0) for candidate in selected]
    total_score = sum(scores)
    if total_score <= 0:
        per_weight = available_weight / len(selected)
        return [per_weight for _ in selected]
    return [available_weight * score / total_score for score in scores]


def _orders_and_turnover(
    *,
    current_positions: Mapping[str, Position],
    holdings_cash: float,
    price_map: Mapping[str, float],
    targets: Sequence[RebalanceTarget],
) -> tuple[list[RebalanceOrder], float]:
    total_value = float(holdings_cash or 0.0)
    current_values: dict[str, float] = {}
    for symbol, position in current_positions.items():
        price = price_map.get(symbol)
        if price is None:
            raise ValueError(f"Missing price for current holding {symbol}")
        current_values[symbol] = position.qty * price
        total_value += current_values[symbol]

    if total_value <= 0:
        raise ValueError("Total portfolio value must be positive")

    target_weights = {target.symbol: target.target_weight for target in targets}
    orders: list[RebalanceOrder] = []
    turnover = 0.0

    symbols = sorted(set(current_positions) | set(target_weights))
    for symbol in symbols:
        price = price_map.get(symbol)
        if price is None:
            continue
        current_position = current_positions.get(symbol)
        current_qty = current_position.qty if current_position is not None else 0.0
        current_weight = current_values.get(symbol, 0.0) / total_value
        target_weight = target_weights.get(symbol, 0.0)
        turnover += abs(target_weight - current_weight)
        target_value = target_weight * total_value
        target_qty = target_value / price if price > 0 else 0.0
        delta_qty = target_qty - current_qty
        if abs(delta_qty) < 1e-6:
            continue
        side = "BUY" if delta_qty > 0 else "SELL"
        orders.append(
            RebalanceOrder(
                symbol=symbol,
                side=side,
                quantity=round(delta_qty, 6),
                notional=round(abs(delta_qty) * price, 2),
            )
        )

    orders.sort(key=lambda order: order.symbol)
    turnover *= 0.5
    return orders, turnover


def _reduce_turnover(
    selected: Sequence[_Candidate],
    exit_symbols: set[str],
    current_positions: Mapping[str, Position],
    holdings_cash: float,
    price_map: Mapping[str, float],
    available_weight: float,
    *,
    equal_weight: bool,
    cap: float,
    notes: list[str],
) -> _Proposal:
    mutable_selected = list(selected)
    # Remove new candidates first to honor the cap.
    for index in range(len(mutable_selected) - 1, -1, -1):
        proposal = _build_proposal(
            selected=mutable_selected,
            exit_symbols=exit_symbols,
            current_positions=current_positions,
            holdings_cash=holdings_cash,
            price_map=price_map,
            available_weight=available_weight,
            equal_weight=equal_weight,
        )
        if proposal.turnover <= cap:
            proposal.notes.append(
                f"Turnover {proposal.turnover:.4f} within cap {cap:.4f}"
            )
            return proposal
        candidate = mutable_selected[index]
        if candidate.is_existing:
            continue
        notes.append(f"Removed {candidate.symbol} to satisfy turnover cap {cap:.4f}")
        mutable_selected.pop(index)

    # Attempt final proposal with remaining candidates (existing only).
    final_proposal = _build_proposal(
        selected=mutable_selected,
        exit_symbols=exit_symbols,
        current_positions=current_positions,
        holdings_cash=holdings_cash,
        price_map=price_map,
        available_weight=available_weight,
        equal_weight=equal_weight,
    )
    if final_proposal.turnover > cap:
        final_proposal.status = "TURNOVER_LIMIT"
        final_proposal.orders.clear()
        final_proposal.targets = []
        final_proposal.turnover = 0.0
        final_proposal.notes.append(f"Turnover cap {cap:.4f} prevented adjustments")
        return final_proposal

    final_proposal.notes.append(
        f"Turnover adjusted to {final_proposal.turnover:.4f} within cap {cap:.4f}"
    )
    return final_proposal


def _exit_orders(
    exit_symbols: set[str],
    current_positions: Mapping[str, Position],
    price_map: Mapping[str, float],
) -> tuple[RebalanceOrder, ...]:
    orders: list[RebalanceOrder] = []
    for symbol in sorted(exit_symbols):
        position = current_positions.get(symbol)
        if position is None or position.qty == 0:
            continue
        price = price_map.get(symbol)
        if price is None:
            continue
        orders.append(
            RebalanceOrder(
                symbol=symbol,
                side="SELL",
                quantity=round(position.qty, 6),
                notional=round(abs(position.qty) * price, 2),
            )
        )
    return tuple(orders)


def _serialize_result(result: RebalanceResult) -> dict[str, object]:
    payload: dict[str, object] = {
        "date": result.as_of.isoformat(),
        "status": result.status,
        "cash_buffer": result.cash_buffer,
        "turnover": result.turnover,
        "targets": [
            {
                "symbol": target.symbol,
                "target_weight": round(target.target_weight, 6),
                "rationale": target.rationale,
            }
            for target in result.targets
        ],
        "orders": [
            {
                "symbol": order.symbol,
                "side": order.side,
                "quantity": order.quantity,
                "notional": order.notional,
            }
            for order in result.orders
        ],
        "notes": list(result.notes),
    }
    if result.output_path is not None:
        payload["output_path"] = str(result.output_path)
    return payload


__all__ = [
    "RebalanceEngine",
    "RebalanceOrder",
    "RebalanceResult",
    "RebalanceTarget",
]
