"""Strategy engine for evaluating entry/exit rules and rankings."""

from __future__ import annotations

import ast
import logging
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from trading_system.config import Config

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class SymbolEvaluation:
    """Snapshot of rule evaluations and feature values for a symbol."""

    symbol: str
    signal: str
    entry_rule: bool
    exit_rule: bool
    rank_score: float
    features: Mapping[str, float]
    indicators: Mapping[str, float]


@dataclass(slots=True)
class StrategyResult:
    """Result produced by the strategy engine."""

    as_of: date
    frame: pd.DataFrame
    evaluations: dict[str, SymbolEvaluation]
    entry_count: int
    exit_count: int
    output_path: Path | None = None


class _RuleEvaluator:
    """Evaluate declarative rule expressions against pandas frames."""

    _BOOL_OPS: Mapping[type[ast.boolop], str] = {
        ast.And: "&",
        ast.Or: "|",
    }

    _BIN_OPS: Mapping[type[ast.operator], str] = {
        ast.Add: "+",
        ast.Sub: "-",
        ast.Mult: "*",
        ast.Div: "/",
        ast.Mod: "%",
        ast.Pow: "**",
    }

    _CMP_OPS: Mapping[type[ast.cmpop], str] = {
        ast.Eq: "==",
        ast.NotEq: "!=",
        ast.Lt: "<",
        ast.LtE: "<=",
        ast.Gt: ">",
        ast.GtE: ">=",
    }

    def __init__(self, expression: str) -> None:
        expression = expression.strip()
        if not expression:
            raise ValueError("Rule expression cannot be empty.")
        self._expression = expression
        try:
            tree = ast.parse(expression, mode="eval")
        except SyntaxError as exc:  # pragma: no cover - defensive
            raise ValueError(f"Invalid rule expression: {expression!r}") from exc
        self._tree = tree
        self._validate(tree.body)

    def evaluate(self, frame: pd.DataFrame) -> pd.Series:
        """Return a boolean series for ``expression`` evaluated on ``frame``."""

        if frame.empty:
            return pd.Series(dtype="bool")
        index = frame.index
        context: dict[str, Any] = {column: frame[column] for column in frame.columns}
        result = self._eval_node(self._tree.body, context, index)
        series = _ensure_series(result, index)
        return series.astype(bool)

    def _eval_node(
        self, node: ast.AST, context: Mapping[str, Any], index: pd.Index
    ) -> Any:
        if isinstance(node, ast.BoolOp):
            op_symbol = self._BOOL_OPS[type(node.op)]
            result = _ensure_series(
                self._eval_node(node.values[0], context, index), index
            )
            for value_node in node.values[1:]:
                operand = _ensure_series(
                    self._eval_node(value_node, context, index), index
                )
                if op_symbol == "&":
                    result = result & operand
                else:
                    result = result | operand
            return result
        if isinstance(node, ast.BinOp):
            left = _ensure_series(self._eval_node(node.left, context, index), index)
            right = _ensure_series(self._eval_node(node.right, context, index), index)
            operator_symbol = self._BIN_OPS.get(type(node.op))
            if operator_symbol is None:
                raise ValueError(
                    f"Unsupported operator in expression: {ast.dump(node.op)}"
                )
            return _apply_operator(operator_symbol, left, right)
        if isinstance(node, ast.UnaryOp):
            operand = _ensure_series(
                self._eval_node(node.operand, context, index), index
            )
            if isinstance(node.op, ast.UAdd):
                return operand
            if isinstance(node.op, ast.USub):
                return -operand
            if isinstance(node.op, ast.Not):
                return ~operand.astype(bool)
            raise ValueError(f"Unsupported unary operator: {ast.dump(node.op)}")
        if isinstance(node, ast.Compare):
            left = _ensure_series(self._eval_node(node.left, context, index), index)
            result = pd.Series(True, index=index)
            for op, comparator in zip(node.ops, node.comparators, strict=False):
                operator_symbol = self._CMP_OPS.get(type(op))
                if operator_symbol is None:
                    raise ValueError(f"Unsupported comparator: {ast.dump(op)}")
                right = _ensure_series(
                    self._eval_node(comparator, context, index), index
                )
                comparison = _apply_operator(operator_symbol, left, right)
                result = result & comparison
                left = right
            return result
        if isinstance(node, ast.Name):
            if node.id not in context:
                raise ValueError(f"Unknown identifier in expression: {node.id}")
            return context[node.id]
        if isinstance(node, ast.Constant):
            return node.value
        raise ValueError(f"Unsupported expression segment: {ast.dump(node)}")

    def _validate(self, node: ast.AST) -> None:
        if isinstance(node, ast.BoolOp):
            if type(node.op) not in self._BOOL_OPS:
                raise ValueError(f"Unsupported boolean operator: {ast.dump(node.op)}")
            for value in node.values:
                self._validate(value)
            return
        if isinstance(node, ast.BinOp):
            if type(node.op) not in self._BIN_OPS:
                raise ValueError(f"Unsupported binary operator: {ast.dump(node.op)}")
            self._validate(node.left)
            self._validate(node.right)
            return
        if isinstance(node, ast.UnaryOp):
            if not isinstance(node.op, ast.UAdd | ast.USub | ast.Not):
                raise ValueError(f"Unsupported unary operator: {ast.dump(node.op)}")
            self._validate(node.operand)
            return
        if isinstance(node, ast.Compare):
            for comparator in node.comparators:
                self._validate(comparator)
            self._validate(node.left)
            return
        if isinstance(node, ast.Name | ast.Constant):
            return
        raise ValueError(f"Unsupported node in expression: {ast.dump(node)}")


def _apply_operator(symbol: str, left: Any, right: Any) -> Any:
    if symbol == "+":
        return left + right
    if symbol == "-":
        return left - right
    if symbol == "*":
        return left * right
    if symbol == "/":
        return left / right
    if symbol == "%":
        return left % right
    if symbol == "**":
        return left**right
    if symbol == "==":
        return left == right
    if symbol == "!=":
        return left != right
    if symbol == "<":
        return left < right
    if symbol == "<=":
        return left <= right
    if symbol == ">":
        return left > right
    if symbol == ">=":
        return left >= right
    raise ValueError(f"Unsupported operator: {symbol}")


def _ensure_series(value: Any, index: pd.Index) -> pd.Series:
    if isinstance(value, pd.Series):
        return value.reindex(index)
    if isinstance(value, pd.Index):
        values = list(value)
    else:
        values = [value] * len(index)
    series: pd.Series[Any] = pd.Series(values, index=index)
    return series


class StrategyEngine:
    """Evaluate configured strategy rules against curated inputs."""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._entry_evaluator = _RuleEvaluator(config.strategy.entry)
        self._exit_evaluator = _RuleEvaluator(config.strategy.exit)
        self._rank_metric = config.strategy.rank or "momentum_63d"
        self._curated_base = config.paths.data_curated
        self._reports_base = config.paths.reports

    def evaluate(
        self, as_of: date | str | pd.Timestamp, *, window: int | None = None
    ) -> StrategyResult:
        """Evaluate strategy rules for ``as_of`` and return in-memory results."""

        as_of_ts = _normalize_timestamp(as_of)
        as_of_date = as_of_ts.date()
        curated_dir = self._curated_base / as_of_ts.strftime("%Y-%m-%d")
        if not curated_dir.is_dir():
            raise FileNotFoundError(f"Curated data directory not found: {curated_dir}")

        window = int(window) if window and window > 0 else None
        records: list[dict[str, Any]] = []
        evaluations: dict[str, SymbolEvaluation] = {}
        entry_count = 0
        exit_count = 0

        for symbol in sorted(self._config.universe.tickers):
            symbol_upper = symbol.upper()
            path = curated_dir / f"{symbol_upper}.parquet"
            if not path.is_file():
                logger.warning(
                    "Curated dataset missing for %s in %s", symbol_upper, curated_dir
                )
                continue

            data = pd.read_parquet(path)
            if data.empty:
                logger.warning("Curated dataset empty for %s", symbol_upper)
                continue

            data["date"] = pd.to_datetime(data["date"], utc=False)
            data = data.sort_values("date")
            if window is not None:
                data = data.tail(window)
            data = data.set_index("date")

            entry_series = self._entry_evaluator.evaluate(data)
            exit_series = self._exit_evaluator.evaluate(data)

            entry_flag = _latest_bool(entry_series)
            exit_flag = _latest_bool(exit_series)

            rank_series = self._compute_rank_series(data)
            rank_score = _latest_rank_value(rank_series)

            features = self._derive_features(data)
            latest = data.iloc[-1]
            indicators = _extract_indicators(latest)

            signal = "EXIT" if exit_flag else ("BUY" if entry_flag else "HOLD")

            record = {
                "date": pd.Timestamp(as_of_date),
                "symbol": symbol_upper,
                "signal": signal,
                "rank_score": rank_score,
            }
            for feature_name, value in features.items():
                record[feature_name] = value

            records.append(record)

            evaluations[symbol_upper] = SymbolEvaluation(
                symbol=symbol_upper,
                signal=signal,
                entry_rule=entry_flag,
                exit_rule=exit_flag,
                rank_score=rank_score,
                features=features,
                indicators=indicators,
            )

            if entry_flag:
                entry_count += 1
            if exit_flag:
                exit_count += 1

        frame = pd.DataFrame(records)
        if not frame.empty:
            feature_columns = [
                column
                for column in frame.columns
                if column not in {"date", "symbol", "signal", "rank_score"}
            ]
            frame = frame.sort_values(["rank_score", "symbol"], ascending=[False, True])
            frame = frame.reset_index(drop=True)
            frame = frame[
                ["date", "symbol", "signal", "rank_score", *sorted(feature_columns)]
            ]

        logger.info(
            "Strategy evaluation for %s processed %d symbols (%d entry, %d exit)",
            as_of_date,
            len(records),
            entry_count,
            exit_count,
        )

        return StrategyResult(
            as_of=as_of_date,
            frame=frame,
            evaluations=evaluations,
            entry_count=entry_count,
            exit_count=exit_count,
        )

    def build(
        self,
        as_of: date | str | pd.Timestamp,
        *,
        window: int | None = None,
        dry_run: bool = False,
    ) -> StrategyResult:
        """Evaluate strategy and persist signals parquet unless ``dry_run``."""

        result = self.evaluate(as_of, window=window)
        if dry_run or result.frame.empty:
            return result

        as_of_str = result.as_of.strftime("%Y-%m-%d")
        output_dir = self._reports_base / as_of_str
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "signals.parquet"
        result.frame.to_parquet(output_path, index=False)
        result.output_path = output_path
        logger.info("Signals written to %s", output_path)
        return result

    def explain(
        self,
        symbol: str,
        as_of: date | str | pd.Timestamp,
        *,
        window: int | None = None,
    ) -> SymbolEvaluation:
        """Return evaluation details for ``symbol`` on ``as_of``."""

        result = self.evaluate(as_of, window=window)
        symbol_upper = symbol.upper()
        evaluation = result.evaluations.get(symbol_upper)
        if evaluation is None:
            raise KeyError(f"Symbol {symbol_upper} not evaluated.")
        return evaluation

    def _compute_rank_series(self, frame: pd.DataFrame) -> pd.Series:
        metric = self._rank_metric
        if metric == "momentum_63d":
            if "close" not in frame.columns:
                raise ValueError("Curated data missing 'close' column.")
            close = pd.to_numeric(frame["close"], errors="coerce")
            return close / close.shift(63) - 1.0
        if metric in frame.columns:
            return pd.to_numeric(frame[metric], errors="coerce")
        raise ValueError(f"Unsupported rank metric: {metric}")

    def _derive_features(self, frame: pd.DataFrame) -> dict[str, float]:
        if "close" not in frame.columns:
            raise ValueError("Curated data missing 'close' column.")
        close = pd.to_numeric(frame["close"], errors="coerce")
        momentum = close / close.shift(63) - 1.0
        return {"momentum_63d": _latest_float(momentum)}


def _latest_bool(series: pd.Series) -> bool:
    if series.empty:
        return False
    value = series.iloc[-1]
    if pd.isna(value):
        return False
    return bool(value)


def _latest_float(series: pd.Series) -> float:
    if series.empty:
        return math.nan
    value = series.iloc[-1]
    if pd.isna(value):
        return math.nan
    return float(value)


def _latest_rank_value(series: pd.Series) -> float:
    value = _latest_float(series)
    if math.isnan(value):
        return float("-inf")
    return value


def _extract_indicators(row: pd.Series) -> dict[str, float]:
    keys = [
        "close",
        "sma_100",
        "sma_200",
        "ret_1d",
        "ret_20d",
        "rolling_peak",
    ]
    indicators: dict[str, float] = {}
    for key in keys:
        if key in row:
            try:
                indicators[key] = float(row[key])
            except (TypeError, ValueError):
                indicators[key] = math.nan
    return indicators


def _normalize_timestamp(value: date | str | pd.Timestamp) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert(None)
    return timestamp.normalize()


__all__ = ["StrategyEngine", "StrategyResult", "SymbolEvaluation"]
