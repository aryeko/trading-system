# Copyright (c) 2025 Arye Kogan
# SPDX-License-Identifier: MIT

"""Reusable expression evaluator for declarative trading rules."""

from __future__ import annotations

import ast
from collections.abc import Mapping
from typing import Any

import pandas as pd


class RuleEvaluator:
    """Evaluate declarative expressions against pandas DataFrames."""

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

    @property
    def expression(self) -> str:
        """Return the original expression string."""

        return self._expression

    def evaluate(self, frame: pd.DataFrame) -> pd.Series:
        """Return a boolean series evaluating ``expression`` on ``frame``."""

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


__all__ = ["RuleEvaluator"]
