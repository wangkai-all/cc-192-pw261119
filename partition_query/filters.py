"""项目明确支持的筛选条件模型。

逻辑结构采用 DNF（析取范式）：
- 一个 filter 是若干「条件组」，组之间是 OR；
- 每个条件组内是若干 Condition，条件之间是 AND；
- 网页只允许构造这里声明过的 列类型/操作符 组合，
  后端遇到不认识的列、类型或操作符时一律保守保留文件（见 pruning）。
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Iterable

import pyarrow.compute as pc

# 操作符 -> 网页展示文案
OPERATOR_LABELS: dict[str, str] = {
    "eq": "= 等于",
    "ne": "!= 不等于",
    "gt": "> 大于",
    "ge": ">= 大于等于",
    "lt": "< 小于",
    "le": "<= 小于等于",
    "in": "在列表中 (逗号分隔)",
    "is_null": "为空 IS NULL",
    "is_not_null": "非空 IS NOT NULL",
    "startswith": "前缀匹配 STARTS WITH",
}

# 不需要填值的操作符
NO_VALUE_OPS = frozenset({"is_null", "is_not_null"})
LIST_VALUE_OPS = frozenset({"in"})

# 每种列 kind 明确支持的操作符——网页只提供这些组合
SUPPORTED_OPS_BY_KIND: dict[str, frozenset[str]] = {
    "int": frozenset(
        {"eq", "ne", "gt", "ge", "lt", "le", "in", "is_null", "is_not_null"}
    ),
    "float": frozenset(
        {"eq", "ne", "gt", "ge", "lt", "le", "in", "is_null", "is_not_null"}
    ),
    "string": frozenset(
        {
            "eq", "ne", "gt", "ge", "lt", "le", "in",
            "is_null", "is_not_null", "startswith",
        }
    ),
}

ALL_SUPPORTED_OPS = frozenset(OPERATOR_LABELS)


@dataclass(frozen=True)
class Condition:
    """单个筛选条件，例如 amount > 100。"""

    column: str
    op: str
    value: Any = None

    def is_supported_for(self, kind: str | None) -> bool:
        """该条件是否是项目明确支持的 列类型/操作符 组合。"""
        if kind is None:
            return False
        ops = SUPPORTED_OPS_BY_KIND.get(kind)
        if ops is None or self.op not in ops:
            return False
        if self.op in NO_VALUE_OPS:
            return True
        if self.op in LIST_VALUE_OPS:
            return isinstance(self.value, list) and len(self.value) > 0
        return self.value is not None

    def to_expression(self) -> pc.Expression:
        """转成 pyarrow.compute 表达式，用于实际扫描时过滤行。"""
        field = pc.field(self.column)
        op = self.op
        if op == "is_null":
            return field.is_null()
        if op == "is_not_null":
            return field.is_valid()
        if op == "startswith":
            return pc.starts_with(field, str(self.value))
        if op == "in":
            return field.isin(list(self.value))
        if op == "eq":
            return field == self.value
        if op == "ne":
            return field != self.value
        if op == "gt":
            return field > self.value
        if op == "ge":
            return field >= self.value
        if op == "lt":
            return field < self.value
        if op == "le":
            return field <= self.value
        # 未知操作符不构造表达式（上层应保守处理）
        raise ValueError(f"不支持的操作符: {op}")


# DNF: list[list[Condition]]，外层 OR、内层 AND
FilterGroups = list[list[Condition]]


def build_expression(groups: FilterGroups) -> pc.Expression | None:
    """把 DNF 条件组合成一个 pyarrow 表达式；无条件时返回 None（全放行）。"""
    active = [g for g in groups if g]
    if not active:
        return None
    or_parts: list[pc.Expression] = []
    for group in active:
        expr = group[0].to_expression()
        for cond in group[1:]:
            expr = expr & cond.to_expression()
        or_parts.append(expr)
    whole = or_parts[0]
    for part in or_parts[1:]:
        whole = whole | part
    return whole


def _coerce_value(raw: Any, kind: str) -> Any:
    if isinstance(raw, bool) or raw is None:
        raise ValueError(f"非法筛选值: {raw!r}")
    if kind == "int":
        if isinstance(raw, int):
            return raw
        if isinstance(raw, str) and raw.strip().lstrip("-").isdigit():
            return int(raw.strip())
        raise ValueError(f"整数列需要整数值，收到: {raw!r}")
    if kind == "float":
        if isinstance(raw, (int, float)) and not isinstance(raw, bool):
            value = float(raw)
        elif isinstance(raw, str):
            value = float(raw.strip())
        else:
            raise ValueError(f"数值列需要数值，收到: {raw!r}")
        if math.isnan(value):
            raise ValueError("不允许使用 NaN 作为筛选值")
        return value
    if not isinstance(raw, str):
        raise ValueError(f"字符串列需要字符串值，收到: {raw!r}")
    return raw


def parse_groups(payload: Any, column_kinds: dict[str, str]) -> FilterGroups:
    """解析网页/API 提交的 JSON 筛选条件。

    期望结构：[[{"column": "amount", "op": "gt", "value": 100}, ...], ...]
    只接受项目明确支持的组合，否则抛 ValueError。
    """
    if not isinstance(payload, list) or not payload:
        raise ValueError("筛选条件必须是非空的条件组列表（组间 OR）")
    groups: FilterGroups = []
    for gi, raw_group in enumerate(payload):
        if not isinstance(raw_group, list) or not raw_group:
            raise ValueError(f"条件组 #{gi + 1} 必须是非空条件列表（组内 AND）")
        group: list[Condition] = []
        for raw_cond in raw_group:
            if not isinstance(raw_cond, dict):
                raise ValueError("条件必须是对象")
            column = raw_cond.get("column")
            op = raw_cond.get("op")
            if not isinstance(column, str) or not isinstance(op, str):
                raise ValueError("条件缺少 column/op 字符串字段")
            kind = column_kinds.get(column)
            if kind is None:
                raise ValueError(f"列 {column!r} 不在索引 schema 中")
            if op not in ALL_SUPPORTED_OPS:
                raise ValueError(f"不支持的操作符: {op!r}")
            if op in NO_VALUE_OPS:
                cond = Condition(column=column, op=op)
            elif op in LIST_VALUE_OPS:
                raw_value = raw_cond.get("value")
                if not isinstance(raw_value, list) or not raw_value:
                    raise ValueError(f"{column} 的 in 条件需要非空列表")
                value = [_coerce_value(v, kind) for v in raw_value]
                cond = Condition(column=column, op=op, value=value)
            else:
                value = _coerce_value(raw_cond.get("value"), kind)
                cond = Condition(column=column, op=op, value=value)
            if not cond.is_supported_for(kind):
                raise ValueError(f"列类型 {kind} 不支持操作符 {op!r}")
            group.append(cond)
        groups.append(group)
    return groups


def iter_conditions(groups: FilterGroups) -> Iterable[Condition]:
    for group in groups:
        yield from group
