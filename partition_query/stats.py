"""单个 Parquet 文件的列统计。

每个文件、每一列维护：
- kind: 列类型（int / float / string），未知类型不参与裁剪；
- row_count / null_count: 行数与空值数量（null_count 为 None 表示统计不可用，保守处理）；
- min / max: 数值或字符串列的最小值、最大值；
- all_null: 是否整列全空；
- prefixes: 字符串前缀摘要（按出现次数排序的前缀及计数）。
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field, asdict
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

# 字符串前缀摘要：长度 1、2 两档，每档保留计数最高的前缀数
PREFIX_LENGTHS = (1, 2)
MAX_PREFIXES_PER_LENGTH = 10


def _kind_of(arrow_type: pa.DataType) -> str | None:
    if pa.types.is_integer(arrow_type):
        return "int"
    if pa.types.is_floating(arrow_type):
        return "float"
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "string"
    # 其他类型（布尔、日期、二进制、嵌套等）本项目不支持裁剪
    return None


def _safe_number(value: Any) -> Any:
    """NaN/Inf 不写入 JSON 索引，统一转成 None（统计不可用，保守处理）。"""
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


@dataclass
class ColumnStat:
    column: str
    kind: str | None
    row_count: int
    null_count: int | None = None
    min: Any = None
    max: Any = None
    min_available: bool = False
    max_available: bool = False
    all_null: bool = False
    # {前缀长度: [[前缀, 计数], ...]}
    prefixes: dict[str, list[list[Any]]] = field(default_factory=dict)
    # {前缀长度: 该长度下的摘要是否覆盖全部不同前缀（False=只保留了 top-K，缺失不能用于排除）}
    prefix_complete: dict[str, bool] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ColumnStat":
        return cls(**data)


def _summarize_prefixes(column: pa.ChunkedArray) -> dict[str, list[list[Any]]]:
    """扫描字符串列，按前缀长度聚合出现次数（空字符串不计）。"""
    counters: dict[int, dict[str, int]] = {length: {} for length in PREFIX_LENGTHS}
    for chunk in column.chunks:
        values = chunk.drop_null().to_pylist()
        for value in values:
            if not value:
                continue
            for length in PREFIX_LENGTHS:
                if len(value) >= length:
                    prefix = value[:length]
                    counters[length][prefix] = counters[length].get(prefix, 0) + 1
    result: dict[str, list[list[Any]]] = {}
    complete: dict[str, bool] = {}
    for length, counter in counters.items():
        items = sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))
        complete[str(length)] = len(items) <= MAX_PREFIXES_PER_LENGTH
        top = items[:MAX_PREFIXES_PER_LENGTH]
        result[str(length)] = [[prefix, count] for prefix, count in top]
    return result, complete


def _build_column_stat(column_name: str, column: pa.ChunkedArray) -> ColumnStat:
    row_count = len(column)
    null_count = column.null_count
    kind = _kind_of(column.type)
    stat = ColumnStat(
        column=column_name,
        kind=kind,
        row_count=row_count,
        null_count=null_count,
        all_null=(null_count == row_count and row_count > 0)
        or (row_count == 0),
    )
    if kind is None:
        return stat
    if stat.all_null:
        return stat

    non_null = column.drop_null()
    if kind in ("int", "float"):
        try:
            min_scalar = pa.compute.min(non_null).as_py()
            max_scalar = pa.compute.max(non_null).as_py()
        except pa.ArrowNotImplementedError:
            min_scalar = max_scalar = None
        stat.min = _safe_number(min_scalar)
        stat.max = _safe_number(max_scalar)
        stat.min_available = stat.min is not None
        stat.max_available = stat.max is not None
    elif kind == "string":
        try:
            min_scalar = pa.compute.min(non_null).as_py()
            max_scalar = pa.compute.max(non_null).as_py()
        except pa.ArrowNotImplementedError:
            min_scalar = max_scalar = None
        stat.min = min_scalar
        stat.max = max_scalar
        stat.min_available = stat.min is not None
        stat.max_available = stat.max is not None
        stat.prefixes, stat.prefix_complete = _summarize_prefixes(column)
    return stat


@dataclass
class FileStat:
    """一个 Parquet 文件的完整统计快照。"""

    path: str
    row_count: int
    columns: dict[str, ColumnStat]

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "row_count": self.row_count,
            "columns": {name: col.to_dict() for name, col in self.columns.items()},
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FileStat":
        return cls(
            path=data["path"],
            row_count=data["row_count"],
            columns={
                name: ColumnStat.from_dict(col)
                for name, col in data["columns"].items()
            },
        )


def build_file_stat(path: str) -> FileStat:
    """实际读取 Parquet 文件并计算列统计（建索引时使用）。"""
    parquet_file = pq.ParquetFile(path)
    table = parquet_file.read()
    columns: dict[str, ColumnStat] = {}
    for name in table.column_names:
        columns[name] = _build_column_stat(name, table.column(name))
    return FileStat(path=path, row_count=table.num_rows, columns=columns)
