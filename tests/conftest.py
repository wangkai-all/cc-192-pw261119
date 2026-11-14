"""测试夹具：构造覆盖各种边界情形的小型 Parquet 分区。"""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from partition_query.index_store import IndexStore

SCHEMA = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("amount", pa.float64()),
    pa.field("name", pa.string()),
    pa.field("feedback", pa.string()),  # 存在全空分区
])


def _write(path: Path, rows: dict[str, list]) -> None:
    arrays = [pa.array(rows[name], type=SCHEMA.field(name).type) for name in SCHEMA.names]
    pq.write_table(pa.Table.from_arrays(arrays, schema=SCHEMA), path, compression="snappy")


@pytest.fixture()
def partitioned_dir(tmp_path: Path) -> Path:
    """4 个小分区，刻意覆盖边界相等、空值与全空列。"""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    # 分区 0：id [0..10]，amount 含空值，name 全是 SKU-A 前缀
    _write(data_dir / "p0.parquet", {
        "id": [0, 2, 4, 6, 8, 10],
        "amount": [10.0, None, 30.0, None, 50.0, 100.0],
        "name": ["SKU-A-1", "SKU-A-2", "SKU-A-3", "SKU-A-4", "SKU-A-5", "SKU-A-6"],
        "feedback": ["good", None, "ok", None, None, "fine"],
    })
    # 分区 1：id 从 10 开始（与 p0 最大值边界相等），amount [100..200]
    _write(data_dir / "p1.parquet", {
        "id": [10, 12, 14, 16],
        "amount": [100.0, 120.0, 150.0, 200.0],
        "name": ["SKU-B-1", "SKU-B-2", "SKU-A-9", "SKU-C-1"],
        "feedback": [None, "meh", None, None],
    })
    # 分区 2：id [20..30]，feedback 整列全空
    _write(data_dir / "p2.parquet", {
        "id": [20, 22, 24],
        "amount": [210.0, 250.0, 300.0],
        "name": ["SKU-C-2", "SKU-D-1", "SKU-D-2"],
        "feedback": [None, None, None],
    })
    # 分区 3：id [40..50]，amount 全空，name 全是 SKU-Z
    _write(data_dir / "p3.parquet", {
        "id": [40, 42, 50],
        "amount": [None, None, None],
        "name": ["SKU-Z-1", "SKU-Z-2", "SKU-Z-3"],
        "feedback": ["a", "b", "c"],
    })
    return data_dir


@pytest.fixture()
def indexed_store(partitioned_dir: Path) -> IndexStore:
    store = IndexStore(partitioned_dir)
    store.build_or_resume()
    return store
