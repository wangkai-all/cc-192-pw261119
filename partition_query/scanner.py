"""实际扫描与真实读取量测量。

两类数字严格区分，网页也用不同标签展示：
- 估算读取量（estimated）：来自索引的文件大小/行数，未经实测，页面标注「估算」；
- 实测读取量（measured）：通过 CountingFile 包装器统计 Parquet 引擎真正
  从文件对象读出的字节数与读取次数，页面标注「实测」。
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .filters import FilterGroups, build_expression


class CountingFile:
    """包装二进制文件对象，统计 Parquet 引擎实际读取的字节。

    pyarrow 通过 read/seek/tell 访问该对象；所有经过 read 的字节都计入
    measured_bytes，这是真实 I/O 测量，不是根据文件大小推算的估算值。
    """

    def __init__(self, path: str):
        self._fh = open(path, "rb")
        self.path = path
        self.measured_bytes = 0
        self.read_calls = 0

    def read(self, nbytes: int | None = None) -> bytes:
        data = self._fh.read() if nbytes is None else self._fh.read(nbytes)
        self.measured_bytes += len(data)
        self.read_calls += 1
        return data

    def seek(self, *args: Any) -> int:
        return self._fh.seek(*args)

    def tell(self) -> int:
        return self._fh.tell()

    def close(self) -> None:
        self._fh.close()

    def seekable(self) -> bool:
        return True

    # 委托其余文件对象属性（closed、mode、fileno 等），保证 pyarrow 当作普通文件使用
    def __getattr__(self, name: str) -> Any:
        return getattr(self._fh, name)

    @property
    def size(self) -> int:
        return os.fstat(self._fh.fileno()).st_size

    def __enter__(self) -> "CountingFile":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


@dataclass
class FileScanResult:
    path: str
    read: bool
    file_size_bytes: int
    # 估算：被保留就按整文件大小估算，排除则为 0；明确标注为估算
    estimated_bytes: int
    # 实测：包装器计数；未读取时为 0
    measured_bytes: int = 0
    read_calls: int = 0
    rows_in_file: int = 0
    rows_matched: int = 0
    elapsed_ms: float = 0.0
    skipped_reason: str | None = None
    error: str | None = None
    # 过滤后的结果表（不参与 JSON 序列化）
    table: pa.Table | None = None


def _parquet_row_count(path: str) -> int:
    pf = pq.ParquetFile(path)
    return pf.metadata.num_rows


def scan_file(
    path: str,
    groups: FilterGroups,
    *,
    columns: list[str] | None = None,
) -> FileScanResult:
    """读取单个文件并应用筛选，返回实测指标。"""
    file_size = os.path.getsize(path)
    rows_in_file = _parquet_row_count(path)
    expression = build_expression(groups)
    started = time.perf_counter()
    try:
        with CountingFile(path) as counting:
            parquet_file = pq.ParquetFile(counting)
            table = parquet_file.read(columns=columns)
            measured = counting.measured_bytes
            calls = counting.read_calls
        # 该 pyarrow 版本的 ParquetFile.read 不支持 filters 参数，
        # 读取后用 Arrow 表达式过滤；读取字节仍由 CountingFile 实测
        if expression is not None and table.num_rows > 0:
            table = table.filter(expression)
        elapsed = (time.perf_counter() - started) * 1000
        return FileScanResult(
            path=path,
            read=True,
            file_size_bytes=file_size,
            estimated_bytes=file_size,
            measured_bytes=measured,
            read_calls=calls,
            rows_in_file=rows_in_file,
            rows_matched=table.num_rows,
            elapsed_ms=round(elapsed, 3),
            table=table,
        )
    except Exception as exc:  # 读取失败也要如实上报，不伪装成裁剪成功
        elapsed = (time.perf_counter() - started) * 1000
        return FileScanResult(
            path=path,
            read=True,
            file_size_bytes=file_size,
            estimated_bytes=file_size,
            rows_in_file=rows_in_file,
            elapsed_ms=round(elapsed, 3),
            error=repr(exc),
        )


def skip_file(path: str, reason: str) -> FileScanResult:
    """被裁剪排除的文件：不读取，实测值为 0，估算值也为 0。"""
    return FileScanResult(
        path=path,
        read=False,
        file_size_bytes=os.path.getsize(path),
        estimated_bytes=0,
        measured_bytes=0,
        skipped_reason=reason,
    )


def tables_equivalent(left: pa.Table, right: pa.Table) -> tuple[bool, str]:
    """核对两次查询结果是否完全一致（schema、行数、排序后的每行内容）。"""
    if left.schema != right.schema:
        return False, "schema 不一致"
    if left.num_rows != right.num_rows:
        return False, f"行数不一致：裁剪查询 {left.num_rows} 行 vs 全量基线 {right.num_rows} 行"
    if left.num_rows == 0:
        return True, "两次查询均为 0 行，结果一致"
    sort_keys = [(name, "ascending") for name in left.column_names]
    left_sorted = left.sort_by(sort_keys)
    right_sorted = right.sort_by(sort_keys)
    if left_sorted.to_pylist() != right_sorted.to_pylist():
        return False, "排序后的行内容存在差异"
    return True, f"两次查询均为 {left.num_rows} 行，逐行核对一致"
