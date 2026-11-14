"""查询编排：裁剪计划、实际扫描、与关闭裁剪的基线查询核对。"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

from .filters import FilterGroups
from .index_store import IndexStore, list_parquet_files
from .pruning import EXCLUDE, FileVerdict, prune_file
from .scanner import FileScanResult, scan_file, skip_file, tables_equivalent


@dataclass
class FilePlan:
    path: str
    action: str  # keep / exclude
    reason: str
    index_state: str  # indexed / stale / unindexed / missing
    group_reasons: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class QueryReport:
    plans: list[FilePlan]
    scan_results: list[FileScanResult]
    baseline_results: list[FileScanResult]
    matched_rows: int
    baseline_rows: int
    equivalent: bool
    equivalence_detail: str
    elapsed_ms: float
    # 估算与实测分别汇总，命名里写死 estimated / measured 防止混用
    estimated_read_bytes: int
    measured_read_bytes: int
    baseline_measured_read_bytes: int
    total_file_bytes: int
    fallback_files: list[str]
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        def plan_dict(plan: FilePlan) -> dict[str, Any]:
            return {
                "path": os.path.relpath(plan.path, self._data_dir),
                "absolute_path": plan.path,
                "action": plan.action,
                "reason": plan.reason,
                "index_state": plan.index_state,
                "group_reasons": plan.group_reasons,
            }

        def scan_dict(result: FileScanResult) -> dict[str, Any]:
            return {
                "path": os.path.relpath(result.path, self._data_dir),
                "read": result.read,
                "file_size_bytes": result.file_size_bytes,
                "estimated_bytes": result.estimated_bytes,
                "measured_bytes": result.measured_bytes,
                "read_calls": result.read_calls,
                "rows_in_file": result.rows_in_file,
                "rows_matched": result.rows_matched,
                "elapsed_ms": result.elapsed_ms,
                "skipped_reason": result.skipped_reason,
                "error": result.error,
            }

        return {
            "matched_rows": self.matched_rows,
            "baseline_rows": self.baseline_rows,
            "equivalent": self.equivalent,
            "equivalence_detail": self.equivalence_detail,
            "elapsed_ms": self.elapsed_ms,
            "estimated_read_bytes": self.estimated_read_bytes,
            "measured_read_bytes": self.measured_read_bytes,
            "baseline_measured_read_bytes": self.baseline_measured_read_bytes,
            "total_file_bytes": self.total_file_bytes,
            "kept_count": sum(1 for p in self.plans if p.action == "keep"),
            "excluded_count": sum(1 for p in self.plans if p.action == EXCLUDE),
            "fallback_files": [
                os.path.relpath(p, self._data_dir) for p in self.fallback_files
            ],
            "files": [
                {
                    "plan": plan_dict(plan),
                    "scan": scan_dict(scan),
                    "baseline_scan": scan_dict(base),
                }
                for plan, scan, base in zip(
                    self.plans, self.scan_results, self.baseline_results
                )
            ],
            "error": self.error,
        }

    # 供 to_dict 计算相对路径
    _data_dir: str = ""


class QueryRunner:
    def __init__(self, store: IndexStore):
        self.store = store

    def _plan_for(self, path: str, groups: FilterGroups) -> FilePlan:
        stat, state = self.store.get_file_stat(path)
        if stat is None:
            # 索引过期 / 指纹不符 / 未建索引：保守保留并回退实际扫描
            reason = {
                "stale": "索引过期或指纹不符，无法信任统计，回退实际扫描并保留文件",
                "unindexed": "该文件尚未建立索引，回退实际扫描并保留文件",
                "missing": "文件在规划后消失，仍尝试扫描以暴露问题",
            }.get(state, "索引状态未知，保守保留文件")
            return FilePlan(path=path, action="keep", reason=reason, index_state=state)

        verdict: FileVerdict = prune_file(stat, groups)
        group_reasons = [
            {
                "action": gv.action,
                "reason": gv.reason,
                "conditions": [
                    {"column": cv.column, "op": cv.op, "action": cv.action, "reason": cv.reason}
                    for cv in gv.condition_verdicts
                ],
            }
            for gv in verdict.group_verdicts
        ]
        return FilePlan(
            path=path,
            action=verdict.action,
            reason=verdict.reason,
            index_state="indexed",
            group_reasons=group_reasons,
        )

    def _concat(self, tables: list[pa.Table], paths: list[str]) -> pa.Table:
        """拼接结果表；没有任何表时，用第一个文件的 schema 构造空表，
        保证裁剪查询与基线查询的空结果 schema 仍然一致。"""
        if tables:
            return pa.concat_tables(tables)
        import pyarrow.parquet as pq

        if paths:
            return pq.ParquetFile(paths[0]).schema_arrow.empty_table()
        return pa.table({})

    def run(self, groups: FilterGroups, columns: list[str] | None = None) -> QueryReport:
        started = time.perf_counter()
        paths = list_parquet_files(self.store.data_dir)
        plans = [self._plan_for(path, groups) for path in paths]
        fallback = [
            p.path for p in plans if p.index_state in ("stale", "unindexed", "missing")
        ]

        # ---- 裁剪查询：只读取保留文件 ----
        scan_results: list[FileScanResult] = []
        kept_tables: list[pa.Table] = []
        for plan in plans:
            if plan.action == EXCLUDE:
                scan_results.append(skip_file(plan.path, plan.reason))
            else:
                result = scan_file(plan.path, groups, columns=columns)
                scan_results.append(result)
                if result.error is None and result.table is not None:
                    kept_tables.append(result.table)
        pruned_table = self._concat(kept_tables, paths)

        # ---- 基线查询：关闭裁剪，全部文件实际扫描（同样应用筛选表达式）----
        baseline_results: list[FileScanResult] = []
        baseline_tables: list[pa.Table] = []
        for path in paths:
            result = scan_file(path, groups, columns=columns)
            baseline_results.append(result)
            if result.error is None and result.table is not None:
                baseline_tables.append(result.table)
        baseline_table = self._concat(baseline_tables, paths)

        equivalent, detail = tables_equivalent(pruned_table, baseline_table)

        report = QueryReport(
            plans=plans,
            scan_results=scan_results,
            baseline_results=baseline_results,
            matched_rows=pruned_table.num_rows,
            baseline_rows=baseline_table.num_rows,
            equivalent=equivalent,
            equivalence_detail=detail,
            elapsed_ms=round((time.perf_counter() - started) * 1000, 3),
            estimated_read_bytes=sum(r.estimated_bytes for r in scan_results),
            measured_read_bytes=sum(r.measured_bytes for r in scan_results),
            baseline_measured_read_bytes=sum(r.measured_bytes for r in baseline_results),
            total_file_bytes=sum(os.path.getsize(p) for p in paths),
            fallback_files=fallback,
            _data_dir=str(self.store.data_dir),
        )
        return report
