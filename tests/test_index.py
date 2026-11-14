"""索引测试：内容指纹、过期回退、新增/修订文件、断点续建、重建。"""

from __future__ import annotations

import json
import os
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from partition_query.index_store import (
    BuildProgress,
    IndexStore,
    list_parquet_files,
)

from tests.conftest import SCHEMA, _write


def test_fingerprint_detects_same_size_replacement(partitioned_dir):
    """同尺寸、同 mtime 精度内的原样替换也要靠 SHA-256 识别为过期。"""
    store = IndexStore(partitioned_dir)
    store.build_or_resume()
    target = partitioned_dir / "p0.parquet"
    original_sha = store.entries[str(target)].sha256

    # 用不同内容但尽量接近的行数重写，并把 mtime 改成与旧文件完全一致
    stat = os.stat(target)
    _write(target, {
        "id": [0, 2, 4, 6, 8, 999],  # 最后一个值改变，行数不变
        "amount": [10.0, None, 30.0, None, 50.0, 100.0],
        "name": ["SKU-A-1", "SKU-A-2", "SKU-A-3", "SKU-A-4", "SKU-A-5", "SKU-A-6"],
        "feedback": ["good", None, "ok", None, None, "fine"],
    })
    os.utime(target, ns=(stat.st_atime_ns, stat.st_mtime_ns))

    # 快速指纹可能恰好相同，也可能不同；内容指纹必须识别
    state = store.file_state(str(target))
    if store.entries[str(target)].quick == {
        "size": os.stat(target).st_size,
        "mtime_ns": stat.st_mtime_ns,
    }:
        assert state == "stale"  # 由 SHA-256 兜底
    else:
        assert state == "stale"
    assert store.entries[str(target)].sha256 == original_sha  # 索引本身未被篡改


def test_new_file_is_unindexed_then_incrementally_added(partitioned_dir):
    store = IndexStore(partitioned_dir)
    store.build_or_resume()
    assert len(store.entries) == 4

    new_path = partitioned_dir / "p4.parquet"
    _write(new_path, {
        "id": [60], "amount": [500.0], "name": ["SKU-N-1"], "feedback": [None],
    })
    assert store.file_state(str(new_path)) == "unindexed"

    store.build_or_resume()
    assert store.file_state(str(new_path)) == "indexed"
    assert len(store.entries) == 5


def test_modified_file_marked_stale_and_reindexed(partitioned_dir):
    store = IndexStore(partitioned_dir)
    store.build_or_resume()
    target = partitioned_dir / "p1.parquet"
    time.sleep(0.01)
    _write(target, {
        "id": [10, 12, 14, 999],
        "amount": [100.0, 120.0, 150.0, 200.0],
        "name": ["SKU-B-1", "SKU-B-2", "SKU-A-9", "SKU-C-1"],
        "feedback": [None, "meh", None, None],
    })
    assert store.file_state(str(target)) == "stale"
    store.build_or_resume()
    assert store.file_state(str(target)) == "indexed"
    assert store.entries[str(target)].stat.columns["id"].max == 999


def test_deleted_file_removed_from_index(partitioned_dir):
    store = IndexStore(partitioned_dir)
    store.build_or_resume()
    (partitioned_dir / "p3.parquet").unlink()
    store.build_or_resume()
    assert len(store.entries) == 3
    assert all("p3" not in path for path in store.entries)


def test_rebuild_discards_journal_and_index(partitioned_dir):
    store = IndexStore(partitioned_dir)
    store.build_or_resume()
    journal = store.journal_path
    # 正常完成后 journal 已清理；手动制造一个残留 journal
    store.index_dir.mkdir(parents=True, exist_ok=True)
    journal.write_text('{"event": "file_start", "path": "x"}\n', encoding="utf-8")
    store.build_or_resume(rebuild=True)
    assert not journal.exists()
    assert len(store.entries) == 4


class _Boom(Exception):
    pass


def test_resume_after_interruption_skips_done_files(partitioned_dir):
    """模拟建索引中途中断：journal 记录的已完成文件，续建时跳过。"""
    calls: list[str] = []

    def flaky_builder(path: str):
        from partition_query.stats import build_file_stat
        calls.append(os.path.basename(path))
        if path.endswith("p2.parquet"):
            raise _Boom("模拟中断")
        return build_file_stat(path)

    store = IndexStore(partitioned_dir)
    with pytest.raises(_Boom):
        store.build_or_resume(stat_builder=flaky_builder)
    # p0、p1 已完成并落盘，p2 中断
    assert store.index_path.exists()
    journal_lines = store.journal_path.read_text(encoding="utf-8").strip().splitlines()
    events = [json.loads(line)["event"] for line in journal_lines]
    assert events.count("file_done") == 2

    # 续建：用正常 builder，已完成文件不再重新统计
    calls.clear()
    progress = store.build_or_resume()
    resumed = [e for e in progress.events if e["event"] == "skip_resumed"]
    assert len(resumed) == 2
    assert progress.processed == 4
    assert len(store.entries) == 4
    assert not store.journal_path.exists()  # 完成后 journal 清理


def test_resume_works_with_fresh_store_without_explicit_load(partitioned_dir):
    """真实 CLI 场景的兜底：新建的 store 不手动 load 也能靠快照+journal 续建。"""
    calls: list[str] = []

    def flaky_builder(path: str):
        from partition_query.stats import build_file_stat
        calls.append(os.path.basename(path))
        if path.endswith("p2.parquet"):
            raise _Boom("模拟中断")
        return build_file_stat(path)

    first = IndexStore(partitioned_dir)
    with pytest.raises(_Boom):
        first.build_or_resume(stat_builder=flaky_builder)

    # 全新对象，不调用 load()，直接续建
    fresh = IndexStore(partitioned_dir)
    progress = fresh.build_or_resume()
    resumed = [e for e in progress.events if e["event"] == "skip_resumed"]
    assert len(resumed) == 2  # p0、p1 从快照+journal 恢复并跳过
    assert len(fresh.entries) == 4
    assert not fresh.journal_path.exists()


def test_index_persistence_roundtrip(partitioned_dir):
    store = IndexStore(partitioned_dir)
    store.build_or_resume()
    reloaded = IndexStore(partitioned_dir)
    assert reloaded.load() is True
    assert set(reloaded.entries) == set(store.entries)
    sample = next(iter(reloaded.entries.values()))
    assert sample.stat.columns["amount"].null_count == 2  # p0 的空值数
    assert sample.stat.columns["feedback"].all_null in (True, False)


def test_stale_index_falls_back_to_scan(partitioned_dir):
    """查询路径上：过期文件状态为 stale，规划时保守保留并登记为回退文件。"""
    from partition_query.filters import Condition
    from partition_query.query import QueryRunner

    store = IndexStore(partitioned_dir)
    store.build_or_resume()
    target = partitioned_dir / "p0.parquet"
    time.sleep(0.01)
    _write(target, {
        "id": [1000, 1002, 1004, 1006, 1008, 1010],
        "amount": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        "name": ["X-1", "X-2", "X-3", "X-4", "X-5", "X-6"],
        "feedback": [None] * 6,
    })
    groups = [[Condition(column="id", op="eq", value=1005)]]
    report = QueryRunner(store).run(groups)
    assert str(target) in report.fallback_files
    target_plan = next(p for p in report.plans if p.path.endswith("p0.parquet"))
    assert target_plan.action == "keep"
    assert target_plan.index_state == "stale"
    # 回退扫描保证结果与基线一致（核对通过）
    assert report.equivalent is True
