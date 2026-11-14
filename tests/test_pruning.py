"""裁剪判定专项测试：全空列、含空值比较、边界相等、逻辑或分别处理。"""

from __future__ import annotations

from partition_query.filters import Condition, parse_groups
from partition_query.pruning import EXCLUDE, KEEP, prune_file


def _stat(store, suffix):
    path = str(store.data_dir / suffix)
    return store.entries[path].stat


def _verdict(store, groups, suffix):
    return prune_file(_stat(store, suffix), groups)


def _cond(column, op, value=None):
    return Condition(column=column, op=op, value=value)


# ---------- 全空列 ----------
def test_all_null_column_is_null_keeps(indexed_store):
    groups = [[_cond("feedback", "is_null")]]
    # p2 的 feedback 整列全空 -> IS NULL 必命中，保留
    assert _verdict(indexed_store, groups, "p2.parquet").action == KEEP


def test_all_null_column_is_not_null_excludes(indexed_store):
    groups = [[_cond("feedback", "is_not_null")]]
    assert _verdict(indexed_store, groups, "p2.parquet").action == EXCLUDE


def test_all_null_column_value_comparison_excludes(indexed_store):
    # 全空列上做等值/大于等比较都不可能命中
    for op, value in [("eq", "x"), ("ne", "x"), ("gt", "a"), ("startswith", "S")]:
        groups = [[_cond("feedback", op, value)]]
        verdict = _verdict(indexed_store, groups, "p2.parquet")
        assert verdict.action == EXCLUDE, op


def test_all_null_amount_comparison_excludes(indexed_store):
    # p3 的 amount 整列全空
    groups = [[_cond("amount", "gt", 0.0)]]
    assert _verdict(indexed_store, groups, "p3.parquet").action == EXCLUDE
    groups = [[_cond("amount", "is_null")]]
    assert _verdict(indexed_store, groups, "p3.parquet").action == KEEP


# ---------- 含空值比较 ----------
def test_nullable_column_range_pruning_still_sound(indexed_store):
    # p0 amount 含空值，值域 [10, 100]；查 > 200 必须排除（null 不会让 > 命中）
    groups = [[_cond("amount", "gt", 200.0)]]
    assert _verdict(indexed_store, groups, "p0.parquet").action == EXCLUDE
    # 查 is_null 时 p0 有空值，保留
    groups = [[_cond("amount", "is_null")]]
    assert _verdict(indexed_store, groups, "p0.parquet").action == KEEP
    # p3 amount 全空，is_not_null 排除
    groups = [[_cond("amount", "is_not_null")]]
    assert _verdict(indexed_store, groups, "p3.parquet").action == EXCLUDE


def test_ne_on_non_constant_column_kept(indexed_store):
    # != 只有常量列才能安全排除；p0 amount 有多个不同值，必须保留
    groups = [[_cond("amount", "ne", 30.0)]]
    assert _verdict(indexed_store, groups, "p0.parquet").action == KEEP


# ---------- 边界相等 ----------
def test_boundary_equality_gt_keeps_file_with_equal_max(indexed_store):
    # p0 amount 最大值恰好 100；amount > 100 不能命中（边界相等严格大于），排除
    groups = [[_cond("amount", "gt", 100.0)]]
    assert _verdict(indexed_store, groups, "p0.parquet").action == EXCLUDE
    # amount >= 100 可以命中最大值 100，保留
    groups = [[_cond("amount", "ge", 100.0)]]
    assert _verdict(indexed_store, groups, "p0.parquet").action == KEEP


def test_boundary_equality_eq_on_shared_id(indexed_store):
    # id=10 同时出现在 p0（max）和 p1（min），等值查询两个文件都必须保留
    groups = [[_cond("id", "eq", 10)]]
    assert _verdict(indexed_store, groups, "p0.parquet").action == KEEP
    assert _verdict(indexed_store, groups, "p1.parquet").action == KEEP
    # id=11 落在两个文件的值域空隙里，两个都排除
    groups = [[_cond("id", "eq", 11)]]
    assert _verdict(indexed_store, groups, "p0.parquet").action == EXCLUDE
    # p1 的 id 取值是 10/12/14/16，min=10、max=16，值域包含 11，
    # min/max 摘要无法判断 11 是否真实存在 -> 保守保留（不漏结果优先）
    assert _verdict(indexed_store, groups, "p1.parquet").action == KEEP


def test_lt_boundary_equality(indexed_store):
    # p1 id 最小值 10；id < 10 排除（边界相等），id <= 10 保留
    groups = [[_cond("id", "lt", 10)]]
    assert _verdict(indexed_store, groups, "p1.parquet").action == EXCLUDE
    groups = [[_cond("id", "le", 10)]]
    assert _verdict(indexed_store, groups, "p1.parquet").action == KEEP


# ---------- 逻辑或 ----------
def test_or_keeps_if_any_group_matches(indexed_store):
    # 组1: id > 100（所有文件都排除）；组2: name startswith SKU-Z（只有 p3 可能）
    groups = [
        [_cond("id", "gt", 100)],
        [_cond("name", "startswith", "SKU-Z")],
    ]
    assert _verdict(indexed_store, groups, "p0.parquet").action == EXCLUDE
    assert _verdict(indexed_store, groups, "p3.parquet").action == KEEP


def test_or_excludes_only_when_all_groups_exclude(indexed_store):
    groups = [
        [_cond("id", "gt", 1000)],
        [_cond("amount", "lt", 0.0)],
    ]
    for name in ("p0.parquet", "p1.parquet", "p2.parquet", "p3.parquet"):
        assert _verdict(indexed_store, groups, name).action == EXCLUDE


def test_and_group_excludes_if_any_condition_excludes(indexed_store):
    # 组内 AND：id 可能命中但 amount>999 不可能 -> 整组排除
    groups = [[_cond("id", "ge", 0), _cond("amount", "gt", 999.0)]]
    for name in ("p0.parquet", "p1.parquet", "p2.parquet", "p3.parquet"):
        assert _verdict(indexed_store, groups, name).action == EXCLUDE


# ---------- 字符串前缀 ----------
def test_startswith_excludes_impossible_prefix(indexed_store):
    groups = [[_cond("name", "startswith", "SKU-Z")]]
    assert _verdict(indexed_store, groups, "p0.parquet").action == EXCLUDE
    assert _verdict(indexed_store, groups, "p3.parquet").action == KEEP


def test_startswith_empty_prefix_kept(indexed_store):
    groups = [[_cond("name", "startswith", "")]]
    assert _verdict(indexed_store, groups, "p0.parquet").action == KEEP


# ---------- 保守性：不支持的情形一律保留 ----------
def test_unknown_column_kept(indexed_store):
    groups = [[_cond("nonexistent", "eq", 1)]]
    verdict = _verdict(indexed_store, groups, "p0.parquet")
    assert verdict.action == KEEP
    # 保守理由记录在条件级判定上，文件级理由是 OR 汇总
    cond_reason = verdict.group_verdicts[0].condition_verdicts[0].reason
    assert "保守保留" in cond_reason


def test_parse_groups_rejects_unsupported_combo(indexed_store):
    kinds = indexed_store.schema_summary()["column_kinds"]
    import pytest
    # 整数列不允许 startswith
    with pytest.raises(ValueError):
        parse_groups([[{"column": "id", "op": "startswith", "value": "1"}]], kinds)
    # 未知列拒绝
    with pytest.raises(ValueError):
        parse_groups([[{"column": "ghost", "op": "eq", "value": 1}]], kinds)
