"""真实读取量测量与「裁剪 vs 全量基线」等价性测试。"""

from __future__ import annotations

import itertools

import pytest

from partition_query.filters import Condition, parse_groups
from partition_query.query import QueryRunner
from partition_query.scanner import CountingFile, scan_file, skip_file


def test_counting_file_measures_real_bytes(partitioned_dir):
    path = str(partitioned_dir / "p0.parquet")
    with CountingFile(path) as counting:
        counting.seek(0)
        counting.read(10)
        counting.read(5)
    assert counting.measured_bytes == 15
    assert counting.read_calls == 2
    assert counting.closed is True


def test_scan_file_measured_bytes_positive_and_marked(partitioned_dir):
    result = scan_file(str(partitioned_dir / "p0.parquet"), [])
    assert result.error is None
    assert result.measured_bytes > 0  # 实测，不是估算
    assert result.estimated_bytes == result.file_size_bytes
    assert result.read is True


def test_skip_file_records_zero_measured_bytes(partitioned_dir):
    result = skip_file(str(partitioned_dir / "p0.parquet"), "测试排除")
    assert result.read is False
    assert result.measured_bytes == 0
    assert result.estimated_bytes == 0
    assert result.skipped_reason == "测试排除"


# 覆盖所有支持的操作符与代表性取值
INT_FLOAT_OPS = ["eq", "ne", "gt", "ge", "lt", "le", "in", "is_null", "is_not_null"]
STRING_OPS = INT_FLOAT_OPS + ["startswith"]


def _cases():
    cases = []
    for op in ("eq", "ne", "gt", "ge", "lt", "le"):
        cases.append(("id", op, 10))
    cases.append(("id", "in", [10, 22, 999]))
    cases += [("id", "is_null", None), ("id", "is_not_null", None)]
    for op in ("gt", "ge", "lt", "le", "eq", "ne"):
        cases.append(("amount", op, 100.0))
    cases.append(("amount", "in", [100.0, 300.0]))
    cases += [("amount", "is_null", None), ("amount", "is_not_null", None)]
    for op in ("eq", "ne", "gt", "ge", "lt", "le"):
        cases.append(("name", op, "SKU-A-1"))
    cases.append(("name", "in", ["SKU-A-1", "SKU-Z-2"]))
    cases += [
        ("name", "is_null", None),
        ("name", "is_not_null", None),
        ("name", "startswith", "SKU-Z"),
        ("name", "startswith", "SKU"),
        ("feedback", "is_null", None),
        ("feedback", "is_not_null", None),
        ("feedback", "eq", "good"),
    ]
    return cases


@pytest.mark.parametrize("column, op, value", _cases())
def test_single_conditions_never_lose_rows(indexed_store, column, op, value):
    cond = Condition(column=column, op=op, value=value)
    report = QueryRunner(indexed_store).run([[cond]])
    assert report.equivalent, report.equivalence_detail
    assert report.matched_rows == report.baseline_rows


def test_dnf_or_equivalence(indexed_store):
    groups = [
        [Condition("id", "gt", 20)],
        [Condition("name", "startswith", "SKU-A")],
        [Condition("amount", "is_null")],
    ]
    report = QueryRunner(indexed_store).run(groups)
    assert report.equivalent
    # OR 查询至少应保留多个文件
    assert sum(1 for p in report.plans if p.action == "keep") >= 2


def test_and_group_equivalence(indexed_store):
    groups = [[
        Condition("id", "ge", 10),
        Condition("amount", "le", 200.0),
        Condition("name", "startswith", "SKU"),
    ]]
    report = QueryRunner(indexed_store).run(groups)
    assert report.equivalent


def test_pruning_reduces_measured_reads(indexed_store):
    # 高选择性等值查询：裁剪后实测字节必须显著低于全量基线
    groups = [[Condition("id", "eq", 50)]]
    report = QueryRunner(indexed_store).run(groups)
    assert report.equivalent
    assert report.measured_read_bytes < report.baseline_measured_read_bytes
    excluded = sum(1 for p in report.plans if p.action == "exclude")
    assert excluded >= 1
    # 估算与实测是两个不同来源的数字，页面也分别标注
    assert isinstance(report.estimated_read_bytes, int)
    assert isinstance(report.measured_read_bytes, int)


def test_excluded_files_are_not_read(indexed_store):
    groups = [[Condition("id", "eq", 50)]]
    report = QueryRunner(indexed_store).run(groups)
    for plan, scan in zip(report.plans, report.scan_results):
        if plan.action == "exclude":
            assert scan.read is False
            assert scan.measured_bytes == 0
            assert scan.skipped_reason is not None


def test_empty_filter_groups_rejected(indexed_store):
    with pytest.raises(ValueError):
        parse_groups([], indexed_store.schema_summary()["column_kinds"])


def test_web_api_query_and_status(indexed_store):
    from partition_query.webapp import create_app

    app = create_app(indexed_store.data_dir)
    client = app.test_client()
    status = client.get("/api/status").get_json()
    assert status["parquet_files"] == 4
    resp = client.post("/api/query", json={
        "filters": [[{"column": "id", "op": "eq", "value": 10}]]
    })
    data = resp.get_json()
    assert resp.status_code == 200
    assert data["equivalent"] is True
    # 不支持的组合被页面后端拒绝
    bad = client.post("/api/query", json={
        "filters": [[{"column": "id", "op": "startswith", "value": "1"}]]
    })
    assert bad.status_code == 400


def test_reason_text_is_present_for_every_file(indexed_store):
    groups = [[Condition("amount", "gt", 150.0)]]
    report = QueryRunner(indexed_store).run(groups)
    for plan in report.plans:
        assert plan.reason
        if plan.index_state == "indexed":
            assert plan.group_reasons and all(
                cv["reason"]
                for gv in plan.group_reasons
                for cv in gv["conditions"]
            )
