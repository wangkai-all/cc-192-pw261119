"""保守的分区裁剪判定。

核心原则：只能在「数学上确定该文件不可能命中任何行」时排除文件；
任何信息缺失、类型不支持、条件不在项目支持范围内的情形，一律保留文件。
因此裁剪永远不会漏掉结果——这一点由 tests/ 中的等价性测试守护。

每个条件的判定返回 (排除?, 理由)，理由会原样展示到网页上。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .filters import Condition, FilterGroups
from .stats import ColumnStat, FileStat

# 判定动作
KEEP = "keep"
EXCLUDE = "exclude"


@dataclass
class CondVerdict:
    action: str
    reason: str
    column: str
    op: str


@dataclass
class GroupVerdict:
    """条件组（组内 AND）：任一条件可排除，则整组不可能命中。"""

    action: str
    reason: str
    condition_verdicts: list[CondVerdict] = field(default_factory=list)


@dataclass
class FileVerdict:
    path: str
    action: str  # keep / exclude
    reason: str
    group_verdicts: list[GroupVerdict] = field(default_factory=list)
    # 指纹/schema 层面的问题（触发实际扫描回退）
    stale: bool = False


def _keep(reason: str, cond: Condition) -> CondVerdict:
    return CondVerdict(KEEP, reason, cond.column, cond.op)


def _exclude(reason: str, cond: Condition) -> CondVerdict:
    return CondVerdict(EXCLUDE, reason, cond.column, cond.op)


def _range_can_contain(stat: ColumnStat, value: Any) -> bool | None:
    """值域 [min, max] 是否可能包含 value；信息不足返回 None（无法判断）。"""
    if not stat.min_available or not stat.max_available:
        return None
    if stat.min <= value <= stat.max:
        return True
    return False


def _prefix_upper(prefix: str) -> str:
    """字典序区间上界：prefix 本身的「刚好大于」串，
    使 [prefix, upper) 恰好覆盖所有以 prefix 开头的字符串。
    全 z 串没有有限上界（例如 'z' 上界超过所有常规字符串）。
    """
    chars = list(prefix)
    for i in range(len(chars) - 1, -1, -1):
        if chars[i] != "z":
            chars[i] = chr(ord(chars[i]) + 1)
            return "".join(chars[: i + 1])
    return None  # 全 z：上界为无穷大


def _eval_condition(cond: Condition, stat: ColumnStat | None) -> CondVerdict:
    # 1) 文件里没有该列 / 类型不支持：无法判断，保守保留
    if stat is None or stat.kind is None:
        return _keep(f"列 {cond.column!r} 无可用统计（缺失或类型不支持），保守保留", cond)
    if not cond.is_supported_for(stat.kind):
        return _keep(
            f"操作符 {cond.op!r} 与列类型 {stat.kind} 不在项目支持组合内，保守保留",
            cond,
        )

    op = cond.op
    col = cond.column
    null_count = stat.null_count
    non_null = None if null_count is None else stat.row_count - null_count

    # 2) 全空列单独处理
    if stat.all_null:
        if op == "is_null":
            return _keep(f"列 {col!r} 整列全空，IS NULL 必命中，保留", cond)
        if op == "is_not_null":
            return _exclude(f"列 {col!r} 整列全空，IS NOT NULL 不可能命中，排除", cond)
        return _exclude(
            f"列 {col!r} 整列全空，{op} 比较只作用于非空值，不可能命中，排除", cond
        )

    # 3) 空值判断（含空值比较时，null 不参与普通比较但不影响排除逻辑的安全性）
    if op == "is_null":
        if null_count is None:
            return _keep(f"列 {col!r} 空值统计不可用，保守保留", cond)
        if null_count == 0:
            return _exclude(f"列 {col!r} 空值数为 0，IS NULL 不可能命中，排除", cond)
        return _keep(f"列 {col!r} 含 {null_count} 个空值，IS NULL 可能命中，保留", cond)

    if op == "is_not_null":
        if null_count is None:
            return _keep(f"列 {col!r} 空值统计不可用，保守保留", cond)
        if null_count == stat.row_count:
            return _exclude(f"列 {col!r} 整列全空，IS NOT NULL 不可能命中，排除", cond)
        return _keep(
            f"列 {col!r} 有 {non_null} 个非空值，IS NOT NULL 可能命中，保留", cond
        )

    # 以下都是只作用于非空值的比较；若文件没有任何非空值，已被上面的全空分支覆盖
    if non_null == 0:
        return _exclude(f"列 {col!r} 无非空值可参与 {op} 比较，排除", cond)

    # 4) 普通比较与 in
    if op in ("eq", "ne", "gt", "ge", "lt", "le", "in"):
        if not stat.min_available or not stat.max_available:
            return _keep(f"列 {col!r} 值域统计不可用，保守保留", cond)
        lo, hi = stat.min, stat.max

        if op == "eq":
            contained = _range_can_contain(stat, cond.value)
            if contained is False:
                return _exclude(
                    f"{col!r} 值域 [{lo}, {hi}] 不含等值 {cond.value!r}，排除", cond
                )
            if lo == hi == cond.value:
                return _keep(
                    f"{col!r} 为常量列且等于 {cond.value!r}（边界相等），必命中，保留",
                    cond,
                )
            return _keep(
                f"{col!r} 值域 [{lo}, {hi}] 可能包含 {cond.value!r}，保留", cond
            )

        if op == "ne":
            # 只有常量列且常量等于 value 时才确定全部非空行不满足 !=
            if lo == hi == cond.value:
                null_part = (
                    "且无空值，" if null_count == 0 else f"（另有 {null_count} 个空值，SQL 中 null!=x 仍为未知），"
                )
                return _exclude(
                    f"{col!r} 非空值恒等于 {cond.value!r}{null_part}!= 不可能命中，排除",
                    cond,
                )
            return _keep(
                f"{col!r} 值域 [{lo}, {hi}] 存在不等于 {cond.value!r} 的值，保留",
                cond,
            )

        if op == "gt":
            # 需要存在 value 严格小于的值；hi <= value 即排除（边界相等 hi == value 也排除）
            if hi <= cond.value:
                return _exclude(
                    f"{col!r} 最大值 {hi} <= {cond.value!r}（含边界相等），> 不可能命中，排除",
                    cond,
                )
            return _keep(f"{col!r} 最大值 {hi} > {cond.value!r}，可能命中，保留", cond)

        if op == "ge":
            if hi < cond.value:
                return _exclude(
                    f"{col!r} 最大值 {hi} < {cond.value!r}，>= 不可能命中，排除", cond
                )
            return _keep(f"{col!r} 最大值 {hi} >= {cond.value!r}，可能命中，保留", cond)

        if op == "lt":
            if lo >= cond.value:
                return _exclude(
                    f"{col!r} 最小值 {lo} >= {cond.value!r}（含边界相等），< 不可能命中，排除",
                    cond,
                )
            return _keep(f"{col!r} 最小值 {lo} < {cond.value!r}，可能命中，保留", cond)

        if op == "le":
            if lo > cond.value:
                return _exclude(
                    f"{col!r} 最小值 {lo} > {cond.value!r}，<= 不可能命中，排除", cond
                )
            return _keep(f"{col!r} 最小值 {lo} <= {cond.value!r}，可能命中，保留", cond)

        if op == "in":
            values = list(cond.value)
            hit = any(lo <= v <= hi for v in values)
            if not hit:
                return _exclude(
                    f"{col!r} 值域 [{lo}, {hi}] 与列表 {values} 无交集，排除", cond
                )
            return _keep(f"{col!r} 值域与列表 {values} 有交集，保留", cond)

    # 5) 字符串前缀：前缀摘要 + 字典序区间，双重保守判断
    if op == "startswith":
        prefix = str(cond.value)
        if prefix == "":
            return _keep(f"{col!r} 空前缀匹配任意非空字符串，保留", cond)
        length = str(len(prefix))

        # (a) 摘要判定：仅当该档摘要完整（覆盖全部不同前缀）时才能用于排除
        prefix_table = stat.prefixes.get(length, [])
        known = {p for p, _ in prefix_table}
        summary_denies = False
        if stat.prefix_complete.get(length, False):
            summary_denies = prefix not in known
        # (b) 值域区间判定：所有匹配串落在 [prefix, upper)
        range_denies = False
        if stat.min_available and stat.max_available:
            upper = _prefix_upper(prefix)
            if upper is None:
                range_denies = stat.max < prefix
            else:
                range_denies = stat.max < prefix or stat.min >= upper
        if summary_denies or range_denies:
            detail = []
            if summary_denies:
                detail.append("完整前缀摘要中不存在该前缀")
            if range_denies:
                detail.append(f"值域 [{stat.min}, {stat.max}] 与前缀区间不相交")
            return _exclude(
                f"{col!r} 前缀 {prefix!r} 不可能出现（{'；'.join(detail)}），排除",
                cond,
            )
        return _keep(f"{col!r} 前缀 {prefix!r} 可能出现（摘要/值域无法排除），保留", cond)

    # 6) 未覆盖的情形：保守保留
    return _keep(f"操作符 {op!r} 未实现裁剪判定，保守保留", cond)


def _eval_group(group: list[Condition], file_stat: FileStat) -> GroupVerdict:
    """组内 AND：任何一个条件确定排除，整组排除。"""
    verdicts = [
        _eval_condition(cond, file_stat.columns.get(cond.column)) for cond in group
    ]
    for verdict in verdicts:
        if verdict.action == EXCLUDE:
            return GroupVerdict(
                EXCLUDE,
                f"AND 组内条件「{verdict.reason}」，整组排除",
                verdicts,
            )
    return GroupVerdict(KEEP, "组内全部条件都无法排除，该 AND 组保留", verdicts)


def prune_file(file_stat: FileStat, groups: FilterGroups) -> FileVerdict:
    """对单个文件执行 DNF 裁剪：组间 OR，所有组都排除才排除文件。"""
    group_verdicts = [_eval_group(group, file_stat) for group in groups]
    if all(gv.action == EXCLUDE for gv in group_verdicts):
        reasons = "；".join(gv.reason for gv in group_verdicts)
        return FileVerdict(
            path=file_stat.path,
            action=EXCLUDE,
            reason=f"OR 的全部条件组都不可能命中（{reasons}），排除文件",
            group_verdicts=group_verdicts,
        )
    kept = [i + 1 for i, gv in enumerate(group_verdicts) if gv.action == KEEP]
    return FileVerdict(
        path=file_stat.path,
        action=KEEP,
        reason=f"OR 条件组 {kept} 仍可能命中，保留文件",
        group_verdicts=group_verdicts,
    )
