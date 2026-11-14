# 分区裁剪查询对照台（Partition Pruning Lab）

为每个 Parquet 分区文件维护**数值范围、空值数量、字符串前缀摘要**三类轻量索引，
在查询时排除数学上不可能命中的文件，并在网页上逐文件展示「保留 / 排除理由」和
**真实读取量**，同时与「关闭裁剪的全量基线查询」逐行核对，确保裁剪永不漏结果。

## 设计原则

1. **保守正确优先于少读数据**：只有能确定文件不可能命中任何行时才排除；
   统计缺失、类型不支持、操作符不在支持列表、索引过期或指纹不符，一律保留文件并回退实际扫描。
2. **估算与实测严格区分**：
   - 「估算读取量」来自文件大小/行数推算，页面与 CLI 都标注「估算」，绝不写成真实测量；
   - 「实测读取量」由 `CountingFile` 字节计数包装器统计 Parquet 引擎真正调用 `read()` 的字节数与次数。
3. **可核对**：每次查询同时执行裁剪查询和全量基线查询，结果排序后逐行比对，不一致时页面红色告警。

## 支持的筛选组合（网页只提供这些）

DNF 结构：条件组之间 **OR**，组内条件之间 **AND**。

| 列类型 | 支持操作符 |
| --- | --- |
| int / float | `= != > >= < <=`、`in`、`is null`、`is not null` |
| string | 上述全部，外加 `startswith` |

不支持的列类型（布尔、日期、嵌套等）只做保守保留，不参与裁剪。

## 四类关键场景的处理

- **全空列**：`IS NULL` 保留、`IS NOT NULL` 排除、任何值比较排除（null 不参与比较）。
- **含空值比较**：空值数单独统计；普通比较只作用于非空值，值域判定不受 null 影响，
  因为 null 与任何比较的结果都不是 true。
- **边界相等**：`>` 遇 `max == value` 仍排除（严格大于），`>=` 保留；
  `<` / `<=` 对称处理；等值命中 min/max 边界时保留（演示数据相邻分区首尾 id 相同）。
- **逻辑或**：每个 AND 组独立判定，任一组保留则文件保留；全部组排除才排除文件。

字符串 `startswith` 用双重判定：完整前缀摘要（top-K 之外的前缀不能用于排除，因此索引记录
`prefix_complete` 完整性标志）+ 字典序值域区间 `[prefix, upper)`，任一能确定不可能才排除。

## 索引与指纹

- 索引位于 `<数据目录>/.partition_index/index.json`，原子写入；
  建索引流水 `journal.jsonl` 逐文件追加，**中断后再次构建自动跳过已完成文件（续建）**。
- 双指纹：快速指纹（size + mtime_ns）秒检；SHA-256 内容指纹兜底同尺寸原样替换。
  指纹不符即标记 `stale`，查询规划时保守保留并回退实际扫描。
- 支持新增文件（`unindexed` → 增量建）、修订数据（`stale` → 重建该文件）、删除文件（条目清理）、整体重建。

## 快速开始

```bash
./run_demo.sh                 # 建 venv、装依赖、生成 48 个小分区、建索引、起网页
# 或手动：
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python scripts/generate_demo_data.py data/demo
.venv/bin/python -m partition_query.cli --data-dir data/demo index
.venv/bin/python -m partition_query.cli --data-dir data/demo serve --port 8000
```

打开 http://127.0.0.1:8000 ：

1. 「索引状态与维护」：增量建 / 续建 / 重建，显示 indexed/stale/unindexed 计数；
2. 「构造筛选条件」：动态下拉只列出受支持的 列类型 × 操作符 组合，可添加 AND 条件与 OR 组；
3. 查询结果：顶部显示核对结论与估算/实测读取汇总，下表逐文件给出决定理由（可展开到每个条件）、
   索引状态、实测字节、read 次数、命中行数；「统计详情」弹窗展示该文件的值域、空值数与前缀摘要。

## CLI

```bash
# 增量建索引（中断后重跑同一命令即续建）；--rebuild 为全量重建
.venv/bin/python -m partition_query.cli --data-dir data/demo index [--rebuild]

# 查询（DNF JSON：外层 OR 组，组内 AND 条件）
.venv/bin/python -m partition_query.cli --data-dir data/demo query --summary \
  --filter-json '[[{"column":"order_id","op":"eq","value":1000}],
                   [{"column":"sku","op":"startswith","value":"SKU-C"}]]'
```

## 测试

```bash
.venv/bin/python -m pytest tests/ -q
```

66 个测试覆盖：四类关键场景的裁剪判定、所有支持操作符的「裁剪 vs 基线」等价性、
指纹过期回退、新增/修订/删除/重建、journal 断点续建、字节计数包装器、Web API 与非法组合拒绝。

## 目录结构

```
partition_query/
  filters.py      # 筛选模型（DNF）、支持组合白名单、Arrow 表达式生成
  stats.py        # 列统计：值域、空值数、全空标志、字符串前缀摘要+完整性标志
  index_store.py  # 索引持久化、双指纹、journal 续建、过期检测
  pruning.py      # 保守裁剪判定（每个条件产出可展示的中文理由）
  scanner.py      # CountingFile 实测字节、扫描、结果等价核对
  query.py        # 裁剪规划 + 裁剪查询 + 全量基线查询编排
  webapp.py       # Flask 页面与 JSON API
  cli.py          # index / query / serve 命令行
scripts/generate_demo_data.py   # 48 分区 × 50 行演示数据（含全空/空值/边界/OR 场景）
tests/            # pytest 测试
templates/index.html
```
