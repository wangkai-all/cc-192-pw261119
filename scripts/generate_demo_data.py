"""生成分区 Parquet 演示数据。

设计目标：用大量小分区让「裁剪 vs 全量扫描」的读取差异肉眼可见，
并刻意覆盖四类关键场景：
- 全空列：feedback 列在部分分区整列为空；
- 含空值比较：amount / sku 在各分区随机注入空值；
- 边界相等：order_id 在相邻分区首尾相同（100 的倍数重复）；
- 逻辑或：status / region / sku 前缀等多条件 OR 查询。
"""

from __future__ import annotations

import argparse
import random
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = pa.schema([
    pa.field("order_id", pa.int64()),
    pa.field("amount", pa.float64()),
    pa.field("status", pa.string()),
    pa.field("region", pa.string()),
    pa.field("sku", pa.string()),
    pa.field("feedback", pa.string()),  # 部分分区全空
    pa.field("created_day", pa.int64()),
])

STATUS_VALUES = ["paid", "pending", "cancelled", "refunded"]
REGION_PREFIXES = ["east", "west", "north", "south"]
SKU_PREFIXES = ["SKU-A", "SKU-B", "SKU-C"]

# 48 个分区，每分区 50 行，共 2400 行；分区按 created_day 划分
NUM_PARTITIONS = 48
ROWS_PER_PARTITION = 50
# order_id 相邻分区首尾相接且边界值重复：part i 的范围 [i*100, i*100+100]
ID_SPAN = 100


def _generate_partition(part_idx: int, rng: random.Random) -> pa.Table:
    base = part_idx * ID_SPAN
    rows: dict[str, list] = {name: [] for name in SCHEMA.names}
    for row_idx in range(ROWS_PER_PARTITION):
        # 边界相等：第一个分区最后一行与第二个分区第一行都取到 base 边界值
        order_id = base + (row_idx * ID_SPAN // ROWS_PER_PARTITION)
        if part_idx > 0 and row_idx == 0:
            order_id = base  # 与上一分区最大值相等

        # 约 8% 空值
        amount = None if rng.random() < 0.08 else round(rng.uniform(5, 950), 2)
        status = rng.choice(STATUS_VALUES)
        region = rng.choice(REGION_PREFIXES) + "-" + rng.choice(["hq", "branch"])
        sku = rng.choice(SKU_PREFIXES) + "-" + str(rng.randint(100, 999))
        if rng.random() < 0.05:
            sku = None
        created_day = part_idx
        rows["order_id"].append(order_id)
        rows["amount"].append(amount)
        rows["status"].append(status)
        rows["region"].append(region)
        rows["sku"].append(sku)
        rows["created_day"].append(created_day)

    # feedback：每 6 个分区里有 1 个整列全空；其余分区约 30% 非空
    if part_idx % 6 == 5:
        rows["feedback"] = [None] * ROWS_PER_PARTITION
    else:
        rows["feedback"] = [
            None if rng.random() < 0.7 else f"note-{rng.randint(1, 999)}"
            for _ in range(ROWS_PER_PARTITION)
        ]

    arrays = [pa.array(rows[name], type=SCHEMA.field(name).type) for name in SCHEMA.names]
    return pa.Table.from_arrays(arrays, schema=SCHEMA)


def generate(data_dir: Path, seed: int = 20261114) -> int:
    rng = random.Random(seed)
    data_dir.mkdir(parents=True, exist_ok=True)
    total = 0
    for part_idx in range(NUM_PARTITIONS):
        table = _generate_partition(part_idx, rng)
        part_dir = data_dir / f"created_day={part_idx:03d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        path = part_dir / f"part-{part_idx:03d}.parquet"
        pq.write_table(table, path, compression="snappy")
        total += table.num_rows
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="生成分区 Parquet 演示数据")
    parser.add_argument("data_dir", help="输出目录（新建子目录存放分区）")
    parser.add_argument("--seed", type=int, default=20261114)
    args = parser.parse_args()
    total = generate(Path(args.data_dir), seed=args.seed)
    print(
        f"已生成 {NUM_PARTITIONS} 个分区、{total} 行数据，目录: {args.data_dir}"
    )


if __name__ == "__main__":
    main()
