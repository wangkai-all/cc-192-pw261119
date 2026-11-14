"""命令行入口：建索引 / 续建 / 重建 / 查询。

示例：
  python -m partition_query.cli --data-dir data/demo index
  python -m partition_query.cli --data-dir data/demo index --rebuild
  python -m partition_query.cli --data-dir data/demo query \
      --filter-json '[[{"column":"order_id","op":"eq","value":1000}]]'
  python -m partition_query.cli --data-dir data/demo serve --port 8000
"""

from __future__ import annotations

import argparse
import json
import sys

from .filters import parse_groups
from .index_store import IndexStore
from .query import QueryRunner


def _cmd_index(args: argparse.Namespace) -> int:
    store = IndexStore(args.data_dir)
    store.load()
    progress = store.build_or_resume(rebuild=args.rebuild)
    print(
        f"索引完成：处理 {progress.processed}/{progress.total}，"
        f"续建跳过 {progress.skipped}，中断标记={progress.interrupted}"
    )
    return 0


def _cmd_query(args: argparse.Namespace) -> int:
    store = IndexStore(args.data_dir)
    if not store.load():
        print("索引不存在，请先运行 index 子命令", file=sys.stderr)
        return 2
    kinds = store.schema_summary()["column_kinds"]
    groups = parse_groups(json.loads(args.filter_json), kinds)
    report = QueryRunner(store).run(groups)
    payload = report.to_dict()
    if args.summary:
        print(
            f"保留 {payload['kept_count']} / 排除 {payload['excluded_count']} 个文件；"
            f"命中 {payload['matched_rows']} 行（基线 {payload['baseline_rows']} 行，"
            f"核对={'通过' if payload['equivalent'] else '失败'}：{payload['equivalence_detail']}）"
        )
        print(
            f"估算读取 {payload['estimated_read_bytes']} B（注意：估算，非测量）；"
            f"实测读取 {payload['measured_read_bytes']} B；"
            f"全量基线实测 {payload['baseline_measured_read_bytes']} B"
        )
        for item in payload["files"]:
            plan, scan = item["plan"], item["scan"]
            print(
                f"  [{plan['action']:7s}] {plan['path']} "
              f"实测 {scan['measured_bytes']:>7d} B / {scan['read_calls']} 次 read — {plan['reason']}"
            )
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload["equivalent"] else 1


def _cmd_serve(args: argparse.Namespace) -> int:
    from .webapp import create_app

    app = create_app(args.data_dir)
    app.run(host=args.host, port=args.port, debug=False)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="分区裁剪查询")
    parser.add_argument("--data-dir", required=True, help="Parquet 分区数据目录")
    sub = parser.add_subparsers(dest="command", required=True)

    p_index = sub.add_parser("index", help="增量建索引 / 中断续建")
    p_index.add_argument("--rebuild", action="store_true", help="丢弃旧索引全量重建")
    p_index.set_defaults(func=_cmd_index)

    p_query = sub.add_parser("query", help="执行裁剪查询并与全量基线核对")
    p_query.add_argument(
        "--filter-json",
        required=True,
        help='DNF 条件 JSON，例如 [[{"column":"x","op":"gt","value":1}]]',
    )
    p_query.add_argument("--summary", action="store_true", help="输出人类可读摘要")
    p_query.set_defaults(func=_cmd_query)

    p_serve = sub.add_parser("serve", help="启动网页")
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=8000)
    p_serve.set_defaults(func=_cmd_serve)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
