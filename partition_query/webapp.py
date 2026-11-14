"""Flask Web 应用：索引管理、筛选查询、逐文件理由与真实读取量对照。"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template, request

from .filters import OPERATOR_LABELS, SUPPORTED_OPS_BY_KIND, parse_groups
from .index_store import IndexStore, list_parquet_files
from .query import QueryRunner


def create_app(data_dir: str | Path) -> Flask:
    data_dir = str(Path(data_dir).resolve())
    # 模板与静态资源位于仓库根目录（包目录之外），显式指定
    root = Path(__file__).resolve().parent.parent
    app = Flask(
        __name__,
        template_folder=str(root / "templates"),
        static_folder=str(root / "static"),
    )
    app.config["DATA_DIR"] = data_dir

    def get_store() -> IndexStore:
        store = IndexStore(data_dir)
        store.load()
        return store

    def _status(store: IndexStore) -> dict[str, Any]:
        paths = list_parquet_files(data_dir)
        states = store.refresh_states(paths)
        counts = {"indexed": 0, "stale": 0, "unindexed": 0, "missing": 0}
        for state in states.values():
            counts[state] = counts.get(state, 0) + 1
        summary = store.schema_summary()
        return {
            "data_dir": data_dir,
            "parquet_files": len(paths),
            "states": states,
            "counts": counts,
            "column_kinds": summary["column_kinds"],
            "built_at": summary["built_at"],
            "index_exists": store.index_path.exists(),
            "journal_exists": store.journal_path.exists(),
            "supported_ops": {
                kind: sorted(ops) for kind, ops in SUPPORTED_OPS_BY_KIND.items()
            },
            "operator_labels": OPERATOR_LABELS,
        }

    @app.get("/")
    def index() -> str:
        return render_template("index.html")

    @app.get("/api/status")
    def api_status() -> Any:
        return jsonify(_status(get_store()))

    @app.post("/api/index/build")
    def api_index_build() -> Any:
        """增量建索引 / 中断续建（journal 中已完成的文件自动跳过）。"""
        rebuild = bool(request.args.get("rebuild"))
        store = get_store()
        progress = store.build_or_resume(rebuild=rebuild)
        return jsonify(
            {
                "processed": progress.processed,
                "skipped": progress.skipped,
                "total": progress.total,
                "interrupted": progress.interrupted,
                "events": progress.events[-200:],
                "status": _status(store),
            }
        )

    @app.get("/api/files/<path:rel_path>")
    def api_file_detail(rel_path: str) -> Any:
        """查看单个文件的索引统计（值域、空值、前缀摘要）与指纹状态。"""
        store = get_store()
        full = os.path.join(data_dir, rel_path)
        state = store.file_state(full)
        entry = store.entries.get(full)
        if entry is None:
            return jsonify({"path": rel_path, "state": state, "stat": None})
        return jsonify(
            {
                "path": rel_path,
                "state": state,
                "fingerprint": {"quick": entry.quick, "sha256": entry.sha256},
                "stat": entry.stat.to_dict(),
            }
        )

    @app.post("/api/query")
    def api_query() -> Any:
        payload = request.get_json(silent=True) or {}
        store = get_store()
        kinds = store.schema_summary()["column_kinds"]
        try:
            groups = parse_groups(payload.get("filters"), kinds)
        except ValueError as exc:
            return jsonify({"error": f"筛选条件不被项目支持: {exc}"}), 400

        columns = payload.get("columns")
        if columns is not None and (
            not isinstance(columns, list) or not all(isinstance(c, str) for c in columns)
        ):
            return jsonify({"error": "columns 必须是字符串列表或省略"}), 400

        try:
            report = QueryRunner(store).run(groups, columns=columns)
        except Exception as exc:  # 扫描失败如实上报，不伪装成功
            return jsonify({"error": f"查询执行失败: {exc!r}"}), 500
        result = report.to_dict()
        result["filters"] = payload.get("filters")
        return jsonify(result)

    return app
