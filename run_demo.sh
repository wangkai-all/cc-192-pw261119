#!/usr/bin/env bash
# 一键准备：建虚拟环境、装依赖、生成 48+1 个小分区、建索引、起网页。
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi
.venv/bin/pip install -q -r requirements.txt

DATA_DIR="${DATA_DIR:-data/demo}"
if [ ! -d "$DATA_DIR" ] || [ -z "$(find "$DATA_DIR" -name '*.parquet' -print -quit 2>/dev/null)" ]; then
  .venv/bin/python scripts/generate_demo_data.py "$DATA_DIR"
fi
.venv/bin/python -m partition_query.cli --data-dir "$DATA_DIR" index
PORT="${PORT:-8000}"
echo "网页将在 http://127.0.0.1:${PORT} 打开（Ctrl+C 退出）"
exec .venv/bin/python -m partition_query.cli --data-dir "$DATA_DIR" serve --port "$PORT"
