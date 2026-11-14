"""索引持久化、内容指纹与断点续建。

数据目录布局：
  <data_dir>/*.parquet        分区数据文件
  <data_dir>/.partition_index/index.json   索引快照（原子写入）
  <data_dir>/.partition_index/journal.jsonl  建索引流水（逐文件追加，支持中断续建）

指纹策略（双保险）：
- 快速指纹：mtime_ns + size，用于秒级发现变更；
- 内容指纹：整文件 SHA-256，建索引时计算并落盘。
查询时快速指纹不符即判定过期；快速指纹相同但内容指纹不符（文件被同尺寸原样替换）
同样判定过期，过期文件一律回退实际扫描。
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

from .stats import FileStat, build_file_stat

INDEX_DIR_NAME = ".partition_index"
INDEX_FILE_NAME = "index.json"
JOURNAL_FILE_NAME = "journal.jsonl"
INDEX_VERSION = 1
CHUNK_SIZE = 1024 * 1024


def quick_fingerprint(path: str | Path) -> dict[str, Any]:
    st = os.stat(path)
    return {"size": st.st_size, "mtime_ns": st.st_mtime_ns}


def content_fingerprint(path: str | Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def list_parquet_files(data_dir: str | Path) -> list[str]:
    root = Path(data_dir).resolve()
    return sorted(str(p.resolve()) for p in root.rglob("*.parquet") if p.is_file())


@dataclass
class IndexEntry:
    quick: dict[str, Any]
    sha256: str
    stat: FileStat

    def to_dict(self) -> dict[str, Any]:
        return {
            "quick": self.quick,
            "sha256": self.sha256,
            "stat": self.stat.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "IndexEntry":
        return cls(
            quick=data["quick"],
            sha256=data["sha256"],
            stat=FileStat.from_dict(data["stat"]),
        )


@dataclass
class BuildProgress:
    """一次建索引过程的进度信息（用于网页展示与中断续建）。"""

    total: int = 0
    processed: int = 0
    skipped: int = 0
    current: str | None = None
    started_at: float | None = None
    finished_at: float | None = None
    interrupted: bool = False
    events: list[dict[str, Any]] = None

    def __post_init__(self) -> None:
        if self.events is None:
            self.events = []


class IndexStore:
    def __init__(self, data_dir: str | Path):
        self.data_dir = Path(data_dir).resolve()
        self.index_dir = self.data_dir / INDEX_DIR_NAME
        self.index_path = self.index_dir / INDEX_FILE_NAME
        self.journal_path = self.index_dir / JOURNAL_FILE_NAME
        self.entries: dict[str, IndexEntry] = {}
        self.built_at: float | None = None

    # ---------- 加载 / 保存 ----------
    def load(self) -> bool:
        if not self.index_path.exists():
            return False
        with open(self.index_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if payload.get("version") != INDEX_VERSION:
            raise ValueError(
                f"索引版本 {payload.get('version')} 与当前 {INDEX_VERSION} 不符，需要重建"
            )
        self.entries = {
            path: IndexEntry.from_dict(data)
            for path, data in payload["entries"].items()
        }
        self.built_at = payload.get("built_at")
        return True

    def _atomic_save(self) -> None:
        self.index_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": INDEX_VERSION,
            "built_at": time.time(),
            "data_dir": str(self.data_dir),
            "entries": {path: entry.to_dict() for path, entry in self.entries.items()},
        }
        fd, tmp_name = tempfile.mkstemp(dir=str(self.index_dir), suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False)
            os.replace(tmp_name, self.index_path)
        except BaseException:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
            raise

    # ---------- 过期检测 ----------
    def file_state(self, path: str) -> str:
        """返回 indexed / missing / unindexed / stale。"""
        if path not in self.entries:
            return "unindexed"
        try:
            quick = quick_fingerprint(path)
        except FileNotFoundError:
            return "missing"
        entry = self.entries[path]
        if quick != entry.quick:
            return "stale"
        # 快速指纹一致时再核对内容指纹（同尺寸替换检测）
        if content_fingerprint(path) != entry.sha256:
            return "stale"
        return "indexed"

    def refresh_states(self, paths: Iterable[str]) -> dict[str, str]:
        return {path: self.file_state(path) for path in paths}

    # ---------- journal ----------
    def _append_journal(self, event: dict[str, Any]) -> None:
        self.index_dir.mkdir(parents=True, exist_ok=True)
        with open(self.journal_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=False) + "\n")

    def _journal_done_paths(self) -> set[str]:
        """读取 journal 中已完成的文件（中断后这些文件无需重建）。"""
        done: set[str] = set()
        if not self.journal_path.exists():
            return done
        with open(self.journal_path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if event.get("event") == "file_done":
                    done.add(event["path"])
        return done

    def _clear_journal(self) -> None:
        if self.journal_path.exists():
            os.unlink(self.journal_path)

    # ---------- 建索引 / 续建 / 重建 ----------
    def build_or_resume(
        self,
        progress: BuildProgress | None = None,
        rebuild: bool = False,
        stat_builder: Callable[[str], FileStat] = build_file_stat,
    ) -> BuildProgress:
        """增量建索引；rebuild=True 时丢弃旧索引与 journal 全量重建。

        中断后续跑：journal 里标记 file_done 的文件直接跳过，
        未完成（只有 file_start 或根本没开始）的文件重新统计。
        """
        progress = progress or BuildProgress()
        if rebuild:
            self.entries = {}
            self._clear_journal()
            if self.index_path.exists():
                os.unlink(self.index_path)
        elif not self.entries and self.index_path.exists():
            # 全新 store 对象直接续建时，先载入已落盘的快照（CLI/Web 通常已显式 load）
            self.load()

        paths = list_parquet_files(self.data_dir)
        done_in_journal = self._journal_done_paths()
        progress.total = len(paths)
        progress.started_at = progress.started_at or time.time()

        # 先清理已不存在的文件条目
        for gone in set(self.entries) - set(paths):
            del self.entries[gone]

        for path in paths:
            progress.current = path
            state = self.file_state(path) if path in self.entries else "unindexed"

            # journal 已完成且当前文件未再变更：续建时跳过
            if (
                not rebuild
                and path in done_in_journal
                and state in ("indexed",)
            ):
                progress.skipped += 1
                progress.processed += 1
                progress.events.append({"event": "skip_resumed", "path": path})
                continue

            self._append_journal({"event": "file_start", "path": path, "at": time.time()})
            try:
                file_stat = stat_builder(path)
                quick = quick_fingerprint(path)
                sha = content_fingerprint(path)
            except BaseException as exc:  # 中断/出错：保留 journal，下次续建
                progress.interrupted = True
                progress.events.append(
                    {"event": "error", "path": path, "error": repr(exc)}
                )
                raise
            self.entries[path] = IndexEntry(quick=quick, sha256=sha, stat=file_stat)
            self._append_journal(
                {"event": "file_done", "path": path, "at": time.time()}
            )
            # 每个文件完成后落一次快照：即使后续中断，已完成部分也可用
            self._atomic_save()
            progress.processed += 1
            progress.events.append(
                {"event": "indexed", "path": path, "rows": file_stat.row_count}
            )

        self._atomic_save()
        self._clear_journal()
        self.built_at = time.time()
        progress.finished_at = time.time()
        progress.current = None
        return progress

    # ---------- 查询侧 ----------
    def get_file_stat(self, path: str) -> tuple[FileStat | None, str]:
        """返回 (统计, 状态)。状态非 indexed 时调用方必须回退实际扫描。"""
        state = self.file_state(path)
        if state == "indexed":
            return self.entries[path].stat, state
        return None, state

    def schema_summary(self) -> dict[str, Any]:
        """汇总所有已索引文件的列 kind（用于网页只展示受支持的筛选组合）。"""
        kinds: dict[str, str] = {}
        for entry in self.entries.values():
            for name, col in entry.stat.columns.items():
                if col.kind is None:
                    continue
                if name in kinds and kinds[name] != col.kind:
                    kinds[name] = "mixed"
                else:
                    kinds[name] = col.kind
        return {
            "column_kinds": kinds,
            "file_count": len(self.entries),
            "built_at": self.built_at,
        }
