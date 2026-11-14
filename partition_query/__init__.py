"""分区裁剪查询项目。

模块划分：
- filters: 项目明确支持的筛选条件模型（DNF：组内 AND、组间 OR）
- stats:   单个 Parquet 文件的列统计（值域、空值数、字符串前缀摘要）
- index_store: 索引持久化、内容指纹、journal 断点续建
- pruning: 保守的裁剪判定（宁可保留，不可漏结果）
- scanner: 实际扫描与真实读取量测量
- query:   裁剪查询 + 全量基线核对
- webapp:  Flask 页面
"""

__all__ = [
    "filters",
    "stats",
    "index_store",
    "pruning",
    "scanner",
    "query",
    "webapp",
]
