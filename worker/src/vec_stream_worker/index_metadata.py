"""索引配置元数据。

worker 在启动时把当前 embedding/chunk/vector backend 写入 PG 的 index_metadata;
rag 启动时读取并校验。这样换模型、换维度或切分参数后,不会在旧索引上悄悄用新 query
向量检索,而是快速失败并提示走蓝绿重建。
"""

from __future__ import annotations

import json
from typing import Any

import psycopg


def metadata_name_from_cfg(cfg) -> str:
    if cfg.vector_backend == "qdrant":
        return f"qdrant:{cfg.qdrant_collection}"
    return "pgvector:doc_vectors"


def metadata_from_cfg(cfg) -> dict[str, Any]:
    return {
        "embed_provider": cfg.embed_provider,
        "embed_model": cfg.embed_model,
        "embed_dim": cfg.embed_dim,
        "embed_service_url": cfg.embed_service_url,
        "embed_openai_base_url": cfg.embed_openai_base_url,
        "chunk_size": cfg.chunk_size,
        "chunk_overlap": cfg.chunk_overlap,
        "vector_backend": cfg.vector_backend,
        "qdrant_collection": cfg.qdrant_collection,
    }


def upsert_index_metadata(dsn: str, metadata: dict[str, Any], name: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO index_metadata(name, metadata, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (name) DO UPDATE
              SET metadata = EXCLUDED.metadata, updated_at = now()
            """,
            (name, json.dumps(metadata, ensure_ascii=False, sort_keys=True)),
        )
