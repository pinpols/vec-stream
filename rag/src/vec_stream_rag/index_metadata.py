"""RAG 启动期索引配置一致性校验。"""

from __future__ import annotations

import os
from typing import Any

import psycopg

BASE_CHECK_KEYS = (
    "embed_model",
    "embed_dim",
    "chunk_size",
    "chunk_overlap",
    "vector_backend",
)


def expected_metadata() -> dict[str, Any]:
    return {
        "embed_provider": os.getenv("EMBED_PROVIDER", "local"),
        "embed_model": os.getenv("EMBED_MODEL", "BAAI/bge-small-zh-v1.5"),
        "embed_dim": int(os.getenv("EMBED_DIM", "512")),
        "embed_service_url": os.getenv("EMBED_SERVICE_URL", ""),
        "embed_openai_base_url": os.getenv("EMBED_OPENAI_BASE_URL", ""),
        "chunk_size": int(os.getenv("CHUNK_SIZE", "400")),
        "chunk_overlap": int(os.getenv("CHUNK_OVERLAP", "50")),
        "vector_backend": os.getenv("VECTOR_BACKEND", "pgvector"),
        "qdrant_collection": os.getenv("QDRANT_COLLECTION", "doc_vectors"),
    }


def metadata_name(metadata: dict[str, Any]) -> str:
    if metadata["vector_backend"] == "qdrant":
        return f"qdrant:{metadata['qdrant_collection']}"
    return "pgvector:doc_vectors"


def _check_keys(expected: dict[str, Any]) -> tuple[str, ...]:
    keys = list(BASE_CHECK_KEYS)
    if expected.get("vector_backend") == "qdrant":
        keys.append("qdrant_collection")
    return tuple(keys)


def fetch_index_metadata(dsn: str, name: str) -> dict[str, Any] | None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        row = conn.execute(
            "SELECT metadata FROM index_metadata WHERE name = %s",
            (name,),
        ).fetchone()
    return row[0] if row else None


def validate_index_metadata(actual: dict[str, Any] | None, expected: dict[str, Any]) -> None:
    if actual is None:
        raise RuntimeError(
            f"index_metadata 缺少 {metadata_name(expected)} 记录:"
            "请先启动对应 worker 写入索引配置,或确认旧库已应用 02-security.sh"
        )
    mismatches = []
    for key in _check_keys(expected):
        if actual.get(key) != expected.get(key):
            mismatches.append(f"{key}: index={actual.get(key)!r}, rag={expected.get(key)!r}")
    if mismatches:
        raise RuntimeError(
            "RAG 配置与已构建索引不一致,请保持 worker/rag embedding 配置一致或走蓝绿重建:\n  - "
            + "\n  - ".join(mismatches)
        )


def check_index_metadata(dsn: str) -> None:
    expected = expected_metadata()
    validate_index_metadata(fetch_index_metadata(dsn, metadata_name(expected)), expected)
