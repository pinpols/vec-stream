"""RAG 服务(阶段 2):
  POST /search — 语义搜索:embed → tenant/status 过滤召回 → 可选 rerank
  POST /ask    — RAG 问答:召回 → rerank → Claude 生成带 [n] 引用
"""
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field
from sentence_transformers import SentenceTransformer

from .llm import generate_answer
from .rerank import Reranker

log = logging.getLogger("rag")

PG_DSN = os.getenv("PG_DSN", "postgresql://vecstream:vecstream@localhost:5433/vecstream")
EMBED_MODEL = os.getenv("EMBED_MODEL", "BAAI/bge-small-zh-v1.5")
RERANK_MODEL = os.getenv("RERANK_MODEL", "BAAI/bge-reranker-base")
RERANK_ENABLED = os.getenv("RERANK_ENABLED", "true").lower() == "true"
# 向量库后端:pgvector(默认)| qdrant —— 与 worker 的 VECTOR_BACKEND 保持一致
VECTOR_BACKEND = os.getenv("VECTOR_BACKEND", "pgvector")
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "doc_vectors")
# bge 系列约定:检索 query 加指令前缀(passage 不加)
QUERY_PREFIX = "为这个句子生成表示以用于检索相关文章:"

state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    state["model"] = SentenceTransformer(EMBED_MODEL)
    state["reranker"] = Reranker(RERANK_MODEL) if RERANK_ENABLED else None
    if VECTOR_BACKEND == "qdrant":
        from qdrant_client import QdrantClient

        state["qdrant"] = QdrantClient(url=QDRANT_URL)
    else:
        # FastAPI 同步端点跑在线程池,psycopg 连接非线程安全 → 必须用连接池
        state["pool"] = ConnectionPool(
            PG_DSN, min_size=1, max_size=8, open=True,
            kwargs={"autocommit": True},
        )
    yield
    if VECTOR_BACKEND == "qdrant":
        state["qdrant"].close()
    else:
        state["pool"].close()


app = FastAPI(title="vecstream-rag", lifespan=lifespan)


class SearchRequest(BaseModel):
    query: str
    tenant_id: str = "default"
    top_k: int = Field(default=5, ge=1, le=50)
    status: str | None = None  # 按 metadata.status 过滤,如 published
    rerank: bool = False


class SearchHit(BaseModel):
    content: str
    score: float
    rerank_score: float | None = None
    source_table: str
    source_pk: str
    chunk_index: int
    metadata: dict | None


class AskRequest(BaseModel):
    query: str
    tenant_id: str = "default"
    top_k: int = Field(default=12, ge=1, le=50)   # 召回数
    top_n: int = Field(default=4, ge=1, le=10)    # rerank 后喂给模型的数
    status: str | None = None


class Source(BaseModel):
    n: int
    source_pk: str
    title: str | None
    content: str
    score: float
    rerank_score: float | None = None


class AskResponse(BaseModel):
    answer: str
    sources: list[Source]
    model: str
    usage: dict


def retrieve(query: str, tenant_id: str, top_k: int, status: str | None) -> list[dict]:
    """向量召回,多租过滤必须带 tenant_id(DESIGN.md §3.5)。"""
    qvec = state["model"].encode(QUERY_PREFIX + query, normalize_embeddings=True).tolist()
    if VECTOR_BACKEND == "qdrant":
        return _retrieve_qdrant(qvec, tenant_id, top_k, status)
    return _retrieve_pgvector(qvec, tenant_id, top_k, status)


def _retrieve_pgvector(qvec: list[float], tenant_id: str, top_k: int, status: str | None) -> list[dict]:
    vec = str(qvec)
    sql = """
        SELECT content, 1 - (embedding <=> %(v)s::vector) AS score,
               source_table, source_pk, chunk_index, metadata
        FROM doc_vectors
        WHERE tenant_id = %(tenant)s
          AND (%(status)s::text IS NULL OR metadata->>'status' = %(status)s)
        ORDER BY embedding <=> %(v)s::vector
        LIMIT %(k)s
    """
    with state["pool"].connection() as conn, conn.cursor() as cur:
        cur.execute(sql, {"v": vec, "tenant": tenant_id, "status": status, "k": top_k})
        rows = cur.fetchall()
    return [
        {
            "content": r[0], "score": round(float(r[1]), 4), "source_table": r[2],
            "source_pk": r[3], "chunk_index": r[4], "metadata": r[5],
        }
        for r in rows
    ]


def _retrieve_qdrant(qvec: list[float], tenant_id: str, top_k: int, status: str | None) -> list[dict]:
    """Qdrant 原生 payload 过滤 + HNSW 召回(先过滤后召回)。"""
    from qdrant_client import models

    must = [models.FieldCondition(key="tenant_id", match=models.MatchValue(value=tenant_id))]
    if status is not None:
        must.append(models.FieldCondition(key="status", match=models.MatchValue(value=status)))
    result = state["qdrant"].query_points(
        collection_name=QDRANT_COLLECTION,
        query=qvec,
        query_filter=models.Filter(must=must),
        limit=top_k,
        with_payload=True,
    )
    return [
        {
            "content": p.payload["content"],
            "score": round(float(p.score), 4),  # cosine 相似度,与 pgvector 同语义
            "source_table": p.payload["source_table"],
            "source_pk": p.payload["source_pk"],
            "chunk_index": p.payload["chunk_index"],
            "metadata": {"status": p.payload.get("status"), "title": p.payload.get("title")},
        }
        for p in result.points
    ]


def apply_rerank(query: str, hits: list[dict], top_n: int) -> list[dict]:
    """有 reranker 用交叉编码器重排;没有则按向量分数截断。"""
    if state.get("reranker") and hits:
        scores = state["reranker"].rerank(query, [h["content"] for h in hits])
        for h, s in zip(hits, scores):
            h["rerank_score"] = round(s, 4)
        hits = sorted(hits, key=lambda h: h["rerank_score"], reverse=True)
    return hits[:top_n]


@app.get("/healthz")
def healthz():
    return {"status": "ok", "rerank": RERANK_ENABLED, "backend": VECTOR_BACKEND}


@app.get("/stats")
def stats():
    """向量库大盘:总量 + 按表/租户分布(worker 侧指标见其 :9100/metrics)。"""
    if VECTOR_BACKEND == "qdrant":
        from qdrant_client import models  # noqa: F401

        total = state["qdrant"].count(QDRANT_COLLECTION).count
        by_table = {
            str(hit.value): hit.count
            for hit in state["qdrant"].facet(
                collection_name=QDRANT_COLLECTION, key="source_table"
            ).hits
        }
        return {"backend": "qdrant", "total_vectors": total, "by_table": by_table}
    with state["pool"].connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT source_table, tenant_id, count(*) FROM doc_vectors "
            "GROUP BY source_table, tenant_id ORDER BY 1, 2"
        )
        rows = cur.fetchall()
    return {
        "backend": "pgvector",
        "total_vectors": sum(r[2] for r in rows),
        "by_table": [
            {"table": r[0], "tenant": r[1], "vectors": r[2]} for r in rows
        ],
    }


@app.post("/search", response_model=list[SearchHit])
def search(req: SearchRequest):
    hits = retrieve(req.query, req.tenant_id, req.top_k, req.status)
    if req.rerank:
        hits = apply_rerank(req.query, hits, req.top_k)
    return [SearchHit(**h) for h in hits]


@app.post("/ask", response_model=AskResponse)
def ask(req: AskRequest):
    if not os.getenv("ANTHROPIC_API_KEY"):
        raise HTTPException(503, "ANTHROPIC_API_KEY 未配置,/ask 不可用(/search 不受影响)")
    hits = retrieve(req.query, req.tenant_id, req.top_k, req.status)
    if not hits:
        return AskResponse(
            answer="知识库中没有相关信息。", sources=[], model="", usage={}
        )
    hits = apply_rerank(req.query, hits, req.top_n)
    sources = [
        {
            "n": i + 1,
            "source_pk": h["source_pk"],
            "title": (h.get("metadata") or {}).get("title"),
            "content": h["content"],
            "score": h["score"],
            "rerank_score": h.get("rerank_score"),
        }
        for i, h in enumerate(hits)
    ]
    result = generate_answer(req.query, sources)
    return AskResponse(
        answer=result["answer"],
        sources=[Source(**s) for s in sources],
        model=result["model"],
        usage=result["usage"],
    )
