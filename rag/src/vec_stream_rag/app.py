"""RAG 服务(阶段 2):
  POST /search — 语义搜索:embed → tenant/status 过滤召回 → 可选 rerank
  POST /ask    — RAG 问答:召回 → rerank → Claude 生成带 [n] 引用
"""
import json
import logging
import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field
from sentence_transformers import SentenceTransformer

from .llm import active_provider, api_key_env, generate_answer, llm_available
from .rerank import Reranker

log = logging.getLogger("rag")

# rag 用最小权限只读角色 vs_rag(不 BYPASSRLS):优先 RAG_PG_DSN,回退 PG_DSN。
# 默认连 vs_rag,查询前必须 SET app.tenant 否则 RLS 命中 0 行。
PG_DSN = os.getenv(
    "RAG_PG_DSN",
    os.getenv("PG_DSN", "postgresql://vs_rag:vs_rag@localhost:5433/vec_stream"),
)
EMBED_MODEL = os.getenv("EMBED_MODEL", "BAAI/bge-small-zh-v1.5")
RERANK_MODEL = os.getenv("RERANK_MODEL", "BAAI/bge-reranker-base")
RERANK_ENABLED = os.getenv("RERANK_ENABLED", "true").lower() == "true"
# 向量库后端:pgvector(默认)| qdrant —— 与 worker 的 VECTOR_BACKEND 保持一致
VECTOR_BACKEND = os.getenv("VECTOR_BACKEND", "pgvector")
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "doc_vectors")
# bge 系列约定:检索 query 加指令前缀(passage 不加)
QUERY_PREFIX = "为这个句子生成表示以用于检索相关文章:"
# M2:embedding 拆服务。非空则 query 向量化走 embed-service,本进程不再加载 embedding 模型
EMBED_SERVICE_URL = os.getenv("EMBED_SERVICE_URL", "")
# embedding 后端可插拔:local(进程内)| openai(OpenAI 兼容 embeddings)。须与 worker 写入侧一致
EMBED_PROVIDER = os.getenv("EMBED_PROVIDER", "local")
EMBED_OPENAI_BASE_URL = os.getenv("EMBED_OPENAI_BASE_URL", "")
# 用远程 embedding(service 或 openai)时,本进程不加载本地 embedding 模型
_USE_LOCAL_EMBED = not EMBED_SERVICE_URL and EMBED_PROVIDER != "openai"

state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 用远程 embedding(service / openai)时不在本进程加载 embedding 模型(解耦省内存)
    state["model"] = SentenceTransformer(EMBED_MODEL) if _USE_LOCAL_EMBED else None
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


app = FastAPI(title="vec-stream-rag", lifespan=lifespan)


def _load_api_keys() -> dict[str, str]:
    """从环境变量 RAG_API_KEYS 读 JSON:{"<api-key>": "<tenant_id>"}。
    每次请求时调用以便测试可 monkeypatch env(数据量小,无性能顾虑)。"""
    raw = os.getenv("RAG_API_KEYS", "{}")
    try:
        keys = json.loads(raw)
    except json.JSONDecodeError:
        log.error("RAG_API_KEYS 不是合法 JSON,鉴权将全部拒绝")
        return {}
    if not isinstance(keys, dict):
        log.error("RAG_API_KEYS 必须是 JSON 对象 {api-key: tenant_id}")
        return {}
    return {str(k): str(v) for k, v in keys.items()}


def require_tenant(x_api_key: str | None = Header(default=None)) -> str:
    """API key 鉴权依赖:X-API-Key → tenant_id;无效/缺失 → 401。
    返回认证后的 tenant_id(token 即租户,调用方不可自选)。"""
    keys = _load_api_keys()
    tenant = keys.get(x_api_key) if x_api_key else None
    if not tenant:
        raise HTTPException(status_code=401, detail="无效或缺失的 X-API-Key")
    return tenant


class SearchRequest(BaseModel):
    query: str
    # tenant_id 来自鉴权 token,不信请求体;此字段保留兼容但被忽略/覆盖。
    tenant_id: str | None = Field(default=None, deprecated=True)
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
    # tenant_id 来自鉴权 token,不信请求体;此字段保留兼容但被忽略/覆盖。
    tenant_id: str | None = Field(default=None, deprecated=True)
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


def embed_query(query: str) -> list[float]:
    """query 向量化,三种后端(与 worker 写入侧对齐,向量分布须一致):
    - EMBED_SERVICE_URL 非空:走 embed-service(它统一加 bge query 前缀)。
    - EMBED_PROVIDER=openai:OpenAI 兼容 embeddings(generic 模型不加 bge 前缀)。
    - 否则:进程内 SentenceTransformer + bge query 前缀。"""
    if EMBED_SERVICE_URL:
        import httpx

        resp = httpx.post(
            EMBED_SERVICE_URL.rstrip("/") + "/embed",
            json={"texts": [query], "kind": "query"},
            timeout=30.0,
        )
        resp.raise_for_status()
        return resp.json()["embeddings"][0]
    if EMBED_PROVIDER == "openai":
        from openai import OpenAI

        client = OpenAI(base_url=EMBED_OPENAI_BASE_URL or None)
        resp = client.embeddings.create(model=EMBED_MODEL, input=[query])
        return resp.data[0].embedding
    return state["model"].encode(QUERY_PREFIX + query, normalize_embeddings=True).tolist()


def retrieve(query: str, tenant_id: str, top_k: int, status: str | None) -> list[dict]:
    """向量召回,多租过滤必须带 tenant_id(DESIGN.md §3.5)。"""
    qvec = embed_query(query)
    if VECTOR_BACKEND == "qdrant":
        return _retrieve_qdrant(qvec, tenant_id, top_k, status)
    return _retrieve_pgvector(qvec, tenant_id, top_k, status)


def _set_app_tenant(cur, tenant_id: str) -> None:
    """RLS 闭环:连接池复用连接,每次借出都要重设 app.tenant。
    用事务级 set_config(..., true):autocommit 下每条语句即一个事务,
    与紧随其后的查询同事务,租户随用随设、不串号(参数化防注入)。"""
    cur.execute("SELECT set_config('app.tenant', %s, true)", (tenant_id,))


def _retrieve_pgvector(qvec: list[float], tenant_id: str, top_k: int, status: str | None) -> list[dict]:
    vec = str(qvec)
    # WHERE tenant_id 保留做双保险;真正强制隔离靠 RLS(SET app.tenant)。
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
        with conn.transaction():
            _set_app_tenant(cur, tenant_id)
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
    return {
        "status": "ok",
        "rerank": RERANK_ENABLED,
        "backend": VECTOR_BACKEND,
        "llm_provider": active_provider(),
        "llm_ready": llm_available(),
    }


@app.get("/stats")
def stats(tenant_id: str = Depends(require_tenant)):
    """向量库大盘:鉴权后只统计本租户(pgvector 同样 SET app.tenant 走 RLS)。"""
    if VECTOR_BACKEND == "qdrant":
        from qdrant_client import models

        flt = models.Filter(
            must=[models.FieldCondition(
                key="tenant_id", match=models.MatchValue(value=tenant_id)
            )]
        )
        total = state["qdrant"].count(
            QDRANT_COLLECTION, count_filter=flt
        ).count
        by_table = {
            str(hit.value): hit.count
            for hit in state["qdrant"].facet(
                collection_name=QDRANT_COLLECTION, key="source_table", facet_filter=flt
            ).hits
        }
        return {"backend": "qdrant", "total_vectors": total, "by_table": by_table}
    with state["pool"].connection() as conn, conn.cursor() as cur:
        with conn.transaction():
            _set_app_tenant(cur, tenant_id)
            # RLS 已限定本租户;GROUP BY tenant_id 保留兼容返回结构。
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
def search(req: SearchRequest, tenant_id: str = Depends(require_tenant)):
    # tenant 来自鉴权 token,忽略 req.tenant_id(不可信)。
    hits = retrieve(req.query, tenant_id, req.top_k, req.status)
    if req.rerank:
        hits = apply_rerank(req.query, hits, req.top_k)
    return [SearchHit(**h) for h in hits]


@app.post("/ask", response_model=AskResponse)
def ask(req: AskRequest, tenant_id: str = Depends(require_tenant)):
    if not llm_available():
        raise HTTPException(
            503,
            f"{api_key_env()} 未配置(LLM_PROVIDER={active_provider()}),/ask 不可用(/search 不受影响)",
        )
    # tenant 来自鉴权 token,忽略 req.tenant_id(不可信)。
    hits = retrieve(req.query, tenant_id, req.top_k, req.status)
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
