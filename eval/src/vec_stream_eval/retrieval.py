"""检索质量评估:对每条 golden query 调 rag /search,算 recall@k 与 MRR。

指标定义(本模块为换 chunk 策略 / 换 embedding 模型前后跑分对比提供客观依据):
  recall@k —— 前 k 个召回结果中命中的期望文档数 / 期望文档总数。
  MRR      —— 每条 query 取「第一个命中期望文档的名次」的倒数 1/rank;
              整条 golden set 取这些倒数的平均(无命中记 0)。
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass

import httpx

from .golden import GoldenQuery, load_golden

DEFAULT_RAG_URL = os.getenv("RAG_URL", "http://localhost:8000")


def hit_key(hit: dict) -> tuple[str, str]:
    """把一条 /search 召回结果归一成 (source_table, source_pk) 比较键。"""
    return (str(hit["source_table"]), str(hit["source_pk"]))


def recall_at_k(retrieved: list[dict], expected: set[tuple[str, str]], k: int) -> float:
    """recall@k = 前 k 个召回里命中的期望文档数 / 期望文档总数。

    同一期望文档可能对应多个 chunk,用 set 去重避免重复计数。
    """
    if not expected:
        return 0.0
    topk_keys = {hit_key(h) for h in retrieved[:k]}
    hit = len(expected & topk_keys)
    return hit / len(expected)


def reciprocal_rank(retrieved: list[dict], expected: set[tuple[str, str]]) -> float:
    """单条 query 的 reciprocal rank:第一个命中期望文档的名次倒数,无命中为 0。"""
    if not expected:
        return 0.0
    for rank, hit in enumerate(retrieved, start=1):
        if hit_key(hit) in expected:
            return 1.0 / rank
    return 0.0


@dataclass
class QueryResult:
    query: str
    tenant_id: str
    expected: int
    num_retrieved: int
    recall_at_k: float
    reciprocal_rank: float


# 报告中 tenant_id 列的口径说明:请求实际命中的租户由所用 API key 决定
# (服务端忽略请求体 tenant_id),golden 的 tenant_id 字段用于展示与选 key。
TENANT_NOTE = (
    "tenant 实际由请求所用 API key 决定(服务端忽略请求体 tenant_id);"
    "per_query.tenant_id 来自 golden,仅作展示/选 key 依据"
)


def invert_api_keys(raw: str) -> dict[str, str]:
    """把 rag 同款 RAG_API_KEYS(JSON,key→tenant)倒排成 tenant→key(首个生效)。"""
    if not raw:
        return {}
    mapping = json.loads(raw)
    by_tenant: dict[str, str] = {}
    for key, tenant in mapping.items():
        by_tenant.setdefault(str(tenant), str(key))
    return by_tenant


def key_for_tenant(tenant_id: str, default_key: str | None, by_tenant: dict[str, str]) -> str:
    """选出打 /search 用的 API key:优先 per-tenant 映射,回退 RAG_API_KEY。"""
    key = by_tenant.get(tenant_id) or default_key
    if not key:
        raise SystemExit(
            f"golden 里的 tenant {tenant_id!r} 没有可用 API key:"
            "设 RAG_API_KEY(单租户)或 RAG_API_KEYS(JSON key→tenant,多租户映射)"
        )
    return key


@dataclass
class RetrievalReport:
    k: int
    rerank: bool
    num_queries: int
    mean_recall_at_k: float
    mrr: float
    per_query: list[QueryResult]
    tenant_note: str = TENANT_NOTE

    def to_json(self) -> str:
        d = asdict(self)
        return json.dumps(d, ensure_ascii=False, indent=2)

    def to_table(self) -> str:
        lines = [
            f"检索质量评估  k={self.k}  rerank={self.rerank}  queries={self.num_queries}",
            f"  ({self.tenant_note})",
            f"  mean recall@{self.k} = {self.mean_recall_at_k:.4f}",
            f"  MRR              = {self.mrr:.4f}",
            "",
            f"{'recall':>8}  {'rr':>6}  {'ret':>4}  {'exp':>4}  query",
        ]
        for r in self.per_query:
            lines.append(
                f"{r.recall_at_k:>8.3f}  {r.reciprocal_rank:>6.3f}  "
                f"{r.num_retrieved:>4}  {r.expected:>4}  {r.query}"
            )
        return "\n".join(lines)


def evaluate_retrieval(
    queries: list[GoldenQuery],
    search_fn,
    k: int = 5,
    rerank: bool = False,
) -> RetrievalReport:
    """纯计算核心:search_fn(query, tenant_id, top_k, rerank) -> list[hit dict]。

    与 HTTP 解耦,便于单测注入构造数据。汇总时取各 query 指标的算术平均。
    """
    per: list[QueryResult] = []
    for q in queries:
        retrieved = search_fn(q.query, q.tenant_id, k, rerank)
        expected = q.expected_keys()
        per.append(
            QueryResult(
                query=q.query,
                tenant_id=q.tenant_id,
                expected=len(expected),
                num_retrieved=len(retrieved),
                recall_at_k=recall_at_k(retrieved, expected, k),
                reciprocal_rank=reciprocal_rank(retrieved, expected),
            )
        )
    n = len(per) or 1
    return RetrievalReport(
        k=k,
        rerank=rerank,
        num_queries=len(per),
        mean_recall_at_k=sum(r.recall_at_k for r in per) / n,
        mrr=sum(r.reciprocal_rank for r in per) / n,
        per_query=per,
    )


def make_http_search(
    rag_url: str,
    api_key: str | None,
    timeout: float = 30.0,
    api_keys_by_tenant: dict[str, str] | None = None,
):
    """构造打真 rag /search 的 search_fn(带 X-API-Key 鉴权)。

    top_k 直接用 k:取前 k 召回算 recall@k。rerank 透传给服务端。
    ⚠️ tenant 由 API key 推导,请求体 tenant_id 被服务端忽略——golden 的
    tenant_id 只用来从 api_keys_by_tenant 选 key(多租户 golden 时必须提供
    映射,否则全部 query 实际打在 RAG_API_KEY 对应的那个租户上)。
    """
    client = httpx.Client(base_url=rag_url, timeout=timeout)
    by_tenant = api_keys_by_tenant or {}

    def _search(query: str, tenant_id: str, k: int, rerank: bool) -> list[dict]:
        resp = client.post(
            "/search",
            headers={"X-API-Key": key_for_tenant(tenant_id, api_key, by_tenant)},
            json={"query": query, "top_k": k, "rerank": rerank},
        )
        resp.raise_for_status()
        return resp.json()

    return _search


def run(
    golden_path: str | None = None,
    rag_url: str | None = None,
    k: int = 5,
    rerank: bool = False,
    json_out: str | None = None,
) -> RetrievalReport:
    """CLI 入口:加载 golden、打真 rag、打印表格、可选写 JSON。"""
    api_key = os.getenv("RAG_API_KEY")
    by_tenant = invert_api_keys(os.getenv("RAG_API_KEYS", ""))
    if not api_key and not by_tenant:
        raise SystemExit(
            "缺少 RAG_API_KEY(或多租户映射 RAG_API_KEYS)环境变量"
            "(rag /search 需要 X-API-Key 鉴权)"
        )
    queries = load_golden(golden_path)
    search_fn = make_http_search(rag_url or DEFAULT_RAG_URL, api_key, api_keys_by_tenant=by_tenant)
    report = evaluate_retrieval(queries, search_fn, k=k, rerank=rerank)
    print(report.to_table())
    if json_out:
        with open(json_out, "w", encoding="utf-8") as f:
            f.write(report.to_json())
        print(f"\nJSON 已写入 {json_out}")
    return report
