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


@dataclass
class RetrievalReport:
    k: int
    rerank: bool
    num_queries: int
    mean_recall_at_k: float
    mrr: float
    per_query: list[QueryResult]

    def to_json(self) -> str:
        d = asdict(self)
        return json.dumps(d, ensure_ascii=False, indent=2)

    def to_table(self) -> str:
        lines = [
            f"检索质量评估  k={self.k}  rerank={self.rerank}  queries={self.num_queries}",
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


def make_http_search(rag_url: str, api_key: str, timeout: float = 30.0):
    """构造打真 rag /search 的 search_fn(带 X-API-Key 鉴权)。

    top_k 直接用 k:取前 k 召回算 recall@k。rerank 透传给服务端。
    """
    client = httpx.Client(base_url=rag_url, timeout=timeout)

    def _search(query: str, tenant_id: str, k: int, rerank: bool) -> list[dict]:
        # tenant 由 api_key 推导,请求体 tenant_id 被服务端忽略,这里不传。
        resp = client.post(
            "/search",
            headers={"X-API-Key": api_key},
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
    if not api_key:
        raise SystemExit("缺少 RAG_API_KEY 环境变量(rag /search 需要 X-API-Key 鉴权)")
    queries = load_golden(golden_path)
    search_fn = make_http_search(rag_url or DEFAULT_RAG_URL, api_key)
    report = evaluate_retrieval(queries, search_fn, k=k, rerank=rerank)
    print(report.to_table())
    if json_out:
        with open(json_out, "w", encoding="utf-8") as f:
            f.write(report.to_json())
        print(f"\nJSON 已写入 {json_out}")
    return report
