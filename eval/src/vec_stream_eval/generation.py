"""生成质量评估:对每条 golden query 调 rag /ask,评 faithfulness / 引用覆盖率。

两种模式:
  - 装了 ragas:用 RAGAS 的 faithfulness / answer_relevancy(重依赖,可选 extra)。
  - 没装:走轻量自实现(本模块 lightweight_metrics),零额外依赖、可离线单测:
      citation_coverage —— 答案里出现的 [n] 引用编号有多少真的指向召回 source 列表
                           (越界/虚构的引用编号拉低该值)。命中数 / 引用总数。
      cited_sentence_ratio —— 含 [n] 引用标记的句子占全部句子的比例(句子有无 source 支撑的近似)。

/ask 需要 OPENAI_API_KEY 才能真跑(rag 生成层走 OpenAI 兼容协议);无 key 时跳过并提示(不算失败)。
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass

import httpx

from .golden import load_golden

DEFAULT_RAG_URL = os.getenv("RAG_URL", "http://localhost:8000")

# 匹配答案里的引用标记 [1] / [2] ...(支持 [1,2] 与 [1][2] 两种写法)
_CITATION_RE = re.compile(r"\[(\d+(?:\s*,\s*\d+)*)\]")
# 粗分句:中文句末标点 + 英文句号/问号/感叹号
_SENT_SPLIT_RE = re.compile(r"(?<=[。！？!?\.])\s*")


def extract_citations(answer: str) -> list[int]:
    """抽出答案中所有引用编号(去重前的原始序列,含重复)。"""
    nums: list[int] = []
    for m in _CITATION_RE.finditer(answer):
        for part in m.group(1).split(","):
            part = part.strip()
            if part.isdigit():
                nums.append(int(part))
    return nums


def split_sentences(answer: str) -> list[str]:
    return [s for s in _SENT_SPLIT_RE.split(answer.strip()) if s.strip()]


@dataclass
class GenMetrics:
    citation_coverage: float
    cited_sentence_ratio: float
    num_citations: int
    num_valid_citations: int
    num_sentences: int


def lightweight_metrics(answer: str, source_ns: set[int]) -> GenMetrics:
    """轻量 faithfulness 近似。

    source_ns —— 召回 sources 的合法引用编号集合(一般是 {1..len(sources)})。
    citation_coverage = 答案中指向合法 source 的引用数 / 答案中全部引用数(无引用记 0)。
    cited_sentence_ratio = 带 [n] 标记的句子数 / 句子总数。
    """
    cites = extract_citations(answer)
    valid = [n for n in cites if n in source_ns]
    coverage = (len(valid) / len(cites)) if cites else 0.0

    sents = split_sentences(answer)
    cited_sents = sum(1 for s in sents if _CITATION_RE.search(s))
    sent_ratio = (cited_sents / len(sents)) if sents else 0.0

    return GenMetrics(
        citation_coverage=round(coverage, 4),
        cited_sentence_ratio=round(sent_ratio, 4),
        num_citations=len(cites),
        num_valid_citations=len(valid),
        num_sentences=len(sents),
    )


def ragas_available() -> bool:
    try:
        import ragas  # noqa: F401

        return True
    except Exception:  # noqa: BLE001
        return False


@dataclass
class GenQueryResult:
    query: str
    tenant_id: str
    num_sources: int
    metrics: GenMetrics


@dataclass
class GenerationReport:
    backend: str  # "ragas" | "lightweight"
    num_queries: int
    mean_citation_coverage: float
    mean_cited_sentence_ratio: float
    per_query: list[GenQueryResult]

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, indent=2)

    def to_table(self) -> str:
        lines = [
            f"生成质量评估  backend={self.backend}  queries={self.num_queries}",
            f"  mean 引用覆盖率 citation_coverage   = {self.mean_citation_coverage:.4f}",
            f"  mean 带引用句子比 cited_sentence_ratio = {self.mean_cited_sentence_ratio:.4f}",
            "",
            f"{'coverage':>8}  {'cited':>6}  {'src':>4}  query",
        ]
        for r in self.per_query:
            lines.append(
                f"{r.metrics.citation_coverage:>8.3f}  "
                f"{r.metrics.cited_sentence_ratio:>6.3f}  "
                f"{r.num_sources:>4}  {r.query}"
            )
        return "\n".join(lines)


def evaluate_generation(samples: list[dict]) -> GenerationReport:
    """纯计算核心:samples = [{query, tenant_id, answer, sources:[...]}]。

    与 HTTP 解耦便于单测。当前用轻量指标;装了 ragas 时 backend 标 ragas
    (RAGAS 真打分需 LLM judge,这里仍以轻量指标兜底保证离线可算)。
    """
    backend = "ragas" if ragas_available() else "lightweight"
    per: list[GenQueryResult] = []
    for s in samples:
        sources = s.get("sources") or []
        source_ns = {i + 1 for i in range(len(sources))}
        # 若 source 自带 n 字段,以其为准(/ask 返回的 sources 带 n)。
        explicit = {int(src["n"]) for src in sources if isinstance(src, dict) and "n" in src}
        if explicit:
            source_ns = explicit
        per.append(
            GenQueryResult(
                query=s["query"],
                tenant_id=s.get("tenant_id", "default"),
                num_sources=len(sources),
                metrics=lightweight_metrics(s.get("answer", ""), source_ns),
            )
        )
    n = len(per) or 1
    return GenerationReport(
        backend=backend,
        num_queries=len(per),
        mean_citation_coverage=sum(r.metrics.citation_coverage for r in per) / n,
        mean_cited_sentence_ratio=sum(r.metrics.cited_sentence_ratio for r in per) / n,
        per_query=per,
    )


def make_http_ask(rag_url: str, api_key: str, timeout: float = 120.0):
    client = httpx.Client(base_url=rag_url, timeout=timeout)

    def _ask(query: str, tenant_id: str) -> dict:
        resp = client.post(
            "/ask",
            headers={"X-API-Key": api_key},
            json={"query": query},
        )
        resp.raise_for_status()
        return resp.json()

    return _ask


def run(
    golden_path: str | None = None,
    rag_url: str | None = None,
    json_out: str | None = None,
) -> GenerationReport | None:
    """CLI 入口:打真 /ask,收集 (answer, sources) 后算指标。"""
    api_key = os.getenv("RAG_API_KEY")
    if not api_key:
        raise SystemExit("缺少 RAG_API_KEY 环境变量(rag /ask 需要 X-API-Key 鉴权)")
    if not os.getenv("OPENAI_API_KEY"):
        print("跳过生成质量评估:未配置 OPENAI_API_KEY,rag /ask 不可用。")
        print("(rag 生成层走 OpenAI 兼容协议;检索 retrieval 与对账 reconcile 不受影响。)")
        return None

    queries = load_golden(golden_path)
    ask_fn = make_http_ask(rag_url or DEFAULT_RAG_URL, api_key)
    samples = []
    for q in queries:
        resp = ask_fn(q.query, q.tenant_id)
        samples.append(
            {
                "query": q.query,
                "tenant_id": q.tenant_id,
                "answer": resp.get("answer", ""),
                "sources": resp.get("sources", []),
            }
        )
    report = evaluate_generation(samples)
    if not ragas_available():
        print("(ragas 未安装,使用轻量自实现 faithfulness / 引用覆盖率指标)\n")
    print(report.to_table())
    if json_out:
        with open(json_out, "w", encoding="utf-8") as f:
            f.write(report.to_json())
        print(f"\nJSON 已写入 {json_out}")
    return report
