"""Embedding 抽象:本地进程内(SentenceTransformer)或远程 HTTP(embed-service)。

M2 领域二:embedding 推理可拆为独立服务,worker 扩容时不再每实例翻倍模型副本。
- EMBED_SERVICE_URL 为空 → 进程内 LocalEmbedder(bge-small-zh,passage 直接编码)。
- EMBED_SERVICE_URL 非空 → HttpEmbedder,POST {texts, kind:"passage"} 给 embed-service。

bge 系列约定:passage 不加前缀,query 加检索指令前缀(query 前缀由 embed-service / rag
侧处理,worker 只编码 passage)。两条路径都 normalize,向量分布可互换,无需重建索引。
"""

import logging

log = logging.getLogger("embedder")


class LocalEmbedder:
    """进程内 SentenceTransformer。"""

    def __init__(self, model_name: str):
        from sentence_transformers import SentenceTransformer

        self.model = SentenceTransformer(model_name)

    def embed_passages(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        vecs = self.model.encode(texts, normalize_embeddings=True, batch_size=32)
        return [v.tolist() for v in vecs]


class HttpEmbedder:
    """远程 embed-service。失败抛异常,交由消费侧重试(TRANSIENT_ERRORS 含 OSError)。"""

    def __init__(self, base_url: str, timeout_s: float = 30.0, max_batch: int = 64):
        import httpx

        self._url = base_url.rstrip("/") + "/embed"
        self._client = httpx.Client(timeout=timeout_s)
        self._max_batch = max_batch

    def embed_passages(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        out: list[list[float]] = []
        # 切片到服务 MAX_BATCH 以内(服务超限返回 413)
        for i in range(0, len(texts), self._max_batch):
            batch = texts[i : i + self._max_batch]
            resp = self._client.post(self._url, json={"texts": batch, "kind": "passage"})
            resp.raise_for_status()
            out.extend(resp.json()["embeddings"])
        return out

    def close(self) -> None:
        self._client.close()


def make_embedder(cfg):
    """按配置选择 embedder 后端:EMBED_SERVICE_URL 非空走 HTTP,否则进程内。"""
    url = getattr(cfg, "embed_service_url", "") or ""
    if url:
        log.info("embedding 后端: HTTP %s", url)
        return HttpEmbedder(
            url,
            timeout_s=getattr(cfg, "embed_service_timeout_s", 30.0),
            max_batch=getattr(cfg, "embed_service_max_batch", 64),
        )
    if getattr(cfg, "embed_provider", "local") == "openai":
        if not getattr(cfg, "embed_egress_allowed", False):
            raise RuntimeError(
                "EMBED_EGRESS_ALLOWED=true 未配置,拒绝向 OpenAI-compatible embeddings 端点发送源文档"
            )
        log.info("embedding 后端: OpenAI 兼容 %s", cfg.embed_model)
        return OpenAIEmbedder(
            cfg.embed_model,
            base_url=getattr(cfg, "embed_openai_base_url", "") or None,
            max_batch=getattr(cfg, "embed_service_max_batch", 64),
        )
    log.info("embedding 后端: 进程内 %s", cfg.embed_model)
    return LocalEmbedder(cfg.embed_model)


class OpenAIEmbedder:
    """OpenAI 兼容 embeddings API(OpenAI / TEI / Ollama / vLLM,靠 base_url 切)。
    passage 直接送;query 前缀由调用侧/服务侧处理(bge 用 prefix,OpenAI 原生模型无需)。
    维度由所选模型决定,必须与向量列(doc_vectors.embedding)一致——换模型/换维需配合蓝绿重建。"""

    def __init__(self, model: str, base_url: str | None = None, max_batch: int = 64):
        from openai import OpenAI

        self._client = OpenAI(base_url=base_url)  # api_key 从 OPENAI_API_KEY/EMBED_API_KEY 读
        self._model = model
        self._max_batch = max_batch

    def embed_passages(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        out: list[list[float]] = []
        for i in range(0, len(texts), self._max_batch):
            batch = texts[i : i + self._max_batch]
            resp = self._client.embeddings.create(model=self._model, input=batch)
            out.extend(d.embedding for d in resp.data)
        return out


# 兼容旧引用
Embedder = LocalEmbedder
