"""query 向量化策略 + 工厂。与 worker 写入侧对齐(同后端同模型,向量分布须一致)。

三后端集中在 make_query_embedder,避免「lifespan 初始化」与「embed_query 运行时」两处
分散维护(原 app.py 的痛点)。加新后端只改这一个工厂。
- embed-service(HTTP):服务端统一加 bge query 前缀;
- openai 兼容:原生模型无需 bge 前缀;
- 进程内 SentenceTransformer:本地加 bge query 前缀。
"""

from fastapi import HTTPException


class _ServiceEmbedder:
    def __init__(self, service_url: str, timeout: float = 30.0):
        import httpx

        self._url = service_url.rstrip("/") + "/embed"
        self._client = httpx.Client(timeout=timeout)

    def embed(self, query: str) -> list[float]:
        import httpx

        try:
            resp = self._client.post(self._url, json={"texts": [query], "kind": "query"})
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            # 4xx(请求过大/非法)是调用方问题 → 透传 4xx;429/5xx 服务端不可用 → 503
            if code < 500 and code != 429:
                raise HTTPException(status_code=400, detail=f"embed 请求被拒({code})") from e
            raise HTTPException(status_code=503, detail="embed-service 暂不可用") from e
        except httpx.RequestError as e:
            raise HTTPException(status_code=503, detail="embed-service 不可达") from e
        return resp.json()["embeddings"][0]

    def close(self) -> None:
        self._client.close()


class _OpenAIEmbedder:
    def __init__(self, model: str, base_url: str | None):
        from openai import OpenAI

        self._client = OpenAI(base_url=base_url or None)
        self._model = model

    def embed(self, query: str) -> list[float]:
        return self._client.embeddings.create(model=self._model, input=[query]).data[0].embedding

    def close(self) -> None:
        self._client.close()


class _LocalEmbedder:
    def __init__(self, model, query_prefix: str):
        self._model = model
        self._prefix = query_prefix

    def embed(self, query: str) -> list[float]:
        return self._model.encode(self._prefix + query, normalize_embeddings=True).tolist()

    def close(self) -> None:
        pass


def make_query_embedder(
    *,
    service_url: str,
    provider: str,
    model_name: str,
    openai_base_url: str,
    query_prefix: str,
    local_model,
):
    """按配置选 query embedder(与 worker make_embedder 对齐的选择顺序)。"""
    if service_url:
        return _ServiceEmbedder(service_url)
    if provider == "openai":
        return _OpenAIEmbedder(model_name, openai_base_url)
    return _LocalEmbedder(local_model, query_prefix)
