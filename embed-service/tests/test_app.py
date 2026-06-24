"""embed-service 单测:全程 mock 掉 SentenceTransformer,不加载真模型。

覆盖:
- /embed 形状(返回向量数 = 输入文本数,维度 = EMBED_DIM,model/dim 字段);
- kind 前缀逻辑(query 加 QUERY_PREFIX、passage 不加)——通过捕获喂给
  fake 模型的实际文本断言;
- 超 MAX_BATCH → 413;非法 kind → 422;空 texts → 422(pydantic);
- 动态批处理:多个并发 /embed 被合并成一次 encode 调用;
- /healthz 模型加载状态。
"""

import os

import pytest
from fastapi.testclient import TestClient

# 收紧 MAX_BATCH 便于测拒绝;在 import app 前设好环境变量。
os.environ.setdefault("MAX_BATCH", "4")
os.environ.setdefault("BATCH_WAIT_MS", "20")

from embed_service import app as appmod  # noqa: E402

DIM = appmod.EMBED_DIM
PREFIX = appmod.QUERY_PREFIX


class FakeModel:
    """记录每次 encode 收到的文本,返回固定维度的假向量。"""

    def __init__(self, *a, **k):
        self.encode_calls: list[list[str]] = []

    def encode(self, texts, normalize_embeddings=True, batch_size=32):
        # 真实接口:list[str] -> ndarray;这里用带 .tolist() 的桩元素。
        self.encode_calls.append(list(texts))

        class _Vec:
            def __init__(self, i):
                self._i = i

            def tolist(self):
                # 用 index 编码可验证切片顺序;补到 DIM 维。
                return [float(self._i)] + [0.0] * (DIM - 1)

        return [_Vec(i) for i in range(len(texts))]


@pytest.fixture
def fake_model(monkeypatch):
    created = {}

    def factory(*a, **k):
        m = FakeModel()
        created["model"] = m
        return m

    monkeypatch.setattr(appmod, "SentenceTransformer", factory)
    return created


@pytest.fixture
def client(fake_model):
    # with 块触发 lifespan(用 fake 模型),退出时停调度器。
    with TestClient(appmod.app) as c:
        c.created = fake_model
        yield c


def test_healthz_ready(client):
    r = client.get("/healthz")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["ready"] is True
    assert body["dim"] == DIM
    assert body["max_batch"] == appmod.MAX_BATCH


def test_embed_passage_shape(client):
    r = client.post("/embed", json={"texts": ["你好", "世界"], "kind": "passage"})
    assert r.status_code == 200
    body = r.json()
    assert body["model"] == appmod.EMBED_MODEL
    assert body["dim"] == DIM
    assert len(body["embeddings"]) == 2
    assert all(len(v) == DIM for v in body["embeddings"])
    # passage 不加前缀:喂给模型的就是原文。
    encoded = client.created["model"].encode_calls[-1]
    assert encoded == ["你好", "世界"]


def test_embed_query_adds_prefix(client):
    r = client.post("/embed", json={"texts": ["什么是向量检索"], "kind": "query"})
    assert r.status_code == 200
    encoded = client.created["model"].encode_calls[-1]
    assert encoded == [PREFIX + "什么是向量检索"]


def test_embed_default_kind_is_passage(client):
    r = client.post("/embed", json={"texts": ["abc"]})
    assert r.status_code == 200
    encoded = client.created["model"].encode_calls[-1]
    assert encoded == ["abc"]  # 无前缀


def test_over_max_batch_rejected(client):
    over = ["x"] * (appmod.MAX_BATCH + 1)
    r = client.post("/embed", json={"texts": over, "kind": "passage"})
    assert r.status_code == 413


def test_invalid_kind_rejected(client):
    r = client.post("/embed", json={"texts": ["x"], "kind": "bogus"})
    assert r.status_code == 422


def test_empty_texts_rejected(client):
    r = client.post("/embed", json={"texts": [], "kind": "passage"})
    assert r.status_code == 422  # pydantic min_length=1


def test_dynamic_batching_merges_requests(client):
    """并发到达的多个请求应被合并进尽量少的 encode 调用。

    用线程并发打 3 个请求,落在同一个 BATCH_WAIT_MS 窗口内,
    断言底层 encode 调用数 < 请求数(发生了合并)。
    """
    import threading

    model = client.created["model"]
    before = len(model.encode_calls)
    results = []

    def hit(t):
        results.append(client.post("/embed", json={"texts": [t], "kind": "passage"}))

    threads = [threading.Thread(target=hit, args=(f"t{i}",)) for i in range(3)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert all(r.status_code == 200 for r in results)
    assert all(len(r.json()["embeddings"]) == 1 for r in results)
    calls_made = len(model.encode_calls) - before
    # 3 个请求合并后 encode 调用数应严格少于 3(至少有一次合并)。
    assert calls_made < 3
