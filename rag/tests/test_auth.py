"""API key 鉴权 + 租户隔离闭环测试。

不真连 PG / 不加载模型:
- monkeypatch RAG_API_KEYS 环境变量配置 key→tenant 映射;
- monkeypatch app.retrieve 捕获实际传入的 tenant(验证来自 token 非请求体);
- /healthz 不鉴权;无 key / 错 key → 401;正确 key → 200 且 tenant 来自 key。
"""

import json

import pytest
from fastapi.testclient import TestClient

from vec_stream_rag import app as appmod

API_KEYS = {"key-acme": "acme", "key-globex": "globex"}


@pytest.fixture
def client(monkeypatch, no_real_startup):
    monkeypatch.setenv("RAG_API_KEYS", json.dumps(API_KEYS))
    # 捕获 retrieve 收到的 tenant_id,避免真连 DB / 加载模型。
    calls = {}

    def fake_retrieve(query, tenant_id, top_k, status):
        calls["tenant_id"] = tenant_id
        calls["query"] = query
        return []  # 空召回即可,不触发 rerank/LLM

    monkeypatch.setattr(appmod, "retrieve", fake_retrieve)
    # TestClient 的 with 会跑 lifespan;no_real_startup 已 mock 掉模型/池/reranker。
    with TestClient(appmod.app) as c:
        c.captured = calls
        yield c


@pytest.fixture(autouse=True)
def no_real_startup(monkeypatch):
    """阻断 lifespan 里的真实重活:模型加载 / 连接池 / qdrant。"""
    monkeypatch.setattr(appmod, "SentenceTransformer", lambda *a, **k: object())
    monkeypatch.setattr(appmod, "Reranker", lambda *a, **k: None)
    monkeypatch.setattr(appmod, "RERANK_ENABLED", False)
    monkeypatch.setattr(appmod, "VECTOR_BACKEND", "pgvector")
    monkeypatch.setattr(appmod, "check_index_metadata", lambda dsn: None)

    class _FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, *a, **k):  # healthz 探针 SELECT 1
            return None

    class _FakePool:
        def __init__(self, *a, **k):
            pass

        def connection(self):
            return _FakeConn()

        def close(self):
            pass

    monkeypatch.setattr(appmod, "ConnectionPool", _FakePool)


def test_healthz_no_auth(client):
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_search_missing_key_401(client):
    r = client.post("/search", json={"query": "hello"})
    assert r.status_code == 401


def test_search_wrong_key_401(client):
    r = client.post("/search", json={"query": "hello"}, headers={"X-API-Key": "bogus"})
    assert r.status_code == 401


def test_search_valid_key_200_and_tenant_from_token(client):
    r = client.post(
        "/search",
        json={"query": "hello"},
        headers={"X-API-Key": "key-acme"},
    )
    assert r.status_code == 200
    assert r.json() == []
    assert client.captured["tenant_id"] == "acme"


def test_search_body_tenant_is_ignored(client):
    """请求体里塞别的 tenant 也无效——以 token 为准。"""
    r = client.post(
        "/search",
        json={"query": "hello", "tenant_id": "globex"},  # 试图冒充
        headers={"X-API-Key": "key-acme"},
    )
    assert r.status_code == 200
    assert client.captured["tenant_id"] == "acme"  # 来自 key 而非 body


def test_different_key_yields_different_tenant(client):
    client.post("/search", json={"query": "x"}, headers={"X-API-Key": "key-globex"})
    assert client.captured["tenant_id"] == "globex"


def test_stats_requires_auth(client):
    assert client.get("/stats").status_code == 401


def test_ask_missing_key_401(client):
    r = client.post("/ask", json={"query": "hello"})
    assert r.status_code == 401


def test_ask_valid_key_no_hits_uses_token_tenant(client, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setenv("LLM_EGRESS_ALLOWED", "true")
    r = client.post("/ask", json={"query": "hello"}, headers={"X-API-Key": "key-acme"})
    assert r.status_code == 200
    assert r.json()["answer"] == "知识库中没有相关信息。"
    assert client.captured["tenant_id"] == "acme"


def test_ask_requires_explicit_llm_egress_allowance(client, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.delenv("LLM_EGRESS_ALLOWED", raising=False)
    r = client.post("/ask", json={"query": "hello"}, headers={"X-API-Key": "key-acme"})
    assert r.status_code == 503
    assert "LLM_EGRESS_ALLOWED=true" in r.json()["detail"]


def test_require_tenant_unit_invalid_json(monkeypatch):
    """RAG_API_KEYS 非法 JSON → 全拒(_load_api_keys 返回空)。"""
    monkeypatch.setenv("RAG_API_KEYS", "{not json")
    assert appmod._load_api_keys() == {}


def test_production_rejects_dev_api_key(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("RAG_API_KEYS", json.dumps({"dev-key-default": "default"}))
    monkeypatch.setattr(appmod, "PG_DSN", "postgresql://vs_rag:strong@postgres:5432/vec_stream")
    monkeypatch.setattr(appmod, "VECTOR_BACKEND", "pgvector")
    with pytest.raises(RuntimeError, match="dev-key-default"):
        appmod._validate_runtime_security()


def test_production_qdrant_requires_api_key(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("RAG_API_KEYS", json.dumps({"prod-key": "acme"}))
    monkeypatch.setattr(appmod, "PG_DSN", "postgresql://vs_rag:strong@postgres:5432/vec_stream")
    monkeypatch.setattr(appmod, "VECTOR_BACKEND", "qdrant")
    monkeypatch.setattr(appmod, "QDRANT_API_KEY", "")
    with pytest.raises(RuntimeError, match="QDRANT_API_KEY"):
        appmod._validate_runtime_security()
