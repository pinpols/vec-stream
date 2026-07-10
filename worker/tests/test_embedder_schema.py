"""make_embedder 工厂选择 + schema_check 校验(M2)。"""

import sys
import types

import pytest

from vec_stream_worker.embedder import (
    HttpEmbedder,
    LocalEmbedder,
    OpenAIEmbedder,
    make_embedder,
)
from vec_stream_worker.schema_check import check_schema


class FakeCfg:
    embed_model = "BAAI/bge-small-zh-v1.5"
    embed_service_url = ""
    embed_service_timeout_s = 30.0
    embed_service_max_batch = 64
    embed_provider = "local"
    embed_openai_base_url = ""
    embed_egress_allowed = False


def _fake_openai(monkeypatch, captured):
    class FakeData:
        def __init__(self, v):
            self.embedding = v

    class FakeResp:
        def __init__(self, n):
            self.data = [FakeData([0.1, 0.2]) for _ in range(n)]

    class FakeEmbeddings:
        def create(self, model, input):
            captured["model"] = model
            captured["input"] = input
            return FakeResp(len(input))

    class FakeOpenAI:
        def __init__(self, base_url=None):
            captured["base_url"] = base_url
            self.embeddings = FakeEmbeddings()

    mod = types.ModuleType("openai")
    mod.OpenAI = FakeOpenAI
    monkeypatch.setitem(sys.modules, "openai", mod)


def test_make_embedder_openai(monkeypatch):
    captured = {}
    _fake_openai(monkeypatch, captured)
    cfg = FakeCfg()
    cfg.embed_provider = "openai"
    cfg.embed_model = "text-embedding-3-small"
    cfg.embed_openai_base_url = "https://api.openai.com/v1"
    cfg.embed_egress_allowed = True
    e = make_embedder(cfg)
    assert isinstance(e, OpenAIEmbedder)
    out = e.embed_passages(["a", "b"])
    assert out == [[0.1, 0.2], [0.1, 0.2]]
    assert captured["model"] == "text-embedding-3-small"
    assert captured["base_url"] == "https://api.openai.com/v1"


def test_openai_embedder_empty(monkeypatch):
    captured = {}
    _fake_openai(monkeypatch, captured)
    cfg = FakeCfg()
    cfg.embed_provider = "openai"
    cfg.embed_egress_allowed = True
    assert make_embedder(cfg).embed_passages([]) == []


def test_make_embedder_openai_requires_explicit_egress_allowance(monkeypatch):
    captured = {}
    _fake_openai(monkeypatch, captured)
    cfg = FakeCfg()
    cfg.embed_provider = "openai"
    cfg.embed_egress_allowed = False

    with pytest.raises(RuntimeError, match="EMBED_EGRESS_ALLOWED=true"):
        make_embedder(cfg)


def test_make_embedder_http_when_url_set(monkeypatch):
    # 不真起 httpx 连接,只验类型选择
    cfg = FakeCfg()
    cfg.embed_service_url = "http://embed:8200"
    e = make_embedder(cfg)
    assert isinstance(e, HttpEmbedder)


def test_make_embedder_local_when_url_empty(monkeypatch):
    # LocalEmbedder 会加载 SentenceTransformer,patch 掉避免真下载模型
    import vec_stream_worker.embedder as mod

    monkeypatch.setattr(mod.LocalEmbedder, "__init__", lambda self, name: None)
    cfg = FakeCfg()
    e = make_embedder(cfg)
    assert isinstance(e, LocalEmbedder)


def test_http_embedder_batches_and_parses(monkeypatch):
    cfg = FakeCfg()
    cfg.embed_service_url = "http://embed:8200"
    e = make_embedder(cfg)
    calls = []

    class FakeResp:
        def raise_for_status(self):
            pass

        def json(self):
            return {"embeddings": [[0.1, 0.2]]}

    def fake_post(url, json):
        calls.append(json)
        return FakeResp()

    monkeypatch.setattr(e._client, "post", fake_post)
    out = e.embed_passages(["a"])
    assert out == [[0.1, 0.2]]
    assert calls[0] == {"texts": ["a"], "kind": "passage"}


def test_http_embedder_empty():
    cfg = FakeCfg()
    cfg.embed_service_url = "http://embed:8200"
    assert make_embedder(cfg).embed_passages([]) == []


# ── schema_check ──
class FakeCur:
    def __init__(self, cols, replident=None):
        self._cols = cols
        self._replident = replident or {}
        self._mode = "cols"

    def execute(self, sql, params):
        self._table = params[0]
        self._mode = "replident" if "relreplident" in sql else "cols"

    def fetchall(self):
        return [(c,) for c in self._cols.get(self._table, [])]

    def fetchone(self):
        if self._table not in self._cols:
            return None  # 表不存在 → to_regclass 为 NULL,查无行
        return (self._replident.get(self._table, "f"),)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConn:
    def __init__(self, cols, replident=None):
        self._cols = cols
        self._replident = replident

    def cursor(self):
        return FakeCur(self._cols, self._replident)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _patch_connect(monkeypatch, cols, replident=None):
    import vec_stream_worker.schema_check as mod

    monkeypatch.setattr(mod.psycopg, "connect", lambda dsn: FakeConn(cols, replident))


def test_schema_check_passes(monkeypatch):
    _patch_connect(
        monkeypatch,
        {
            "article": ["id", "tenant_id", "title", "body", "status"],
            "comment": ["id", "tenant_id", "article_id", "body"],
        },
    )
    tables = {
        "article": {"fields": ["title", "body"], "pk": "id", "title_field": "title"},
        "comment": {"reembed_parent": {"table": "article", "fk": "article_id"}},
    }
    check_schema("dsn", tables)  # 不抛即通过


def test_schema_check_fails_missing_field(monkeypatch):
    _patch_connect(monkeypatch, {"article": ["id", "tenant_id", "title"]})  # 缺 body
    tables = {"article": {"fields": ["title", "body"], "pk": "id"}}
    with pytest.raises(RuntimeError, match="缺字段"):
        check_schema("dsn", tables)


def test_schema_check_fails_missing_fk(monkeypatch):
    _patch_connect(monkeypatch, {"comment": ["id", "tenant_id", "body"]})  # 缺 article_id
    tables = {"comment": {"reembed_parent": {"table": "article", "fk": "article_id"}}}
    with pytest.raises(RuntimeError, match="reembed_parent"):
        check_schema("dsn", tables)


# ── P2:CDC 表必须 REPLICA IDENTITY FULL(否则 before 镜像不完整,
#        tenant 变更删旧向量 / DELETE 定位 / TOAST 回退全部失效)──


def test_schema_check_fails_replica_identity_not_full(monkeypatch):
    _patch_connect(
        monkeypatch,
        {"article": ["id", "tenant_id", "title", "body"]},
        replident={"article": "d"},  # d = default(仅 PK)
    )
    tables = {"article": {"fields": ["title", "body"], "pk": "id"}}
    with pytest.raises(RuntimeError, match="REPLICA IDENTITY FULL"):
        check_schema("dsn", tables)


def test_schema_check_checks_reembed_parent_table_identity(monkeypatch):
    # 只配了子表 comment:父表 article 也是 CDC 依赖(重 embed 反查其行),RI 一样要查
    _patch_connect(
        monkeypatch,
        {
            "article": ["id", "tenant_id", "title", "body"],
            "comment": ["id", "tenant_id", "article_id", "body"],
        },
        replident={"article": "n", "comment": "f"},
    )
    tables = {"comment": {"reembed_parent": {"table": "article", "fk": "article_id"}}}
    with pytest.raises(RuntimeError, match="article.*REPLICA IDENTITY FULL"):
        check_schema("dsn", tables)


def test_schema_check_passes_with_full_identity(monkeypatch):
    _patch_connect(
        monkeypatch,
        {
            "article": ["id", "tenant_id", "title", "body"],
            "comment": ["id", "tenant_id", "article_id", "body"],
        },
    )  # replident 默认全 'f'
    tables = {
        "article": {"fields": ["title", "body"], "pk": "id"},
        "comment": {"reembed_parent": {"table": "article", "fk": "article_id"}},
    }
    check_schema("dsn", tables)  # 不抛即通过
