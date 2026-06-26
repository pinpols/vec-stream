"""OpenTelemetry 追踪装配测试。

两条路径:
- 默认路径(OTEL_ENABLED 未设/非 true):setup_tracing 为 no-op,且不要求 otel 已安装
  —— 默认 venv 无 otel extra 也必须全过。
- 启用路径(OTEL_ENABLED=true):需 otel 已装(importorskip),验证装配不抛、
  instrument_app / get_tracer 正常,且不破坏现有 app。
"""

import importlib

import pytest


@pytest.fixture(autouse=True)
def _reset_tracing_state():
    """每个用例后把 tracing 模块的全局 _enabled 复位,避免用例间污染。"""
    import vec_stream_rag.tracing as t

    saved = t._enabled
    yield
    t._enabled = saved


# ---------------------------------------------------------------------------
# 默认路径:不依赖 otel
# ---------------------------------------------------------------------------


def test_setup_tracing_noop_when_disabled(monkeypatch):
    """OTEL_ENABLED 未设 → setup_tracing 直接 return,不导入 otel、不抛错。"""
    monkeypatch.delenv("OTEL_ENABLED", raising=False)
    import vec_stream_rag.tracing as t

    t._enabled = False
    t.setup_tracing("vec-stream-rag")  # 不应抛
    assert t._enabled is False


def test_setup_tracing_noop_when_false(monkeypatch):
    monkeypatch.setenv("OTEL_ENABLED", "false")
    import vec_stream_rag.tracing as t

    t._enabled = False
    t.setup_tracing("vec-stream-rag")
    assert t._enabled is False


def test_get_tracer_noop_usable_when_disabled(monkeypatch):
    """未启用时 get_tracer 返回可用的本地 no-op tracer(不 import otel)。"""
    monkeypatch.delenv("OTEL_ENABLED", raising=False)
    import vec_stream_rag.tracing as t

    t._enabled = False
    tracer = t.get_tracer("x")
    # start_as_current_span 是空上下文管理器,span 各方法均 no-op。
    with tracer.start_as_current_span("s") as span:
        span.set_attribute("k", "v")
        span.set_attributes({"a": 1})
        span.add_event("e")


def test_instrument_app_noop_when_disabled(monkeypatch):
    """未启用时 instrument_app 为 no-op,不触碰 otel。"""
    monkeypatch.delenv("OTEL_ENABLED", raising=False)
    import vec_stream_rag.tracing as t

    t._enabled = False
    t.instrument_app(object())  # 不应抛(根本不会用到 app)


def test_app_imports_without_otel(monkeypatch):
    """默认路径下导入 app 模块不应触发 otel 导入或装配,现有 app 不受影响。"""
    monkeypatch.delenv("OTEL_ENABLED", raising=False)
    import vec_stream_rag.app as appmod

    importlib.reload(appmod)
    assert appmod.app.title == "vec-stream-rag"
    # tracer 是 no-op,关键函数仍存在
    assert hasattr(appmod, "embed_query")
    assert hasattr(appmod, "retrieve")
    assert hasattr(appmod, "apply_rerank")


# ---------------------------------------------------------------------------
# 启用路径:需 otel 已安装
# ---------------------------------------------------------------------------


def test_setup_tracing_enabled(monkeypatch):
    pytest.importorskip("opentelemetry")
    monkeypatch.setenv("OTEL_ENABLED", "true")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "none")
    import vec_stream_rag.tracing as t

    t._enabled = False
    t.setup_tracing("vec-stream-rag")  # 不应抛(导出器仅在 flush 时才真连)
    assert t._enabled is True

    # 启用后 get_tracer 返回真实 tracer,span 上下文可正常进出。
    tracer = t.get_tracer("x")
    with tracer.start_as_current_span("s") as span:
        span.set_attribute("k", "v")


def test_instrument_app_enabled_does_not_break_app(monkeypatch):
    pytest.importorskip("opentelemetry")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    monkeypatch.setenv("OTEL_ENABLED", "true")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "none")
    import vec_stream_rag.tracing as t

    t._enabled = False
    t.setup_tracing("vec-stream-rag")

    app = FastAPI()

    @app.get("/ping")
    def ping():
        return {"ok": True}

    t.instrument_app(app)  # 启用时对 app 自动埋点,不应破坏请求
    with TestClient(app) as client:
        resp = client.get("/ping")
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}
