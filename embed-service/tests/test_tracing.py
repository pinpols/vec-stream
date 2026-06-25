"""embed-service 追踪接线单测。

两类:
- OTEL_ENABLED 未设 / 非 true → setup_tracing / instrument_app 均 no-op,
  且**不要求安装 otel**(默认路径不 import opentelemetry,只验返回 False);
- OTEL_ENABLED=true → 用 pytest.importorskip 跳过未装 otel 的环境,
  验 setup_tracing 真装上 TracerProvider、instrument_app 真埋点。
"""

import importlib

import pytest

from embed_service import tracing


def test_setup_tracing_noop_when_disabled(monkeypatch):
    monkeypatch.delenv("OTEL_ENABLED", raising=False)
    # 关闭时直接 return False,全程不 import otel。
    assert tracing.setup_tracing("vec-stream-embed-service") is False


def test_setup_tracing_noop_when_false(monkeypatch):
    monkeypatch.setenv("OTEL_ENABLED", "false")
    assert tracing.setup_tracing("vec-stream-embed-service") is False


def test_instrument_app_noop_when_disabled(monkeypatch):
    monkeypatch.delenv("OTEL_ENABLED", raising=False)

    # 传一个假 app:no-op 路径根本不碰它。
    assert tracing.instrument_app(object()) is False


def test_app_imports_without_otel_when_disabled(monkeypatch):
    """默认(关闭)下 import app 不应要求 otel,且 instrument 不生效。"""
    monkeypatch.delenv("OTEL_ENABLED", raising=False)
    appmod = importlib.import_module("embed_service.app")
    assert appmod.app.title == "vec-stream-embed-service"


def test_setup_tracing_enabled(monkeypatch):
    pytest.importorskip("opentelemetry.sdk.trace")
    monkeypatch.setenv("OTEL_ENABLED", "true")
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider

    assert tracing.setup_tracing("vec-stream-embed-service") is True
    provider = trace.get_tracer_provider()
    assert isinstance(provider, TracerProvider)


def test_instrument_app_enabled(monkeypatch):
    pytest.importorskip("opentelemetry.instrumentation.fastapi")
    monkeypatch.setenv("OTEL_ENABLED", "true")
    from fastapi import FastAPI

    app = FastAPI()
    assert tracing.instrument_app(app) is True
