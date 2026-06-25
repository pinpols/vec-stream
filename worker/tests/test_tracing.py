"""tracing.setup_tracing 守卫测试。

核心契约:OTEL_ENABLED 未设/非 true 时 setup_tracing 是 no-op,且**不 import 任何 otel**——
默认 venv 不装 otel 也必须能过。启用路径用 importorskip 跳过(CI 默认不装 otel)。
"""

import sys

import pytest

from vec_stream_worker import tracing


def test_disabled_is_noop_without_otel(monkeypatch):
    """OTEL_ENABLED 未设 → setup_tracing 不抛、不 import otel。"""
    monkeypatch.delenv("OTEL_ENABLED", raising=False)
    # 默认 venv 本就没装 otel;断言调用前后 opentelemetry 未被导入。
    assert "opentelemetry" not in sys.modules
    assert tracing.tracing_enabled() is False
    tracing.setup_tracing("vec-stream-worker")  # 不应抛
    assert "opentelemetry" not in sys.modules


@pytest.mark.parametrize("value", ["false", "0", "no", "off", "", "FALSE"])
def test_explicit_false_values_disabled(monkeypatch, value):
    monkeypatch.setenv("OTEL_ENABLED", value)
    assert tracing.tracing_enabled() is False
    tracing.setup_tracing("vec-stream-worker")  # no-op,不抛


@pytest.mark.parametrize("value", ["true", "True", "1", "yes", "ON"])
def test_truthy_values_enabled(monkeypatch, value):
    monkeypatch.setenv("OTEL_ENABLED", value)
    assert tracing.tracing_enabled() is True


def test_noop_span_ctx_yields_none(monkeypatch):
    """未启用时 start_process_span 返回 no-op 上下文,产出 None span。"""
    monkeypatch.delenv("OTEL_ENABLED", raising=False)
    with tracing.start_process_span("article", "u", 7) as span:
        assert span is None
    # span 为 None 时 record/set 辅助函数均 no-op
    tracing.record_span_exception(None, ValueError("x"))
    tracing.set_span_attribute(None, "action", "upserted")


def test_extract_context_noop_when_disabled(monkeypatch):
    monkeypatch.delenv("OTEL_ENABLED", raising=False)
    assert tracing.extract_context([("traceparent", b"00-abc-def-01")]) is None


def test_enabled_path_produces_span(monkeypatch):
    """启用 + 装了 otel 时:setup_tracing 装配 provider 且 process_event span 真产生。

    importorskip 保证默认(不装 otel)环境下整段跳过,不报错。
    """
    pytest.importorskip("opentelemetry")
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    monkeypatch.setenv("OTEL_ENABLED", "true")

    # 直接装一个 InMemory provider(避免依赖真实 OTLP 端点);埋点装配由 setup_tracing 走过即可。
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    with tracing.start_process_span("article", "u", 7) as span:
        assert span is not None
        tracing.set_span_attribute(span, "action", "upserted")

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    s = spans[0]
    assert s.name == "process_event"
    assert s.attributes["table"] == "article"
    assert s.attributes["op"] == "u"
    assert s.attributes["pk"] == "7"
    assert s.attributes["action"] == "upserted"
