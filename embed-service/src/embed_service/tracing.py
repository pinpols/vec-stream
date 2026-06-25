"""OpenTelemetry 追踪接线(可选,默认零开销)。

统一契约(与 monorepo 其它模块一致):
- OTEL_ENABLED:默认 "false"。非 "true" 时 setup_tracing 直接 no-op return,
  **不 import 任何 opentelemetry**——关闭即零开销、无需装 otel extra。
- OTEL_EXPORTER_OTLP_ENDPOINT:默认 "http://localhost:4317"(OTLP gRPC)。
- service.name 固定 "vec-stream-embed-service"(由调用方传入)。

启用时:
- 装一个全局 TracerProvider + BatchSpanProcessor(OTLPSpanExporter);
- instrument_app(app) 给 FastAPI 自动埋点——每个 /embed 请求一个 server span,
  且会**自动解析上游传来的 W3C traceparent header**,把本服务的 span 续接到
  rag 发起的 trace 上,从而在 Jaeger 里看到 rag → embed-service 的完整链路。

otel 作为可选 extra(`pip install .[otel]`),默认依赖里不含。
"""

from __future__ import annotations

import os

_TRUE = {"true", "1", "yes", "on"}


def _enabled() -> bool:
    return os.getenv("OTEL_ENABLED", "false").strip().lower() in _TRUE


def setup_tracing(service_name: str = "vec-stream-embed-service") -> bool:
    """初始化全局 tracer provider 与 OTLP 导出器。

    返回是否实际启用了追踪(关闭时 False,且全程不 import otel)。
    """
    if not _enabled():
        return False

    # 仅在启用时才 import,保证默认路径零开销、不要求安装 otel extra。
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    # 已经装过(幂等)就不重复装,避免重复 provider。
    current = trace.get_tracer_provider()
    if isinstance(current, TracerProvider) and getattr(current, "_vec_stream_setup", False):
        return True

    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    provider._vec_stream_setup = True  # 幂等标记
    trace.set_tracer_provider(provider)
    return True


def instrument_app(app) -> bool:
    """给 FastAPI app 自动埋点(启用时)。

    自动从上游(rag)的 traceparent header 续接 trace,实现跨服务串联。
    返回是否实际埋点。
    """
    if not _enabled():
        return False
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    FastAPIInstrumentor.instrument_app(app)
    return True
