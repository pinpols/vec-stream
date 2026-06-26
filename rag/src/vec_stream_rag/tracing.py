"""OpenTelemetry 分布式追踪(可选,默认零开销)。

统一契约(与 worker / embed-service 一致):
  OTEL_ENABLED                  非 "true" → setup_tracing 直接 return(no-op,不 import otel);
                                "true"     → 装配全局 TracerProvider + OTLP 导出 + 自动埋点。
  OTEL_EXPORTER_OTLP_ENDPOINT   默认 "http://localhost:4317"(OTLP/gRPC)。
  service.name                  固定传入 "vec-stream-rag"。

默认路径(OTEL_ENABLED 未设/非 true)绝不 import opentelemetry,因此 otel 作为可选 extra
(pyproject `[project.optional-dependencies] otel`)默认 `uv sync` 不安装也能正常运行。

自动埋点(仅启用时):
  - FastAPI(opentelemetry-instrumentation-fastapi)—— /search /ask 请求 span。
    需在 app 创建后调用 instrument_app(app)。
  - httpx(opentelemetry-instrumentation-httpx)—— embed-service / LLM(openai SDK 走 httpx)
    出站调用,自动注入 W3C traceparent 头实现跨服务串联(rag→embed-service、rag→LLM 网关)。
  - psycopg(opentelemetry-instrumentation-psycopg)—— pgvector 检索 SQL span。
"""

import logging
import os

log = logging.getLogger("rag")

_SERVICE_NAME_DEFAULT = "vec-stream-rag"
_DEFAULT_OTLP_ENDPOINT = "http://localhost:4317"

# 模块级标记:仅启用且成功装配后置 True,instrument_app / get_tracer 据此决定是否触碰 otel。
_enabled = False


def _otel_on() -> bool:
    """读环境变量判断是否启用。统一用此判断,保证默认路径不 import otel。"""
    return os.getenv("OTEL_ENABLED", "false").lower() == "true"


def setup_tracing(service_name: str = _SERVICE_NAME_DEFAULT) -> None:
    """装配全局追踪。OTEL_ENABLED 非 true 时为 no-op 且不 import opentelemetry。

    启用时:TracerProvider(Resource: service.name)+ BatchSpanProcessor(OTLPSpanExporter),
    设为全局 provider,并对 httpx / psycopg 做进程级自动埋点(FastAPI 单独经 instrument_app)。
    幂等:重复调用只装配一次。
    """
    global _enabled
    if not _otel_on():
        return  # no-op:默认路径,绝不 import otel
    if _enabled:
        return

    # 仅在启用分支内 import,缺 extra 时给出清晰提示而非默认路径崩溃。
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError:
        log.error(
            "OTEL_ENABLED=true 但未安装 otel 依赖;请 `uv sync --extra otel` 后重试。追踪未启用。"
        )
        return

    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", _DEFAULT_OTLP_ENDPOINT)
    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    if endpoint.lower() not in {"none", "disabled"}:
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    trace.set_tracer_provider(provider)

    # httpx:出站调用自动埋点 + 注入 traceparent(跨服务 context 传播的关键)。
    try:
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

        HTTPXClientInstrumentor().instrument()
    except ImportError:
        log.warning("opentelemetry-instrumentation-httpx 未安装,跳过 httpx 自动埋点")

    # psycopg:pgvector 检索 SQL span。
    try:
        from opentelemetry.instrumentation.psycopg import PsycopgInstrumentor

        PsycopgInstrumentor().instrument()
    except ImportError:
        log.warning("opentelemetry-instrumentation-psycopg 未安装,跳过 psycopg 自动埋点")

    _enabled = True
    log.info("OpenTelemetry 追踪已启用 service.name=%s endpoint=%s", service_name, endpoint)


def instrument_app(app) -> None:
    """对 FastAPI app 做请求级自动埋点(/search /ask 请求 span)。

    须在 app 创建后调用。未启用追踪时为 no-op(不 import otel)。
    """
    if not _enabled:
        return
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(app)
    except ImportError:
        log.warning("opentelemetry-instrumentation-fastapi 未安装,跳过 FastAPI 自动埋点")


def get_tracer(name: str = "vec_stream_rag"):
    """返回一个 tracer。

    启用时返回真实 tracer;未启用时返回 otel 的 no-op tracer —— 通过 trace.get_tracer
    (此时全局是默认的 NoOpTracerProvider)。为避免默认路径 import otel,未启用时返回
    一个轻量本地 no-op tracer(start_as_current_span 返回空上下文管理器)。
    """
    if not _enabled:
        return _NoOpTracer()
    from opentelemetry import trace

    return trace.get_tracer(name)


class _NoOpSpan:
    """未启用时的占位 span:set_attribute 等方法全部 no-op。"""

    def set_attribute(self, *args, **kwargs):
        pass

    def set_attributes(self, *args, **kwargs):
        pass

    def add_event(self, *args, **kwargs):
        pass

    def record_exception(self, *args, **kwargs):
        pass

    def set_status(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _NoOpTracer:
    """未启用时的占位 tracer:不 import otel,start_as_current_span 返回空上下文。"""

    def start_as_current_span(self, *args, **kwargs):
        return _NoOpSpan()
