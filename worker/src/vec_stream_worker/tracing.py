"""OpenTelemetry 分布式追踪初始化(可选,默认零开销且不 import otel)。

统一契约(worker / rag / eval 三模块一致):
  OTEL_ENABLED                  非 "true" → setup_tracing 直接 return,no-op,
                                **不 import 任何 opentelemetry**(默认安装无 otel 依赖也能跑)。
  OTEL_EXPORTER_OTLP_ENDPOINT   OTLP/gRPC 端点,默认 "http://localhost:4317"。
  service.name                  固定 "vec-stream-worker"(由调用方传入)。

otel 作为可选依赖 extra(pyproject [project.optional-dependencies] otel),
默认 `uv sync` 不装;`uv sync --extra otel` 才装。启用追踪前需先装该 extra。

启用时:建 TracerProvider(Resource service.name)+ BatchSpanProcessor(OTLPSpanExporter)
设为全局 provider,再对 psycopg / httpx /(可选)confluent-kafka 自动埋点。
"""

import logging
import os

log = logging.getLogger("worker")

# 启用判定:仅当 OTEL_ENABLED 显式为 true(大小写不敏感)。其余一切值(含未设)→ 关。
_TRUE = {"true", "1", "yes", "on"}


def tracing_enabled() -> bool:
    return os.getenv("OTEL_ENABLED", "false").strip().lower() in _TRUE


def setup_tracing(service_name: str) -> None:
    """装配全局 TracerProvider 并自动埋点。

    OTEL_ENABLED 非 true 时立即 return —— 此路径下函数体不引用任何 opentelemetry 符号,
    保证默认安装(无 otel 依赖)导入并调用本函数不抛错、不产生开销。
    """
    if not tracing_enabled():
        return

    # 仅在启用分支内 import otel,默认路径绝不触达这些可选依赖。
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError as e:
        log.warning("OTEL_ENABLED=true 但未安装 otel 依赖(uv sync --extra otel),追踪关闭: %s", e)
        return

    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    trace.set_tracer_provider(provider)

    _instrument(endpoint)
    log.info("otel tracing on: service=%s endpoint=%s", service_name, endpoint)


def _instrument(endpoint: str) -> None:
    """对依赖库自动埋点。每个库独立 try,缺哪个埋点包就跳过哪个,互不影响。"""
    # psycopg —— sink / source_db 的 DB 调用
    try:
        from opentelemetry.instrumentation.psycopg import PsycopgInstrumentor

        PsycopgInstrumentor().instrument()
    except Exception as e:  # noqa: BLE001 —— 埋点失败不应拖垮 worker
        log.warning("psycopg 埋点跳过: %s", e)

    # httpx —— HttpEmbedder / OpenAIEmbedder 的远程调用
    try:
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

        HTTPXClientInstrumentor().instrument()
    except Exception as e:  # noqa: BLE001
        log.warning("httpx 埋点跳过: %s", e)

    # confluent-kafka —— 埋点包存在才用;不稳/缺失则跳过,由 main 的手动 span 兜底
    try:
        from opentelemetry.instrumentation.confluent_kafka import ConfluentKafkaInstrumentor

        ConfluentKafkaInstrumentor().instrument()
    except Exception as e:  # noqa: BLE001
        log.info("confluent-kafka 埋点跳过(手动 span 兜底): %s", e)


def extract_context(headers):
    """从 Kafka 消息 header 的 traceparent 续接上游 trace context。

    Debezium 事件本身不带 trace context,故多数情况返回 None(每条事件起新 trace);
    若有上游(如回投/重放)注入了 traceparent,则续接。未启用或缺依赖时返回 None。
    """
    if not tracing_enabled() or not headers:
        return None
    try:
        from opentelemetry.propagate import extract
    except ImportError:
        return None
    carrier = {}
    for key, value in headers:
        if isinstance(value, bytes | bytearray):
            value = value.decode("utf-8", "replace")
        carrier[key] = value
    if "traceparent" not in carrier:
        return None
    return extract(carrier)


def start_process_span(table: str, op: str | None, pk, context=None):
    """为单条事件处理开 span `process_event`,返回上下文管理器。

    进入(__enter__)产出 span(未启用时为 None,调用方据此判断是否 set 属性 / 记异常)。
    span 名 `process_event`,初始属性 table/op(及 pk,有则);action 由调用方在拿到返回值后补设。
    context:可选,由 extract_context 续接的上游 trace context。
    """
    if not tracing_enabled():
        return _NoopSpanCtx()
    try:
        from opentelemetry import trace
    except ImportError:
        return _NoopSpanCtx()

    tracer = trace.get_tracer("vec_stream_worker")
    attributes = {"table": table, "op": op or ""}
    if pk is not None:
        attributes["pk"] = str(pk)
    return tracer.start_as_current_span("process_event", context=context, attributes=attributes)


def record_span_exception(span, exc: Exception) -> None:
    """在 span 上记录异常并置 ERROR 状态;span 为 None(未启用)时 no-op。"""
    if span is None:
        return
    try:
        from opentelemetry.trace import Status, StatusCode

        span.record_exception(exc)
        span.set_status(Status(StatusCode.ERROR, str(exc)))
    except Exception:  # noqa: BLE001 —— 记录失败不影响主流程
        pass


def set_span_attribute(span, key: str, value) -> None:
    """给 span 补设属性;span 为 None 时 no-op。"""
    if span is None or value is None:
        return
    try:
        span.set_attribute(key, value)
    except Exception:  # noqa: BLE001
        pass


class _NoopSpanCtx:
    """OTEL_ENABLED 关时用的零开销上下文管理器,进入产出 None span。"""

    def __enter__(self):
        return None

    def __exit__(self, *exc):
        return False
