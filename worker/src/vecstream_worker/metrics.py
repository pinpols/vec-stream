"""Prometheus 指标(DESIGN.md §5 可观测):同步延迟 / 跳过率 / DLQ / slot lag。
worker 启动时在 METRICS_PORT 暴露 /metrics,Prometheus 直接抓。"""
from prometheus_client import Counter, Gauge, Histogram, start_http_server

EVENTS = Counter(
    "vecstream_events_total", "CDC events processed by table and action",
    ["table", "action"],
)
CHUNKS_EMBEDDED = Counter(
    "vecstream_chunks_embedded_total", "Chunks embedded (the expensive call)"
)
DLQ_SENT = Counter("vecstream_dlq_sent_total", "Messages sent to DLQ")
SYNC_DELAY = Histogram(
    "vecstream_sync_delay_seconds",
    "Delay from Debezium event time to worker completion",
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, 300, 1800, float("inf")),
)
SLOT_LAG_BYTES = Gauge("vecstream_slot_lag_bytes", "Replication slot WAL lag")
SLOT_ACTIVE = Gauge("vecstream_slot_active", "Replication slot active (1/0)")
DLQ_BACKLOG = Gauge(
    "vecstream_dlq_backlog", "DLQ messages not yet consumed by the replay group"
)


def start_metrics(port: int) -> None:
    start_http_server(port)
