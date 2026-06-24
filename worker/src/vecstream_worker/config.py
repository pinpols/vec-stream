"""环境变量配置,全部带本地默认值,docker compose 场景零配置可跑。"""
import json
import os
from dataclasses import dataclass, field

# 每表的同步配置:fields=拼进 source_text 的字段(按序),pk=主键列,
# title_field=存进 metadata.title 用于召回展示的字段;
# enrich_sql=反查 SQL(%s=本行 pk),查到的文本追加进 source_text(跨表文档);
# reembed_parent=本表是子表,自身不进向量库,变更触发父行重新 embed。
# 可用 TABLES_JSON 环境变量整体覆盖。
DEFAULT_TABLES: dict = {
    "article": {
        "fields": ["title", "body"], "pk": "id", "title_field": "title",
        # 反查 SQL 用命名参数:%(pk)s=本行主键,%(tenant)s=租户(防跨租户混入)
        "enrich_sql": "SELECT body FROM comment "
                      "WHERE article_id = %(pk)s AND tenant_id = %(tenant)s ORDER BY id",
    },
    "product": {"fields": ["name", "description"], "pk": "id", "title_field": "name"},
    "comment": {"reembed_parent": {"table": "article", "fk": "article_id"}},
}


def _tables_from_env() -> dict:
    raw = os.getenv("TABLES_JSON", "")
    return json.loads(raw) if raw else DEFAULT_TABLES


@dataclass(frozen=True)
class Config:
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    # 正则订阅:新表只要进了 Debezium 的 table.include.list 并配好 tables,无需改订阅
    kafka_topic_pattern: str = os.getenv("KAFKA_TOPIC_PATTERN", r"^cdc\.public\..*")
    kafka_group_id: str = os.getenv("KAFKA_GROUP_ID", "vecstream-worker")
    # worker 用最小权限角色 vs_worker(doc_vectors/processed_offsets DML),
    # 不再默认超级账号:优先 WORKER_PG_DSN,回退共享 PG_DSN,再回退本地 vs_worker 默认
    pg_dsn: str = (
        os.getenv("WORKER_PG_DSN")
        or os.getenv("PG_DSN")
        or "postgresql://vs_worker:vs_worker@localhost:5433/vecstream"
    )
    embed_model: str = os.getenv("EMBED_MODEL", "BAAI/bge-small-zh-v1.5")
    embed_dim: int = int(os.getenv("EMBED_DIM", "512"))
    chunk_size: int = int(os.getenv("CHUNK_SIZE", "400"))
    chunk_overlap: int = int(os.getenv("CHUNK_OVERLAP", "50"))
    # 单文档字符上限(防超大文本拖垮单条处理)
    max_doc_chars: int = int(os.getenv("MAX_DOC_CHARS", "200000"))
    tables: dict = field(default_factory=_tables_from_env)
    # 失败重试 + DLQ
    dlq_topic: str = os.getenv("DLQ_TOPIC", "cdc.dlq")
    max_retries: int = int(os.getenv("MAX_RETRIES", "3"))
    retry_backoff_s: float = float(os.getenv("RETRY_BACKOFF_S", "1.0"))
    # 向量库后端:pgvector(默认)| qdrant
    vector_backend: str = os.getenv("VECTOR_BACKEND", "pgvector")
    qdrant_url: str = os.getenv("QDRANT_URL", "http://localhost:6333")
    qdrant_collection: str = os.getenv("QDRANT_COLLECTION", "doc_vectors")
    # replication slot lag 监控(slot 不消费会撑爆 PG 磁盘,DESIGN.md §5)
    metrics_port: int = int(os.getenv("METRICS_PORT", "9100"))
    slot_name: str = os.getenv("SLOT_NAME", "vecstream_slot")
    slot_check_interval_s: int = int(os.getenv("SLOT_CHECK_INTERVAL_S", "60"))
    slot_lag_warn_mb: int = int(os.getenv("SLOT_LAG_WARN_MB", "256"))

    def __post_init__(self):
        if self.chunk_overlap >= self.chunk_size:
            raise ValueError(
                f"CHUNK_OVERLAP({self.chunk_overlap}) 必须小于 CHUNK_SIZE({self.chunk_size})"
            )
        if self.vector_backend not in ("pgvector", "qdrant"):
            raise ValueError(f"VECTOR_BACKEND 必须是 pgvector|qdrant,得到 {self.vector_backend}")
        if self.embed_dim <= 0 or self.max_doc_chars <= 0:
            raise ValueError("EMBED_DIM / MAX_DOC_CHARS 必须为正数")
