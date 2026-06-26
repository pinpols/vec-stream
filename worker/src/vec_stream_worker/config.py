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
        "fields": ["title", "body"],
        "pk": "id",
        "title_field": "title",
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
    kafka_bootstrap: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    )
    # 正则订阅:新表只要进了 Debezium 的 table.include.list 并配好 tables,无需改订阅
    kafka_topic_pattern: str = field(
        default_factory=lambda: os.getenv("KAFKA_TOPIC_PATTERN", r"^cdc\.public\..*")
    )
    kafka_group_id: str = field(
        default_factory=lambda: os.getenv("KAFKA_GROUP_ID", "vec-stream-worker")
    )
    # worker 用最小权限角色 vs_worker(doc_vectors/processed_offsets DML),
    # 不再默认超级账号:优先 WORKER_PG_DSN,回退共享 PG_DSN,再回退本地 vs_worker 默认
    pg_dsn: str = field(
        default_factory=lambda: os.getenv("WORKER_PG_DSN")
        or os.getenv("PG_DSN")
        or "postgresql://vs_worker:vs_worker@localhost:5433/vec_stream"
    )
    embed_model: str = field(
        default_factory=lambda: os.getenv("EMBED_MODEL", "BAAI/bge-small-zh-v1.5")
    )
    embed_dim: int = field(default_factory=lambda: int(os.getenv("EMBED_DIM", "512")))
    # M2:embedding 拆服务。非空则 worker 走 HTTP 调 embed-service,否则进程内加载模型
    embed_service_url: str = field(default_factory=lambda: os.getenv("EMBED_SERVICE_URL", ""))
    embed_service_timeout_s: float = field(
        default_factory=lambda: float(os.getenv("EMBED_SERVICE_TIMEOUT_S", "30"))
    )
    embed_service_max_batch: int = field(
        default_factory=lambda: int(os.getenv("EMBED_SERVICE_MAX_BATCH", "64"))
    )
    # embedding 后端可插拔:local(进程内 SentenceTransformer)| openai(OpenAI 兼容 embeddings)
    # ⚠️ 换 provider/模型常意味着换维度,必须与 doc_vectors.embedding 维度一致(换维走蓝绿重建)
    embed_provider: str = field(default_factory=lambda: os.getenv("EMBED_PROVIDER", "local"))
    embed_openai_base_url: str = field(
        default_factory=lambda: os.getenv("EMBED_OPENAI_BASE_URL", "")
    )
    # M2:启动时校验配置字段确实存在于源表(改列/删列快速失败,不静默用错数据)
    schema_check: bool = field(
        default_factory=lambda: os.getenv("SCHEMA_CHECK", "true").lower() == "true"
    )
    # M2:记录当前索引配置,供 rag 启动时校验 worker/rag embedding 与 chunk 参数一致
    index_metadata_enabled: bool = field(
        default_factory=lambda: os.getenv("INDEX_METADATA_ENABLED", "true").lower() == "true"
    )
    chunk_size: int = field(default_factory=lambda: int(os.getenv("CHUNK_SIZE", "400")))
    chunk_overlap: int = field(default_factory=lambda: int(os.getenv("CHUNK_OVERLAP", "50")))
    # 单文档字符上限(防超大文本拖垮单条处理)
    max_doc_chars: int = field(default_factory=lambda: int(os.getenv("MAX_DOC_CHARS", "200000")))
    tables: dict = field(default_factory=_tables_from_env)
    # 失败重试 + DLQ
    dlq_topic: str = field(default_factory=lambda: os.getenv("DLQ_TOPIC", "cdc.dlq"))
    max_retries: int = field(default_factory=lambda: int(os.getenv("MAX_RETRIES", "3")))
    retry_backoff_s: float = field(
        default_factory=lambda: float(os.getenv("RETRY_BACKOFF_S", "1.0"))
    )
    # M2 DLQ 工具链:重投次数上限,超限归档到 dead_letter_archive(PG)而非无限重投
    dlq_max_replays: int = field(default_factory=lambda: int(os.getenv("DLQ_MAX_REPLAYS", "5")))
    # 向量库后端:pgvector(默认)| qdrant
    vector_backend: str = field(default_factory=lambda: os.getenv("VECTOR_BACKEND", "pgvector"))
    qdrant_url: str = field(
        default_factory=lambda: os.getenv("QDRANT_URL", "http://localhost:6333")
    )
    qdrant_api_key: str = field(default_factory=lambda: os.getenv("QDRANT_API_KEY", ""))
    qdrant_collection: str = field(
        default_factory=lambda: os.getenv("QDRANT_COLLECTION", "doc_vectors")
    )
    # replication slot lag 监控(slot 不消费会撑爆 PG 磁盘,DESIGN.md §5)
    metrics_port: int = field(default_factory=lambda: int(os.getenv("METRICS_PORT", "9100")))
    slot_name: str = field(default_factory=lambda: os.getenv("SLOT_NAME", "vec_stream_slot"))
    slot_check_interval_s: int = field(
        default_factory=lambda: int(os.getenv("SLOT_CHECK_INTERVAL_S", "60"))
    )
    slot_lag_warn_mb: int = field(default_factory=lambda: int(os.getenv("SLOT_LAG_WARN_MB", "256")))

    def __post_init__(self):
        if self.chunk_overlap >= self.chunk_size:
            raise ValueError(
                f"CHUNK_OVERLAP({self.chunk_overlap}) 必须小于 CHUNK_SIZE({self.chunk_size})"
            )
        if self.vector_backend not in ("pgvector", "qdrant"):
            raise ValueError(f"VECTOR_BACKEND 必须是 pgvector|qdrant,得到 {self.vector_backend}")
        if self.embed_dim <= 0 or self.max_doc_chars <= 0:
            raise ValueError("EMBED_DIM / MAX_DOC_CHARS 必须为正数")
        # 表配置校验(TABLES_JSON 是运行时配置,启动期快速失败而非运行时崩)
        for name, tcfg in self.tables.items():
            if tcfg.get("reembed_parent"):
                continue  # 子表不进向量库,无 fields/enrich_sql 要求
            if not tcfg.get("fields"):
                raise ValueError(
                    f"表 {name} 的 fields 不能为空(否则 metadata.title 取 fields[0] 会崩)"
                )
            esql = tcfg.get("enrich_sql")
            if esql:
                # enrich_sql 在 vs_worker 权限下执行,限制为单条 SELECT 且必带租户条件,
                # 防止误配/被篡改的 TABLES_JSON 注入任意 SQL 或泄漏跨租户数据。
                if not esql.strip().lower().startswith("select") or ";" in esql:
                    raise ValueError(f"表 {name} 的 enrich_sql 必须是单条 SELECT(禁分号/DDL/DML)")
                if "%(tenant)s" not in esql:
                    raise ValueError(
                        f"表 {name} 的 enrich_sql 必须带 %(tenant)s 条件(防跨租户混入)"
                    )
        if os.getenv("APP_ENV", "").lower() in {"prod", "production"}:
            weak_dsn_markers = ("change-me", "vs_worker:vs_worker@", "postgres:postgres@")
            if any(marker in self.pg_dsn for marker in weak_dsn_markers):
                raise ValueError("APP_ENV=production 时禁止使用默认/弱 WORKER_PG_DSN")
            if self.vector_backend == "qdrant" and not self.qdrant_api_key:
                raise ValueError(
                    "APP_ENV=production 且 VECTOR_BACKEND=qdrant 时必须设置 QDRANT_API_KEY"
                )
            if self.embed_provider == "openai" and not os.getenv("OPENAI_API_KEY"):
                raise ValueError(
                    "APP_ENV=production 且 EMBED_PROVIDER=openai 时必须设置 OPENAI_API_KEY"
                )
