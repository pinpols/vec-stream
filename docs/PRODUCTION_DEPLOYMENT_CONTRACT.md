# 生产部署契约

本仓库提供 CDC 多 sink 参考实现和本地可跑环境。生产部署不应直接照搬本地
`docker compose` 的单节点拓扑,而应把下面能力交给目标平台。

## 平台必须提供

| 领域 | 生产要求 |
| --- | --- |
| Kafka | 3+ broker、RF>=3、min ISR、SASL/TLS、ACL、磁盘/retention 告警 |
| Kafka Connect | distributed mode、多 worker、内部 topic RF、REST 管理面鉴权或内网隔离 |
| Postgres 源库 | HA、备份恢复、逻辑复制槽监控、DDL 变更流程、`REPLICA IDENTITY FULL` 契约 |
| 向量库 | Qdrant API key/TLS 或托管向量库;pgvector 场景需独立向量库实例和备份 |
| Lakehouse | 生产级 S3/对象存储、认证 catalog、Spark on Kubernetes/YARN/托管 Spark |
| Secret | Vault/KMS/云 Secret Manager;不得用 `.env` 明文作为生产密钥源 |
| 管理面 | Connect、Qdrant、MinIO/S3 Console、Iceberg REST、Prometheus、Grafana、Spark UI 不暴露公网 |
| 日志告警 | 集中日志、Alertmanager/通知路由、SLO 阈值按真实负载校准 |

## 环境变量边界

生产必须显式设置:

- `APP_ENV=production`
- `WORKER_PG_DSN`、`RAG_PG_DSN`、`EVAL_PG_DSN`:使用最小权限账号和强密码。
- `KAFKA_BOOTSTRAP`:指向生产 Kafka;同时在部署平台侧配置 SASL/TLS/ACL。
- `QDRANT_API_KEY`:当 `VECTOR_BACKEND=qdrant` 时必填。
- `EMBED_EGRESS_ALLOWED=true`:仅当允许源文档发送到外部 embedding provider 时设置。
- `LLM_EGRESS_ALLOWED=true`:仅当允许 `/ask` 把召回内容发送到外部 LLM provider 时设置。
- `OPENAI_API_KEY` / 受控网关凭据:只从 secret manager 注入。
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`:最小权限对象存储凭据。

生产禁止:

- 使用 `dev-key-default`、`change-me-*`、`vs_worker:vs_worker@...`、`vs_rag:vs_rag@...`。
- 让业务流量直接访问 Connect REST、Spark UI、Prometheus、Grafana 等管理面。
- 多个 Hudi writer 同时写同一 base path,除非已配置生产级锁服务。
- 在未验证 schema contract / smoke / 回放幂等前切换 CDC topic 或表 schema。

## 发布准入

每次发布至少执行:

```bash
bash scripts/validate.sh
uvx ruff@0.8.6 format --check --config ruff.toml worker rag eval embed-service spark-lake
uvx ruff@0.8.6 check --config ruff.toml worker rag eval embed-service spark-lake
(cd worker && uv run --group dev python -m pytest -q)
(cd rag && OTEL_ENABLED=false uv run --group dev python -m pytest -q)
(cd eval && uv run --group dev python -m pytest -q)
(cd embed-service && uv run --group dev python -m pytest -q)
```

湖腿发布还应在目标环境跑:

```bash
bash scripts/hudi-smoke.sh
bash scripts/iceberg-smoke.sh
bash scripts/stream-smoke.sh
```

## 上线后必须观察

- `vec_stream_sync_delay_seconds` p99 是否低于业务 SLO。
- `vec_stream_dlq_sent_total` 是否持续为 0。
- `vec_stream_dlq_backlog` 是否可被 replay 清空。
- `vec_stream_slot_active` 是否为 1。
- `vec_stream_slot_lag_bytes` 是否低于磁盘和 RPO 阈值。
- Spark Hudi/Iceberg stream input/processing rate 是否匹配 Kafka 写入速率。
