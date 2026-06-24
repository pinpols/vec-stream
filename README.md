# vec_stream

把业务数据库的变更(CDC)**实时**同步进向量数据库,作为 RAG / 语义检索的实时数据底座。

```
MySQL / PostgreSQL → Debezium → Kafka → Vector Sync Worker → 向量库(pgvector/Qdrant) → RAG 服务
```

数据一改,向量秒级更新;数据删除,向量同步失效。区别于「定时全量重建索引」的传统做法。

## 文档

- 设计文档:[`docs/DESIGN.md`](docs/DESIGN.md) — 架构、核心难点、选型、分阶段路线图
- 企业级演进规划:[`docs/ENTERPRISE.md`](docs/ENTERPRISE.md) — 7 大领域差距、该做/按需/越界判定、M1–M3 路线

## 快速开始(基础设施)

> 阶段 0:先把 CDC 链路的底座跑起来(Postgres + Kafka + Debezium),再接 Sync Worker。

```bash
# 0) 准备凭据:复制 .env.example → .env,改成强随机密码(.env 已 gitignore)
cp .env.example .env && $EDITOR .env

# 1) 启动 Postgres(逻辑复制 + 最小权限角色 + RLS)、Kafka、Kafka Connect
#    首次初始化会跑 db/init/01-init.sql(schema)+ 02-security.sh(角色/RLS/账本/publication)
docker compose up -d

# 2) 注册 Debezium connector(用最小权限 vs_debezium,密码由脚本从 .env 注入,不落盘明文)
./debezium/register.sh

# 查看 connector 状态
curl http://localhost:8083/connectors/vec-stream-pg/status
```

> 旧环境升级注意:Postgres 官方镜像只会在 fresh `pgdata` 卷首次初始化时执行
> `db/init/02-security.sh`。如果你已有旧卷,新增的最小权限角色、RLS、
> `processed_offsets` 和 publication 不会自动创建。可删除卷重建,或手动应用一次:
>
> ```bash
> set -a && source .env && set +a
> docker exec \
>   -e POSTGRES_USER="$POSTGRES_USER" \
>   -e POSTGRES_DB="$POSTGRES_DB" \
>   -e DEBEZIUM_PASSWORD="$DEBEZIUM_PASSWORD" \
>   -e WORKER_PASSWORD="$WORKER_PASSWORD" \
>   -e RAG_PASSWORD="$RAG_PASSWORD" \
>   vec-stream-postgres bash /docker-entrypoint-initdb.d/02-security.sh
> ```

## 目录结构

```
vec-stream/
├── docs/DESIGN.md          # 设计文档
├── docker-compose.yml      # 基础设施:Postgres / Kafka / Kafka Connect(Debezium)
├── db/init/                # Postgres 初始化(逻辑复制 + pgvector + 示例表)
├── debezium/               # Debezium connector 配置
├── worker/                 # Vector Sync Worker(Python:CDC 消费 → embedding → pgvector)
└── rag/                    # RAG 服务(Python/FastAPI:/search,阶段 2 加 /ask)
```

## 运行 Worker 与 RAG 服务

Embedding 用本地 **BAAI/bge-small-zh-v1.5**(512 维,免 key,首次运行自动下载模型)。

```bash
# Vector Sync Worker:消费 CDC → embedding → 写 pgvector
cd worker
uv sync
set -a && source ../.env && set +a
uv run python -m vec_stream_worker.main

# RAG 服务(另一个终端)
cd rag
uv sync
set -a && source ../.env && set +a
uv run uvicorn vec_stream_rag.app:app --port 8000

# 语义搜索验证
curl -s -X POST http://localhost:8000/search \
  -H 'X-API-Key: dev-key-default' \
  -H 'Content-Type: application/json' \
  -d '{"query": "向量检索扩展怎么选"}' | jq
```

> 启动注意(资源紧张的机器):`docker compose up -d` 后若 Connect 起不来,
> 按「先等 Kafka healthy → 再单独 `docker start vec-stream-connect`」串行启动。

## 运维

- **多表**:Debezium `table.include.list` 加表 + `worker/config.py` 的 `DEFAULT_TABLES`
  (或 `TABLES_JSON` 环境变量)配字段映射;worker 按 `cdc.public.*` 正则订阅,无需改订阅。
- **slot lag 监控**:worker 内置后台线程,每 60s 查 `pg_replication_slots`,
  lag 超 `SLOT_LAG_WARN_MB`(默认 256MB)或 slot 失活时打 WARNING——
  slot 不消费会撑爆 PG 磁盘,这条日志要接告警。
- **DLQ 重投**:`uv run python -m vec_stream_worker.dlq_replay [--dry-run|--limit N]`;
  只处理启动时水位线之前的消息(防止重投失败回流后被同一进程再次捡起死循环)。
- **错误分类**:基础设施瞬时故障(PG/Qdrant 连不上)无限退避重试、阻塞分区、不进 DLQ;
  数据性错误(解析失败等)重试 3 次进 DLQ。upsert 一律按**源库当前态**重建文档
  (CDC 事件只当触发器),所以重投旧消息、跨表事件乱序都收敛到正确状态。
- **metadata 刷新**:文本没变只改结构化字段(如 status)时,worker 只刷
  `doc_vectors.metadata` 不重新 embedding。
- **跨表文档**:父表配 `enrich_sql`(反查关联文本,参与 hash);子表配
  `reembed_parent: {table, fk}` —— 子表任何变更(含删除)触发父行重新 embed,
  评论等关联内容随主文档可搜。见 `worker/config.py` 的 article/comment 示例。
- **监控指标**:worker 在 `:9100/metrics`(qdrant worker `:9101`)暴露 Prometheus 指标:
  `vec_stream_events_total{table,action}` / `vec_stream_chunks_embedded_total`(成本)/
  `vec_stream_sync_delay_seconds`(CDC→向量延迟)/ `vec_stream_dlq_sent_total` /
  `vec_stream_dlq_backlog` / `vec_stream_slot_lag_bytes` / `vec_stream_slot_active`;
  rag 提供 `GET /stats`(向量总量、按表分布)。
- **已知边界**:上游 DDL 变更未做兼容(约定不做破坏性 DDL);换 embedding 模型 = 全量重建。
- **向量库后端**:`VECTOR_BACKEND=pgvector|qdrant`(worker 与 rag 都认)。切 Qdrant:
  `docker compose up -d qdrant`,worker 用**新 consumer group** 从 Kafka 重放即全量回填
  (`VECTOR_BACKEND=qdrant KAFKA_GROUP_ID=vec-stream-worker-qdrant`);超出 Kafka retention
  的历史要走 Debezium re-snapshot。双 worker 双 group 可让两个库并行保持同步。
- **冒烟验证**:基础设施、connector、worker、rag 都启动后,运行
  `bash scripts/smoke.sh`;脚本会检查 `/healthz`、connector 状态、插入一条
  smoke 文章并用带 API key 的 `/search` 验证召回。

## 路线图

- ~~**阶段 0**:Debezium 监听一张表 → Kafka → Worker 处理 INSERT → 写 pgvector → `/search` 能搜到~~ ✅ 2026-06-10
- ~~**阶段 1**:UPDATE / DELETE、确定性 ID、hash 去重、DLQ~~ ✅ 2026-06-10(DLQ topic:`cdc.dlq`,失败消息带 error/source_offset header)
- ~~**阶段 2**:`/ask` 生成 + rerank + 多租过滤~~ ✅ 2026-06-10(rerank 用 bge-reranker-base,`RERANK_ENABLED=false` 可关)。**生成层统一走 OpenAI 兼容协议**:`OPENAI_BASE_URL` 指 agent-ctl 网关或任意兼容服务(OpenAI/DeepSeek/通义/Ollama/vLLM;网关侧再处理 Claude/路由/回退),见 `.env.example`
- ~~**阶段 3**:切 Qdrant、跨表文档、监控指标~~ ✅ 2026-06-10(全部阶段完成)

详见 [`docs/DESIGN.md`](docs/DESIGN.md) §7。
