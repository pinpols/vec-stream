# Hudi Lakehouse 旁路设计

> 状态:设计稿,暂不落地运行环境  
> 目标:在不影响现有向量/RAG 主链路的前提下,新增一条 CDC → Hudi 的分析视图物化链路。

---

## 1. 为什么 Hudi 这条腿顺

Hudi 适合接 Debezium CDC,不是因为它能"凑合消费 Kafka",而是因为 Hudi utilities
内置了 Debezium 专用 source 与 payload:

- `org.apache.hudi.utilities.sources.debezium.PostgresDebeziumSource`
- MySQL 对应 `MysqlDebeziumSource`
- `org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload`

这套路径把 Debezium 的 CDC 语义直接映射到 Hudi 的 upsert/delete:

| Debezium `op` | 含义 | Hudi 语义 |
|---|---|---|
| `c` | create | upsert |
| `r` | snapshot read | upsert |
| `u` | update | upsert |
| `d` | delete | delete |

同时用 Debezium 位点字段做 precombine,避免乱序旧事件覆盖新事件。Postgres 常用
`_event_lsn`,MySQL 则用 binlog 位点字段。

参考:

- Apache Hudi Debezium CDC blog:
  https://hudi.apache.org/blog/2022/01/14/change-data-capture-with-debezium-and-apache-hudi/
- Apache Hudi 0.10.0 release notes(Debezium DeltaStreamer sources):
  https://hudi.apache.org/releases/release-0.10.0/

---

## 2. 和现有项目的关系

现有项目主线保持不变:

```text
Postgres → Debezium(JSON) → Kafka cdc.public.* → Vector Sync Worker → pgvector/Qdrant → RAG
```

新增 Hudi 旁路:

```text
Postgres → Debezium(Avro) → Kafka lake.public.* → HoodieStreamer → Hudi(MOR) → Spark/Trino/Athena
```

两条腿共享:

- 同一个业务库。
- 同一批源表:`article` / `product` / `comment`。
- 同一个业务主键语义。
- 同一个 `tenant_id` 维度。
- 同一套 CDC 变更事实。

两条腿隔离:

- 向量腿继续消费 `cdc.public.*` JSON topic。
- Hudi 腿消费新增 `lake.public.*` Avro topic。
- Hudi 作业失败不影响 worker/RAG。
- worker/RAG 不依赖 Hudi 表。

---

## 3. 为什么不直接复用现有 JSON topic

当前 `debezium/register-postgres.json` 使用:

```json
"key.converter": "org.apache.kafka.connect.json.JsonConverter",
"value.converter": "org.apache.kafka.connect.json.JsonConverter",
"key.converter.schemas.enable": "false",
"value.converter.schemas.enable": "false"
```

这对 Python worker 很轻量,但不是 Hudi Debezium source/payload 最顺的输入形态。
Hudi 官方 Debezium + HoodieStreamer/DeltaStreamer 路线通常走 Avro + Schema Registry,
并需要 schema registry provider 来解析 Debezium envelope/schema。

因此最稳方案不是改现有 connector,而是新增第二个 Debezium connector:

| connector | topic prefix | converter | 消费方 |
|---|---|---|---|
| `vec-stream-pg` | `cdc` | JSON,schemas disabled | Vector Sync Worker |
| `vec-stream-pg-lake` | `lake` | Avro + Schema Registry | HoodieStreamer |

这样 Hudi 实验失败、Schema Registry 配置错误、Streamer 版本冲突,都不会影响现有 RAG 链路。

---

## 4. 范围边界

### 做

- 新增一条 CDC → Hudi 的旁路物化链路。
- 本地最小环境用 MinIO + Schema Registry + Spark/Hudi。
- 每张源表落一张 Hudi 表。
- Hudi 表表达当前态,支持 upsert/delete。
- 用 `tenant_id` 做分区或一级裁剪维度。
- 提供 insert/update/delete smoke 验证。

### 不做

- 不替换现有向量库。
- 不替换 RAG `/search` / `/ask`。
- 不改变 worker 的 CDC 消费逻辑。
- 不把项目升级成通用数据湖/治理平台。
- 不引入 Flink、多源融合、复杂权限系统。
- 不做生产 HA Kafka / Connect distributed / object store HA。

---

## 5. 目标架构

```text
                           ┌──────────────────────────┐
                           │ Postgres source tables    │
                           │ article/product/comment   │
                           └─────────────┬────────────┘
                                         │ WAL / pgoutput
                 ┌───────────────────────┴───────────────────────┐
                 │                                               │
                 ▼                                               ▼
      ┌──────────────────────┐                        ┌──────────────────────┐
      │ Debezium JSON         │                        │ Debezium Avro         │
      │ connector vec-stream  │                        │ connector lake        │
      └──────────┬───────────┘                        └──────────┬───────────┘
                 │                                               │
                 ▼                                               ▼
      ┌──────────────────────┐                        ┌──────────────────────┐
      │ Kafka cdc.public.*    │                        │ Kafka lake.public.*   │
      │ JSON envelope         │                        │ Avro + schema ids     │
      └──────────┬───────────┘                        └──────────┬───────────┘
                 │                                               │
                 ▼                                               ▼
      ┌──────────────────────┐                        ┌──────────────────────┐
      │ Vector Sync Worker    │                        │ HoodieStreamer        │
      │ current semantic view │                        │ Debezium source       │
      └──────────┬───────────┘                        └──────────┬───────────┘
                 │                                               │
                 ▼                                               ▼
      ┌──────────────────────┐                        ┌──────────────────────┐
      │ pgvector / Qdrant     │                        │ Hudi MOR tables       │
      │ RAG retrieval view    │                        │ lakehouse view        │
      └──────────────────────┘                        └──────────────────────┘
```

---

## 6. Debezium lake connector 设计

新增 connector 建议:

- name:`vec-stream-pg-lake`
- topic prefix:`lake`
- slot name:`vec_stream_lake_slot`
- publication:可以复用 `vec_stream_pub`,也可以新建 `vec_stream_lake_pub`
- include list:`public.article,public.product,public.comment`
- converter:Avro converter
- schema registry:`http://schema-registry:8081`

关键点:

- 不动现有 `vec-stream-pg` JSON connector。
- 使用独立 replication slot,避免 lake 消费进度影响向量 connector。
- `tombstones.on.delete` 可以保留 true,但 Hudi delete 依赖的是 Debezium `op=d`
  的 delete event,不是 Kafka null tombstone 本身。
- Kafka retention 必须覆盖 HoodieStreamer 停机窗口;否则 streamer 可能漏读 CDC。

---

## 7. Hudi 表设计

### 表类型

默认选 MOR(Merge-On-Read):

| 表类型 | 适合场景 | 判断 |
|---|---|---|
| MOR | CDC 高频 update/delete | 推荐 |
| COW | 更新少、读多、追求读简单 | CDC 默认不选 |

MOR 写入快,把增量写到 log file,后续通过 compaction 合并。CDC 高频更新下比 COW 更合适。

### 每表一张 Hudi 表

| 源表 | Hudi 表 | record key | precombine | partition |
|---|---|---|---|---|
| `article` | `article_hudi` | `id` | `_event_lsn` | `tenant_id` |
| `product` | `product_hudi` | `id` | `_event_lsn` | `tenant_id` |
| `comment` | `comment_hudi` | `id` | `_event_lsn` | `tenant_id` |

说明:

- `recordkey.field = id`:和向量腿的 source PK 对齐。
- `precombine.field = _event_lsn`:Postgres CDC 乱序时按 LSN 取新版本。
- `partitionpath.field = tenant_id`:延续多租维度,便于查询裁剪。

如果未来 `tenant_id` 基数太高导致小文件明显,再评估:

- `tenant_id/date`
- `date`
- 不分区 + secondary index / metadata table

---

## 8. HoodieStreamer 配置边界

每张表一份配置,例如 `hudi/article.properties`。核心配置维度:

```properties
# source
hoodie.streamer.source.class=org.apache.hudi.utilities.sources.debezium.PostgresDebeziumSource
hoodie.streamer.source.kafka.topic=lake.public.article
hoodie.streamer.source.kafka.bootstrap.servers=kafka:29092

# payload / CDC semantics
hoodie.datasource.write.payload.class=org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload
hoodie.datasource.write.precombine.field=_event_lsn

# keys
hoodie.datasource.write.recordkey.field=id
hoodie.datasource.write.partitionpath.field=tenant_id
hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.SimpleKeyGenerator

# table
hoodie.datasource.write.table.type=MERGE_ON_READ
hoodie.table.name=article_hudi
```

具体属性名会随 Hudi 版本在 `deltastreamer` / `streamer` 命名上有差异。落地时以选定 Hudi
版本的 HoodieStreamer 文档为准,但语义保持上面这几组。

---

## 9. 本地最小环境

新增 overlay 文件建议为 `docker-compose.lake.yml`:

- `schema-registry`
- `minio`
- `spark-hudi` 或 `hudi-streamer`
- 可选 `hive-metastore`

第一版可以不引入 Hive Metastore:

- Hudi 表直接写到 `s3a://vec-stream-lake/hudi/<table>`。
- smoke 用 Spark SQL 或 `spark-shell` 读取 Hudi path 验证。
- 后续再加 Hive/Trino/Athena catalog。

MinIO bucket:

- `vec-stream-lake`

Hudi base paths:

- `s3a://vec-stream-lake/hudi/article`
- `s3a://vec-stream-lake/hudi/product`
- `s3a://vec-stream-lake/hudi/comment`

---

## 10. Smoke 验证

新增 `scripts/hudi-smoke.sh`,与现有 `scripts/smoke.sh` 分开。

验证顺序:

1. 启动基础设施 + lake overlay。
2. 注册 JSON connector:`./debezium/register.sh`。
3. 注册 Avro lake connector:`./debezium/register-lake.sh`。
4. 启 HoodieStreamer article 作业。
5. 插入 article:
   - Postgres `INSERT`
   - Hudi 当前态可查到该 `id`
6. 更新 article:
   - Postgres `UPDATE title/body/status`
   - Hudi 当前态只保留新值
7. 删除 article:
   - Postgres `DELETE`
   - Hudi 当前态查不到该 `id`

删除验证的关键是确认 streamer 处理了 Debezium `op=d` event。Kafka null tombstone 可以存在,
但不能用它替代 `op=d`。

---

## 11. 运维注意事项

### Kafka retention

HoodieStreamer 从 Kafka 消费 CDC。停机时间超过 retention 会漏事件,导致 Hudi 表漂移。
本地可以用较长 retention;生产需要监控 lag。

### Compaction

MOR 需要 compaction:

- 本地 smoke 可以 inline compaction。
- 长期运行建议 async compaction。
- 小文件明显时再加 clustering。

### Schema 演进

lake 腿建议强制 Avro + Schema Registry:

- 加列:允许 Hudi schema 演进。
- 改列/删列:仍然需要兼容性策略,不能无脑放行。
- 破坏性 DDL 需要运维 runbook。

### Bootstrap / re-snapshot

首次建表可以让 Debezium `snapshot.mode=initial` 产生 `op=r` snapshot event,Hudi 当 upsert 写入。
如果 Kafka retention 内没有完整历史,需要重新 snapshot 或重新建 lake 表。

---

## 12. 与现有向量腿的呼应

两条腿都从 CDC 重建当前态,只是物化目标不同:

| 维度 | 向量腿 | Hudi 腿 |
|---|---|---|
| Kafka topic | `cdc.public.*` | `lake.public.*` |
| 格式 | JSON envelope | Avro + Schema Registry |
| 消费进程 | Python worker | HoodieStreamer |
| 目标 | pgvector/Qdrant | Hudi MOR |
| 查询场景 | 语义检索/RAG | 分析/数仓/time travel |
| 幂等 key | `tenant:table:pk:chunk` | `id` record key |
| 乱序处理 | 源库当前态反查 + hash | `_event_lsn` precombine |
| 删除 | delete vectors | Hudi delete |

这让项目从单一语义检索底座,自然扩展成"CDC 多视图物化"架构,但不改变主线边界。

---

## 13. 分阶段落地

### H0:只写设计

- 本文档。
- README 增加链接。
- 不新增容器、不注册 connector。

### H1:本地最小可跑

- `docker-compose.lake.yml`
- Schema Registry + MinIO + Spark/Hudi runner
- `debezium/register-lake.sh`
- `hudi/article.properties`
- `scripts/hudi-smoke.sh`
- 只跑 article insert/update/delete。

### H2:多表与运维

- product/comment 配置。
- compaction 配置。
- smoke 覆盖三表。
- lake lag / streamer health 指标。

### H3:查询层

- Hive Metastore / Trino / Athena 任选其一。
- 增量查询和 time travel 示例。

---

## 14. 当前不改名

现阶段不建议改项目名。`vec-stream` 仍以向量/RAG 为主线,Hudi 是 CDC 旁路物化。
如果 Hudi、更多 sink 和统一运维成为一等主线,再考虑把项目定位升级为 CDC 多 sink 平台。
