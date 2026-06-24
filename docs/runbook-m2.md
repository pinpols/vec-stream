# M2 运维手册:embedding 服务 / 评估 / 监控 / 蓝绿 / 水平扩展

ENTERPRISE.md M2「生产必需」的落地与操作流程。代码改动见各模块,本文是**怎么用**。

## 1. Embedding 拆独立服务(item 5)

worker / rag 默认进程内加载模型;设 `EMBED_SERVICE_URL` 即切到独立服务,扩容不再每实例翻倍模型副本。

```bash
# 起服务(独立目录,自带 Dockerfile)
cd embed-service && uvicorn embed_service.app:app --host 0.0.0.0 --port 8200
# worker / rag 指过去(.env 或 export)
export EMBED_SERVICE_URL=http://localhost:8200
```
- 服务统一处理 bge query 前缀(rag 侧用 service 时不再自己拼 `QUERY_PREFIX`)。
- 动态批处理 + MAX_BATCH(413)/ 队列背压(429)。两路 embedding 都 normalize,**向量分布可互换,切换无需重建索引**。

## 2. 质量评估 + 一致性对账(item 6)

```bash
cd eval && export RAG_API_KEY=dev-key-default          # rag 已鉴权,需带 key
python -m vec_stream_eval retrieval   --k 5             # recall@k / MRR(换模型/换 chunk 的客观依据)
python -m vec_stream_eval generation                   # 引用忠实度 / 幻觉(装了 ragas 用 RAGAS,否则轻量版)
python -m vec_stream_eval reconcile                    # 向量数 vs 源表行数漂移检测
```
golden set 在 `eval/golden/queries.jsonl`,扩充标注即提高评估覆盖。

## 3. 告警 + SLO(item 7)

```bash
docker compose -f docker-compose.monitoring.yml up -d   # Prometheus :9090 + Grafana :3000
```
告警规则 `monitoring/alerts.yml`(用 worker 真实指标名):同步延迟 p99>60s、DLQ 增长、slot inactive/lag、worker 掉线。SLO 写在各规则注释。真正发通知需另接 Alertmanager。

## 4. DLQ 工具链增强(item 8)

```bash
python -m vec_stream_worker.dlq_replay --dry-run     # 看
python -m vec_stream_worker.dlq_replay               # 重投(带 replay_count,水位线防乒乓)
```
- 重投次数上限 `DLQ_MAX_REPLAYS`(默认 5):超限的死信落档 PG `dead_letter_archive`,**不再无限重投**,留待人工排查。
- `replay_count` 经 DLQ↔源 topic 往返累加(main.py 透传),达上限触发归档。
- 查归档:`SELECT * FROM dead_letter_archive ORDER BY archived_at DESC;`

## 5. 蓝绿索引切换(item 9)—— 换模型/换 chunk 不停机

向量分布因 normalize 可互换,但换 embedding 模型仍需重算全量。蓝绿避免"边重算边对外服务"的脏读:

1. **建绿索引**:起第二个 worker,**新 collection + 新 group + 新模型**,从 earliest 重放灌满:
   ```bash
   VECTOR_BACKEND=qdrant QDRANT_COLLECTION=doc_vectors_v2 \
   EMBED_MODEL=<new-model> KAFKA_GROUP_ID=vec-stream-worker-v2 \
   python -m vec_stream_worker.main
   ```
2. **评估对比**:rag 分别指向 v1 / v2,跑 `eval retrieval` 比 recall@k / MRR(§2)——**达标才切,有客观依据**。
3. **切流量**:rag 的 `QDRANT_COLLECTION` 指向 v2,重启 rag;确认无误后删 v1 collection。

> pgvector 后端:表名 `doc_vectors` 当前固定,蓝绿需把表名参数化或换 schema/库(同样的双写→评估→切流程)。Qdrant 后端 collection 即天然的切换单元,推荐用它做蓝绿。

索引配置保护:worker 启动会按索引写 `index_metadata`
(`pgvector:doc_vectors` 或 `qdrant:<collection>`),rag 启动会校验
`EMBED_MODEL` / `EMBED_DIM` / `CHUNK_SIZE` /
`CHUNK_OVERLAP` / `VECTOR_BACKEND` / `QDRANT_COLLECTION`。不一致时 rag 直接失败,
这是防止"旧索引用新 query embedding"的硬保护。旧库升级后先手动跑 `02-security.sh`,
再启动对应 worker 写入元数据;迁移窗口必要时可临时 `INDEX_METADATA_CHECK=false`。

## 6. worker 水平扩展验证(item 10)

代码已支持(同 `KAFKA_GROUP_ID` → Kafka 按分区分配;Debezium 按 PK 做 key,同一行事件落同一分区,顺序天然保持):

1. 给 `cdc.public.*` topic 加分区(`kafka-topics --alter --partitions N`)。
2. 起多个 worker 实例,**同一个 `KAFKA_GROUP_ID`**。
3. 验证:各实例消费不同分区子集;无重复处理(末端幂等 upsert + `processed_offsets` 账本可查"处理一次")。

启动期 `SCHEMA_CHECK=true`(默认)会校验配置字段在源表存在,改列/删列**快速失败**而非静默用错数据。
