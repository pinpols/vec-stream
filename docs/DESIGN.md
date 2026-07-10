# CDC 实时多 sink 分发底座(向量 + lakehouse) · 设计文档

> 工作代号:**cdc-vector-rag**
> 版本:v0.2 · 初稿日期:2026-06-09
> 状态:向量主链路已可跑;**Hudi + Iceberg 双 lakehouse sink 统一到 Spark(连续流 Structured Streaming),本地端到端验证通过**(统一 cdc.* JSON,insert/update/delete 全绿)

以业务数据库的变更(CDC)为单一事实源,经 Kafka 解耦后**实时扇出到多个下游 sink**(统一消费 `cdc.public.*` JSON,见 `docs/TOPICS.md`):
- **向量 sink**(本项目核心):构建可检索文档 → 向量化 → 写向量库,作为 RAG / 语义检索的实时数据底座;
- **Lakehouse sink**:统一 Spark 引擎(`spark-lake`)写两种主流湖表——**Hudi**(Spark 主场,`df.write.format("hudi")` upsert)与 **Iceberg**(Spark v2 `MERGE INTO`)。
  > 选型:湖腿都收敛到 **Spark**(各自主流引擎、开源友好、不背 Flink 集群/连接器坑)。**连续流(Structured Streaming,~10-20s)默认**,
  > 批量(幂等读全 topic)做回填;共用同一段解析+写入。两条湖腿与向量腿共用同一份 cdc.* 流。
  > 延迟分层:向量腿连续消费亚秒~秒级(核心实时),湖腿微批十几秒(准实时)。真·亚秒需 Flink(已退役)。
  > 历史尝试(HoodieStreamer / Flink→Iceberg/Hudi / pyiceberg)已退役,见 git 历史。

共同的差异化:区别于「定时全量重建」的传统做法——数据一改,下游秒级更新,删除即失效;
各 sink 独立消费、互不阻塞,任一条腿故障不影响其他。

---

## 1. 目标与范围边界

### 1.1 目标(做什么)

- 监听 MySQL / PostgreSQL 的行级变更(INSERT / UPDATE / DELETE)
- 以 Kafka 为解耦总线,把同一份 CDC 流**扇出到多个独立 sink**
- **向量 sink**:将「可检索文档」抽取、切分、向量化后写入向量库,并提供 RAG / 语义搜索 API(检索 → 重排 → 生成)
- **lakehouse sink**:统一 Spark 引擎(`spark-lake`)把行级变更物化为 Hudi / Iceberg 表(连续流 Structured Streaming,`df.write` upsert / `MERGE INTO`),供分析 / 数仓回填
- 全程**幂等**:同一条变更重放结果一致;数据删除则下游同步失效

### 1.2 范围边界(不做什么)

明确划线,避免项目无限膨胀:

| ✅ 做 | ❌ 不做 |
|---|---|
| 单库 / 单数据源的 CDC → 多 sink 扇出 | 多源异构数据融合(多上游 join) |
| 向量 sink + lakehouse(Hudi + Iceberg)sink | 通用 ETL / 数据治理平台 |
| 行级变更驱动的增量同步 | 自托管 Spark / Flink 集群(湖侧用 Spark local 模式轻量落地) |
| 向量检索 + 基础 RAG 问答 | 复杂 Agent 编排 / 多轮对话记忆 |
| 单表文档 + 简单跨表反查拼接 | 流式多表 JOIN(交给后续可选的 Flink CDC 阶段) |
| 多租隔离(payload 过滤) | 行级权限 / 细粒度 ACL |

> **定位**:本项目是一个 **CDC 实时多 sink 分发底座**——以「Kafka 为单一事实源、多个下游各自消费」为骨架,当前落地三条 sink:向量(RAG 底座)、Hudi(分析视图)、Iceberg(分析视图)。核心练的是 **CDC(Debezium/WAL)+ Embedding + 向量检索 + RAG + lakehouse 物化** 这套技术栈,**不是**再造分布式批处理 / 数据治理平台。凡是会把项目拖向「重运维平台」的需求(自托管 Spark/Flink 集群、K8s 调度、多源融合),一律推迟或限定在最小演示范围(湖侧为 Spark local runner,生产可迁移到托管 Spark)。

---

## 2. 总体架构

```
┌─────────────────┐
│ 业务数据库       │  MySQL binlog / PostgreSQL WAL(逻辑复制槽 pgoutput)
│ MySQL / Postgres│
└────────┬────────┘
         │ 变更事件(c=create / u=update / d=delete / r=snapshot)
         ▼
┌─────────────────┐
│ Debezium        │  Kafka Connect 模式,standalone 起步
│ (Kafka Connect) │
└────────┬────────┘
         ▼
┌─────────────────┐
│ Kafka CDC Topic │  统一:每表一个 topic cdc.public.<table>(JSON,schemas.enable=false)
│ cdc.public.*    │  单 Debezium connector / 单复制槽,详见 docs/TOPICS.md
└────────┬────────┘
         │  扇出:同一份 cdc.public.* 流,各 sink 独立消费、互不阻塞
         ├──────────────────────┬──────────────────────┐
         ▼                      ▼                      ▼
┌──────────────────────┐  ┌──────────────────────────────────────┐
│ ① Vector Sync Worker │  │ ②③ spark-lake(统一 Spark 引擎)      │
│  c/r→upsert          │  │  hudi   : df.write.format("hudi") upsert│
│  u→re-embed d→delete │  │           d→_hoodie_is_deleted          │
│  文档构建/hash 去重  │  │  iceberg: MERGE INTO(v2)               │
│  确定性向量 ID       │  │           d→DELETE 其余 UPSERT          │
│  批量 Embedding      │  │  解析 envelope,按 PK 合并(批量幂等)  │
│  upsert/delete 向量库│  └───────┬──────────────────┬─────────────┘
└────────┬─────────────┘          ▼                  ▼
         ▼                  ┌──────────────┐  ┌──────────────┐
┌─────────────────┐         │ Hudi 表(MOR)│  │ Iceberg 表   │
│ 向量数据库       │        │ MinIO/s3a    │  │ REST+MinIO   │
│ pgvector/Qdrant │         │ 分析/数仓     │  │ 分析/数仓     │
└────────┬────────┘         └──────────────┘  └──────────────┘
         ▼
┌─────────────────┐
│ RAG 服务         │  检索 → (可选)rerank → OpenAI 兼容 API 生成
│  /search /ask   │
└─────────────────┘
```

### 2.1 设计原则

1. **解耦**:CDC、同步、向量库、RAG 之间靠 Kafka / HTTP 解耦,任一环节可独立替换(例如后期把文档拼接换成 Flink CDC,只改 Sync Worker)。
2. **幂等优先**:全链路按「至少一次投递」设计,靠确定性 ID + upsert 保证最终一致。
3. **成本敏感**:Embedding 是最贵的一环,默认所有变更先做 hash 去重再决定是否调用。
4. **小步可跑**:MVP 先打通最窄闭环,难点(UPDATE/DELETE、跨表、rerank)分阶段加。

---

## 3. 组件设计

### 3.1 CDC 采集层(Debezium)

- **PostgreSQL**:逻辑复制(`pgoutput` 插件),需 `wal_level=logical`,创建 replication slot + publication。
- **MySQL**:binlog(`ROW` 格式),需开启 binlog 并授予 `REPLICATION SLAVE` 权限。
- **快照策略**:`snapshot.mode=initial`——首次全量快照(事件类型 `r`)后转增量流(`c/u/d`)。
- **Topic 命名**:`cdc.<server>.<schema>.<table>`,每表一个 topic,key = 主键。
- **Schema 变更**:PostgreSQL connector **没有** schema history topic 机制(那是 MySQL 等 binlog 系 connector 的),它在读取时实时从数据库元数据获取表结构。MVP 阶段约定上游不做破坏性 DDL,后续再做兼容策略。

> 关键学习点:replication slot 不消费会撑爆磁盘 WAL;snapshot 与 streaming 的衔接点(LSN / GTID);Debezium 的 `before` / `after` 镜像。

### 3.2 Kafka 层

- 每张表一个 CDC topic,分区数按吞吐设定(MVP 可 1~3 分区)。
- key 用主键 → 保证同一行的变更进同一分区,**有序**(避免 UPDATE 乱序覆盖)。
- 配 DLQ topic(`cdc.dlq`)收容处理失败的事件。

### 3.3 Vector Sync Worker(核心)

整条管道最难、最值得做的部分。处理流程见架构图 1~7 步。下面是几个硬问题的设计:

#### (a) 变更类型分流

| Debezium op | 含义 | 处理 |
|---|---|---|
| `r` | snapshot 读 | 当作 upsert |
| `c` | INSERT | 构建文档 → embed → upsert |
| `u` | UPDATE | 比对 hash:变了则删旧 chunk + 重新 embed upsert;没变只刷 metadata |
| `d` | DELETE | 从 before 镜像取 `tenant_id` + PK,删除该行全部 chunk 向量 |

**核心原则:CDC 事件只当「触发器」,落库始终收敛到源库当前态。** upsert 路径(c/r/u)拿到事件后**反查一次源库**取当前行,以它为准重建文档——而非信任事件里的 `after` 镜像。这同时消解两类竞态:① DLQ 重投的旧 `after` 覆盖新状态;② 跨表反查与本表事件的乱序。反查发现源行已不存在 → 直接删向量(等价 delete)。

**边界与失败分类**(均有回归测试,见 `worker/tests/`):

- **空内容也要删向量**:UPDATE 把可检索字段全清空时,源行还在但 `source_text` 为空 → **删旧向量**,否则留下指向空行的「幽灵向量」仍可被检索。
- **falsy 字段不丢**:拼 `source_text` 时只跳过 `None`(列缺失),保留 `0`/`False`/`""`——否则数值零/空串被静默丢出文本,导致 hash 漂移 + 内容缺失且无报错。
- **NaN/Inf 向量拒绝**:embedding 含非有限值(模型 bug/量化误差)当数据性错误进 DLQ,不写垃圾向量。
- **失败分类决定重试策略**:基础设施瞬时故障(PG/向量库/embed-service 连接失败超时、HTTP 429/5xx)→ **无限退避重试不进 DLQ**(进了也修不好);数据性错误(解析失败/缺字段/NaN)→ 有限重试后进 DLQ。DLQ header 里的异常字符串经脱敏(抹掉 DSN 密码)。
- **TOAST 大列占位符**:pgoutput 对 UPDATE 中**未变更的 TOAST 大列**在 after 镜像填占位符 `__debezium_unavailable_value`(REPLICA IDENTITY FULL 只保证 before 完整;bytea 列经 JSON base64 后是其 base64 形态)。worker 不受影响(upsert 一律反查源库当前态,不信 after);Hudi/Iceberg 湖腿已对字符串列做「占位符 → 回退 before」(`spark-lake/lakehouse_logic.py`);Flink→Paimon 参考腿 SQL 层做不了(debezium-json 拆 -U/+U,无状态拿不到 before),**记为该腿已知边界**,见 `flink-paimon/sql/cdc_to_paimon.sql` 头注。
- **tenant_id 视为不可变(变更有在线兜底)**:业务上 tenant_id 不应变;若真发生 UPDATE 改 tenant_id,worker 会按事件 before 镜像的旧租户**顺带删一次旧向量**(防孤儿),新向量按反查后的新租户写入。该兜底只覆盖被正常消费的事件——事件丢失/DLQ 归档跳过时仍可能留孤儿,需离线 reconcile 对账清理。
  **湖腿行为差异(参考级,不改写入逻辑)**:三条湖腿对 t1→t2 的处理不一致——
  - Hudi:recordkey=`tenant_id,id`,t1→t2 后是**新 key**,upsert 只写新行,t1 旧行残留;
  - Iceberg:`MERGE ON (tenant_id,id)` 同理,旧 (t1,id) 行不匹配、不被更新/删除,残留;
  - Paimon(Flink 参考腿):debezium-json 的 -U/+U retraction 会撤回旧行,**行为正确**。
  残留旧行意味着按 t1 过滤的分析查询仍能看到已迁走的数据(参考级可接受;清理 SQL 见
  `docs/runbook-lakehouse.md` 的「tenant_id 变更清理」)。

#### (b) 确定性向量 ID(幂等基石)

向量 ID 由业务主键派生,**不用随机 UUID**:

```
vector_id = sha256(f"{tenant_id}:{table}:{primary_key}:{chunk_index}")
```

好处:同一行重放 → 同一组 ID → upsert 天然幂等;删除时按 `tenant_id:table:pk` 前缀能定位并删掉这行的全部 chunk。

> 实现注意:一行更新后 chunk 数量可能变化(文本变长/变短)。删除时**不能**只删 `chunk_index` 已知的几个,要么先按行删全部再写新的,要么记录该行上次的 chunk 数。MVP 采用「先删该行所有 chunk → 再写新 chunk」最稳。

#### (c) 源文本 hash 去重(成本核心)

```
source_text = concat(可检索字段...)
text_hash   = sha256(source_text)
```

`text_hash` 直接存在向量记录里(pgvector 的 `text_hash` 列 / Qdrant payload),变更进来先读该行上次的 hash,相同则跳过最贵的 embedding,**只刷 metadata**(`status` 等过滤字段必须跟上,否则过滤检索用旧值)。生产中可省 80%+ embedding 调用。

> 一个隐蔽缺口已堵:hash 命中但目标向量实际已不存在(并发删/丢失)时,不能假装「刷新成功」——Qdrant `update_metadata` 返回真实命中数,0 命中则**回退到全量 embed+upsert**(pgvector 同理按 rowcount 判断)。

#### (d) 文档构建与字段策略

- **embed 字段**:文本类(标题 / 描述 / 正文)拼接成 `source_text`。
- **payload(metadata)字段**:结构化字段(`tenant_id` / `status` / 时间 / 分类)存进向量库 payload,用于**先过滤后召回**。
- **跨表拼接**:MVP 用「收到变更后反查一次 DB」把关联数据拉全(简单、够用)。仅当吞吐压力大、需要实时流 JOIN 时,才评估引入 Flink CDC——架构已解耦,替换成本低。

#### (e) Chunk 切分

- 按 token 长度切(如 512 token,overlap 50),保留来源 PK / 字段定位。
- 每个 chunk 一条向量记录,共享同一行的 payload + 各自 `chunk_index`。

#### (f) 与现有批处理经验的复用

Sync Worker 可照搬 `batch-worker-*` 的 **CLAIM → EXECUTE → REPORT** 幂等思路:消费即「认领」,处理完提交 offset 即「上报」,失败进 DLQ 重试。分布式经验在这里直接变现。

### 3.4 Embedding 层

- **重要**:生成模型和 embedding 模型是两条链路。Embedding 需单独选模型,生成层只负责 RAG 最后的回答。
- **可插拔后端**(`EMBED_PROVIDER` / `EMBED_SERVICE_URL`):① 进程内 SentenceTransformer(本地 bge,默认);② 独立 `embed-service`(HTTP,动态批处理,可单独扩展);③ OpenAI 兼容 embeddings(需 `EMBED_EGRESS_ALLOWED=true`)。worker 写入侧与 rag 检索侧**必须同后端同模型**,否则向量分布不一致召回失真。
- **批量调用**:Worker 攒一批 chunk 一次性请求,提升吞吐、降成本。
- **维度锁死 + 启动 fail-fast**:选定模型后维度锁死;换维度 = 蓝绿重建新索引(不能在旧索引上切)。Qdrant 后端对**已存在的 collection 也校验维度**,不一致直接启动失败,避免静默接受错维向量;rag/worker 间还有 `index_metadata` 校验 embedding 模型/维度/chunk 参数一致。
- **远程后端的失败语义**:走 HTTP 的后端,连接失败/超时归入瞬时故障无限退避;服务端 429/5xx 同样当瞬时(过载应退避而非进 DLQ);仅 4xx(请求过大/非法)算数据性错误。

### 3.5 向量数据库

**MVP 用 pgvector,进阶切 Qdrant。**

| | pgvector | Qdrant |
|---|---|---|
| 起步成本 | 极低(你已有 PG) | 需多起一个服务 |
| 过滤检索 | SQL where + 向量 | 原生 payload filter + HNSW 调优 |
| 学习价值 | 快速跑通 | 学专用向量库的工程细节 |
| 处理账本一致性 | **与向量写入同 PG 事务,原子** | best-effort(Qdrant 无事务,写完单独记 PG 账本,失败只 warn) |
| 建议 | 第一阶段 | 第二阶段 |

> **一致性分层(重要,别混为一谈)**:pgvector 后端里「向量写入 + processed_offsets 账本」在**同一个 PG 事务**内提交,原子。Qdrant 后端做不到(Qdrant 非事务存储),账本是写完 Qdrant 后**单独 best-effort** 写 PG,两者之间无原子性——账本仅作审计参考,不是强一致真相。两者的**投递语义都是至少一次 + 确定性 ID 幂等**(崩溃重放收敛),但审计账本的可靠性 pgvector 强于 Qdrant。选 Qdrant 时这点要进运维认知。

**Schema(pgvector 示例)**:

```sql
CREATE TABLE doc_vectors (
    vector_id   TEXT PRIMARY KEY,          -- 确定性 ID
    tenant_id   TEXT NOT NULL,             -- 多租过滤
    source_table TEXT NOT NULL,
    source_pk   TEXT NOT NULL,
    chunk_index INT  NOT NULL,
    text_hash   TEXT NOT NULL,             -- 去重用
    content     TEXT NOT NULL,             -- 原文 chunk(便于召回展示)
    metadata    JSONB,                     -- 业务过滤字段
    embedding   vector(1024),              -- 维度随模型
    updated_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX ON doc_vectors USING hnsw (embedding vector_cosine_ops);
CREATE INDEX ON doc_vectors (tenant_id, source_table, source_pk);
```

> 多租隔离(双保险):tenant 从 API key 推导(请求体的 `tenant_id` 被忽略),pgvector 后端**既走 PG RLS(`SET app.tenant` 事务局部)又带显式 `WHERE tenant_id=`**——即便漏写 WHERE,RLS 兜底不泄漏;Qdrant 后端用 payload filter 的 `must` 条件。先按 tenant 过滤再向量召回。

### 3.6 RAG 服务

接口:

- `POST /search` — 纯语义搜索:query → embed → 向量召回(带 tenant 过滤)→ (可选)rerank → 返回 chunk 列表。
- `POST /ask` — RAG 问答:上面召回结果作为上下文 → OpenAI 兼容 API 生成答案,带引用来源。该接口涉及把租户召回内容发给 LLM 端点,需显式设置 `LLM_EGRESS_ALLOWED=true`;未允许时 `/search` 仍可用。

链路:`query embedding → 向量召回 topK → rerank 重排 topN → 拼 prompt → OpenAI 兼容 API 生成 → 附 source`。

> rerank 是 RAG 质量提升最明显的一步(召回拿 topK=50,重排后取 topN=5 喂模型),MVP 可先不做,第二阶段加。

---

## 4. 关键数据流(端到端示例)

**UPDATE 一行 `article` 表:**

```
1. Debezium 捕获 WAL → 发 Kafka(op=u, after={id:42, title:"新标题", body:"..."})
2. Sync Worker 消费(key=42,保证有序)
3. 构建 source_text = title + body;算 text_hash
4. 查元数据:id=42 上次 hash != 当前 → 文本变了,继续
5. 删除 doc_vectors 中 source_pk=42 的所有 chunk
6. 切分新文本 → N 个 chunk → 批量 embedding
7. 生成确定性 vector_id → upsert N 条到 pgvector,更新 text_hash
8. RAG /search 立即能搜到新内容,旧内容不再命中
```

**DELETE 一行:** Debezium op=d → Worker 按 `source_pk` 删除全部 chunk → 立即从检索结果消失。

---

## 5. 一致性、容错与运维

| 关注点 | 方案 |
|---|---|
| 投递语义 | 至少一次 + 确定性 ID upsert = 最终幂等;事件即触发器,落库收敛到源库当前态 |
| 顺序 | Kafka key=主键,同行变更同分区有序;反查源库进一步消解乱序 |
| 失败重试 | 瞬时故障无限退避;数据性错误有限重试 → DLQ(上限归档,带乒乓水位线防护) |
| 崩溃恢复 | 向量 worker:offset 处理完才 commit(pgvector 账本同事务);湖腿:S3 checkpoint + restart |
| Embedding 限流 | 批量切片(MAX_BATCH)+ 队列天然背压:429/5xx 时指数退避重试、阻塞本分区消费(无令牌桶);snapshot 全量阶段尤其注意 |
| WAL 膨胀 | 监控 replication slot lag,Worker 长时间挂掉要告警(否则 PG 磁盘爆);同库非监听表写入导致的 lag 由 connector heartbeat(10s)推进 LSN 自愈 |
| 数值/时间列编码 | `decimal.handling.mode=string`(默认 precise 会把 NUMERIC 编成 base64 二进制垃圾)+ `time.precision.mode=connect`(统一毫秒,避免按列精度输出 µs/ns) |
| 重建索引 | 换 embedding 模型 / 切分策略变更 → 触发全量重放(Debezium re-snapshot) |
| 湖腿并发写 | Hudi 默认无锁,**流是唯一写者**(批量回填须流停时跑);真多 writer 才上 ZK 锁(S3 不支持零依赖文件锁) |
| 可观测 | 同步延迟(CDC→向量)、embedding 调用量/命中跳过率、DLQ 积压、流速率/批延迟 |

> **故障注入实测**(`docs/test-plan-fault-injection.md`):真 `docker kill` 流容器 / 重启 connector,验证 T1 崩溃恢复不丢、T2 崩溃窗口内 update 传播、T3 幂等不重不漏(Hudi 行数 == 源库,精确镜像)、T4 connector 重启 slot 续传——全过。

---

## 6. 技术栈选型

| 环节 | 当前选型 | 生产演进 |
|---|---|---|
| CDC | Debezium standalone(Kafka Connect),单 connector / 单复制槽 | Connect distributed + connector 配置备份 |
| 队列 | 单节点 Kafka(KRaft) | 3+ broker,RF>=3,min ISR,容量/磁盘告警 |
| Sync Worker | Python(Confluent Kafka + pgvector/Qdrant) | 同 group 多实例水平扩展 |
| Embedding | 本地 bge-small 或独立 embed-service / OpenAI compatible | 独立推理服务池,动态批处理,GPU/托管推理 |
| 向量库 | pgvector + RLS,Qdrant 可切 | Qdrant collection 蓝绿 / 托管向量库 |
| RAG 服务 | Python(FastAPI),/search + /ask + rerank | API gateway、限流、审计日志 |
| Lakehouse | Spark local runner 写 Hudi + Iceberg | Spark on Kubernetes/YARN/托管 Spark |
| Embedding / 生成模型 | OpenAI 兼容 API(`EMBED_EGRESS_ALLOWED=true` / `LLM_EGRESS_ALLOWED=true` 后启用;`*_BASE_URL` 可指 agent-ctl 网关 / OpenAI / DeepSeek / 通义 / Ollama / vLLM) | 网关路由、降级、成本治理 |
| 质量评估 | `eval/` retrieval/generation/reconcile | 发布前强制质量门禁 |

---

## 7. 已落地里程碑

- **阶段 0**:Debezium → Kafka → Worker → pgvector → `/search` 最小闭环已完成。
- **阶段 1**:UPDATE / DELETE、确定性向量 ID、hash 去重、DLQ、处理账本已完成。
- **阶段 2**:`/ask`、OpenAI compatible LLM、rerank、多租过滤、RLS、API key 绑定 tenant 已完成。
- **阶段 3**:Qdrant 后端、跨表文档、Prometheus/Grafana、OTel、eval 评估与对账已完成。
- **Lakehouse**:Spark Hudi + Spark Iceberg 批量/连续流、smoke、table maintenance、batch infra 复用已完成。
- **工程化**:Python lint/test CI、compose/shell/lake 脚本结构校验、pre-commit 门禁已完成。

当前成熟度与准入标准见 [`PRODUCTION_READINESS.md`](PRODUCTION_READINESS.md)。

---

## 8. 下一阶段生产决策

这些不是当前本地样板必须内建的能力,而是真实上线前需要按部署环境拍板:

1. **Schema governance**:继续 JSON + 契约测试,还是引入 Avro/Protobuf + Schema Registry。
2. **HA 边界**:Kafka / Connect / Postgres 是否使用托管服务,以及 replication slot failover 策略。
3. **Spark 运行面**:local runner 迁移到 Spark on Kubernetes/YARN/托管 Spark 的提交与权限模型。
4. **安全基线**:Kafka SASL/TLS、PG TLS、secret manager、密钥轮换周期。
5. **SLO 校准**:按真实数据量确定 CDC 延迟、DLQ backlog、slot lag、Spark 微批延迟阈值。
6. **数据治理**:保留周期、合规删除、备份恢复、审计日志和 lakehouse time-travel 窗口。
