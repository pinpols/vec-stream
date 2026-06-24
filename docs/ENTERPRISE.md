# 企业级演进规划 · cdc-vector-rag

> 版本:v0.2 · 日期:2026-06-24 · 状态:规划 + **M1 上线阻断项已落地**
> 前置:[`DESIGN.md`](DESIGN.md)(MVP 设计)· 当前已完成阶段 0–3 + 健壮性修复 + M1 安全/一致性加固

---

## M1 已落地(2026-06-24)

5 项「上线阻断项」全部完成,落地一句话指引(详见各 §1 对应行):

| M1 项 | 落地 | 关键文件 |
|---|---|---|
| 处理账本(offset 写入原子性) | `processed_offsets` 表与向量 upsert/delete **同事务**提交;Qdrant 后端记 PG(best-effort) | `db/init/02-security.sh`、`worker/.../sink.py`、`main.py` |
| 租户隔离强制化(RLS) | `doc_vectors` 开行级安全,策略按 `current_setting('app.tenant')`;`vs_rag` 无 BYPASSRLS,查询前 `set_config('app.tenant')`——**漏写 WHERE 也越权不了**(实测:设 A 只见 A、不设见 0 行、rag 越权写被拒) | `db/init/02-security.sh`、`rag/.../app.py` |
| PG 最小权限账号 | 三专用角色:`vs_debezium`(LOGIN+REPLICATION+源表 SELECT)/`vs_worker`(向量+账本 DML+BYPASSRLS)/`vs_rag`(只读·受 RLS),CDC 不再用超级账号 | `db/init/02-security.sh` |
| RAG API 认证 + token 即租户 | `X-API-Key`→租户(`RAG_API_KEYS` JSON),**租户从 key 推导不信任请求体**,并与 RLS 闭环 | `rag/.../app.py` |
| Secrets 管理 | 凭据移出代码进 `.env`(gitignore);compose env 注入;Debezium 密码注册时注入不落盘 | `.env.example`、`docker-compose.yml`、`debezium/register.sh` |

> 复刻验证:`docker compose up -d`(自动跑 01-init + 02-security)→ `./debezium/register.sh`;单测 `worker/.venv/bin/python -m pytest worker/tests`、`rag/.venv/bin/python -m pytest rag/tests`。

## M2 已落地(2026-06-24)

「生产必需」6 项 ✅该做 全部完成(M3 的 🔴越界项按本文档判定**显式不做**,仅留 §3 知识卡片)。操作手册见 [`runbook-m2.md`](runbook-m2.md)。

| M2 项 | 落地 | 关键文件 |
|---|---|---|
| Embedding 拆独立服务 | FastAPI + 动态批处理 + 背压;worker/rag 设 `EMBED_SERVICE_URL` 即切 HTTP,空则进程内(向量可互换无需重建索引) | `embed-service/`、`worker/.../embedder.py`、`rag/.../app.py` |
| 质量评估 + 一致性对账 | recall@k/MRR(检索)+ 引用忠实度/RAGAS(生成)+ 向量vs源表漂移(对账);golden set | `eval/`(34 单测) |
| 告警规则 + SLO | Prometheus 规则(同步延迟 p99 / DLQ / slot / worker 掉线,用真实指标名)+ Grafana 大盘 | `monitoring/`、`docker-compose.monitoring.yml` |
| DLQ 工具链增强 | 重投次数上限 `DLQ_MAX_REPLAYS`,超限落档 `dead_letter_archive`(不无限重投);replay_count 往返累加 | `worker/.../dlq_replay.py`、`main.py`、`02-security.sh` |
| 蓝绿索引切换 | 新 collection+新 group+新模型双写 → eval 评估达标 → 切流量(runbook §5);Qdrant collection 为切换单元 | `runbook-m2.md` |
| 水平扩展 + schema 校验 | 同 group 多实例按分区并行(代码已支持,runbook §6 验证流程);启动校验配置字段存在,改列/删列快速失败 | `worker/.../schema_check.py`、`runbook-m2.md` |

> M3(Kafka 3 节点 / Connect distributed / PG failover / exactly-once / Schema Registry / 传输加密 / OTel / ELK / 删除合规)按 §2/§3 判定**默认不做**——纯运维/合规工程,本机跑不动且偏离学习目标,生产应如何做见 §3 知识卡片。

---

## 0. 怎么读这份文档

这份文档把"从能跑的学习项目 → 企业级生产系统"的差距拆成 **7 个领域、3 个梯队**。

**先说一个诚实的前提**:`DESIGN.md` §1.2 明确划了范围边界——本项目的定位是**学 CDC+Embedding+向量检索+RAG 这套技术栈**,不是再造一个重运维平台。下面很多企业级项(3 节点 Kafka、Flink 集群、GPU 池)一旦做下去,项目重心就从"学新技术"滑向"搞基础设施运维",与初衷背离。

所以每一项都标了 **判定**:

- ✅ **该做(学习+生产双赢)**:既补生产短板,又能学到新东西,改动可控 → 建议纳入
- 🟡 **按需(看是否真上生产)**:纯生产工程,学习边际收益低,只在"真要上线给真实用户"时做
- 🔴 **越界(违背项目定位)**:做了就变成运维平台项目,建议**显式不做**,只在文档里写清"生产应如何做"作为知识留存

梯队:**M1 上线阻断项**(不修不能给真实用户)→ **M2 生产必需**(上线后稳定运行)→ **M3 规模化/合规**(用户量/合规要求上来才需要)。

---

## 1. 七大领域差距与判定

### 领域一:投递语义与一致性

| 项 | 现状 | 目标 | 判定 | 梯队 |
|---|---|---|---|---|
| 消费侧 offset/写入原子性 | commit offset 与写向量两步,崩在中间靠幂等兜底 | 幂等消费 + 可审计的"处理一次"账本 | ✅ 该做 | M1 |
| Debezium→Kafka exactly-once | at-least-once | Kafka 事务(Debezium 2.x `exactly.once.support`) | 🟡 按需 | M2 |
| Schema 治理 / DDL | 字段写死,DDL 裸奔 | Schema Registry + 兼容性策略 + 消费端版本协商 | 🟡 按需 | M2 |

**关键判断**:我们的链路**末端是幂等 upsert(确定性 ID)**,这从根本上降低了对 exactly-once 的需求——重复投递不会产生重复数据。所以:

- offset 原子性(M1):不必上 Kafka 事务,做轻量**处理账本**即可——一张 `processed_offsets(topic, partition, offset, processed_at)` 表,与向量写入同事务提交(pgvector 后端天然可做;Qdrant 后端则记在 PG 里)。这把"说不清处理过几次"变成"可查",审计闭环,改动小、学到 outbox/inbox 模式。
- Debezium exactly-once(M2):末端已幂等,收益主要在"避免重复 embedding 成本"——但 hash 去重已经挡掉了绝大部分。**性价比低,按需**。
- Schema Registry(M2):真正痛点是 DDL 演进。轻量解法见领域七;全套 Avro+Registry 是 🟡。

---

### 领域二:高可用与容量

| 项 | 现状 | 目标 | 判定 | 梯队 |
|---|---|---|---|---|
| Embedding 推理拆服务 | 跑在 worker 进程内 | 独立推理服务(HTTP),worker 调用 | ✅ 该做 | M2 |
| worker 水平扩展 | 单实例 | 同 group 多实例,按分区并行 | ✅ 该做 | M2 |
| Kafka 3 节点 | 单节点 KRaft | 3 节点,RF=3 | 🔴 越界 | M3 |
| Connect distributed | 单 worker standalone | distributed 模式多 worker | 🔴 越界 | M3 |
| PG 流复制 + failover | 单实例 | primary + standby + 自动切换 | 🔴 越界 | M3 |

**关键判断**:

- Embedding 拆服务(✅ M2)是**最有学习价值的一项**:换成 HuggingFace TEI 或 Infinity,worker 改为 HTTP 调用。学到推理服务的动态批处理、限流、模型与消费解耦;还顺带解决"worker 扩容=模型副本翻倍"。**强烈建议做**,即使不上生产。
- worker 水平扩展(✅ M2):代码已经支持(同 group、key 顺序性自动保持),只需 topic 加分区 + 起多实例验证。**几乎零成本,该验证一次**留作能力证明。
- HA 三件套(🔴 M3):3 节点 Kafka / Connect distributed / PG failover——这些是**纯运维工程**,本机 8GB Docker 也跑不动,做了就偏离学习目标。**显式不做**,在 §3 留"生产应如何做"的知识卡片即可。

---

### 领域三:安全

| 项 | 现状 | 目标 | 判定 | 梯队 |
|---|---|---|---|---|
| 租户隔离强制化 | tenant_id 查询条件(纪律) | 强制机制(RLS / 独立 collection) | ✅ 该做 | M1 |
| Secrets 管理 | 明文写配置 | Vault/KMS / 至少 .env + 注入 | ✅ 该做 | M1 |
| PG 最小权限账号 | Debezium 用超级账号 | 专用账号,只给 REPLICATION + SELECT | ✅ 该做 | M1 |
| rag API 认证授权 | 无认证 | JWT / API key,租户绑定 token | ✅ 该做 | M1 |
| Kafka SASL/TLS、PG TLS | PLAINTEXT | 传输加密 | 🟡 按需 | M2 |

**关键判断**:这是**唯一一个 M1 项最密集的领域**——因为安全短板是真正的上线阻断项,且和你那条"审计 vs 架构评审"备忘直接呼应:**tenant_id 过滤是纪律,不是机制**。

- 租户隔离(✅ M1)是重中之重。两条路线,二选一:
  - **pgvector**:PG 行级安全(RLS)——`CREATE POLICY` 按 `current_setting('app.tenant')` 过滤,rag 连接设置租户后查询**无法**越权,即使 SQL 漏写 WHERE。这是"机制"而非"纪律"。
  - **Qdrant**:每租户独立 collection,或用 partition key(Qdrant 1.x 多租户特性)。物理隔离更强。
  - 学习价值高(RLS / 多租架构),改动中等,**建议做 pgvector RLS 这条**(与现有栈贴合)。
- Secrets / 最小权限账号 / API 认证(✅ M1):都是改动小、收益直接的硬项。API 认证顺便把 token 里的 tenant 与查询绑定,和租户隔离形成闭环。
- 传输加密(🟡 M2):内网部署可延后,公网必做。纯配置工程,学习收益低。

---

### 领域四:运维成熟度

| 项 | 现状 | 目标 | 判定 | 梯队 |
|---|---|---|---|---|
| 选择性重放工具 | 全量重放 / DLQ 水位线重投 | 按表/时间窗/租户重放 | ✅ 该做 | M2 |
| DLQ 自动重投 + 次数上限 + 归档 | 手动重投,无上限 | 定时重投 + max-retry + 死信归档表 | ✅ 该做 | M2 |
| 告警规则 + SLO | 指标已暴露,无告警 | Prometheus 告警规则(延迟 p99 / DLQ 积压) | ✅ 该做 | M2 |
| Grafana 大盘 | 无 | 同步延迟/跳过率/DLQ/slot 看板 | 🟡 按需 | M2 |
| 分布式追踪 OTel | 无 | trace 贯穿 CDC→向量→RAG | 🟡 按需 | M3 |
| 结构化日志 → ELK | 文本日志 | JSON 日志 + 采集 | 🟡 按需 | M3 |

**关键判断**:指标的"采集端"已经做完(Prometheus 格式),缺的是"消费端"。

- 告警规则(✅ M2):写一组 Prometheus alert rule(`vec_stream_sync_delay_seconds` p99、`vec_stream_dlq_backlog`、`vec_stream_slot_active==0`)。**几乎零代码**,定义 SLO 是好习惯,该做。
- 选择性重放(✅ M2):基于 Debezium incremental snapshot(信号表触发按表/按条件重放)——这是**比自己写重放更该学的东西**,Debezium 原生能力,正好补"按表重建"短板。
- DLQ 工具链增强(✅ M2):重投次数上限(消息头记 replay_count,超限进归档表)、死信归档——是已有 `dlq_replay.py` 的自然延伸,改动小。
- Grafana / OTel / ELK(🟡):接现成系统的工程活,你主项目已有 OTel 经验,**学习边际收益低**,真上生产再接。

---

### 领域五:数据质量与评估(RAG 特有)

| 项 | 现状 | 目标 | 判定 | 梯队 |
|---|---|---|---|---|
| 索引一致性对账 | 无 | 向量数 vs 源表行数漂移检测任务 | ✅ 该做 | M2 |
| 检索质量回归 | 无 | golden set + recall@k / MRR 评估流水线 | ✅ 该做 | M2 |
| 生成质量评估 | 无 | faithfulness / 幻觉率(RAGAS) | ✅ 该做 | M2 |

**关键判断**:这是**最容易被忽视、但学习价值最高的领域**——也是"会用 RAG"和"会做 RAG"的分水岭。整块判定 ✅,因为它几乎不碰运维,纯粹是 RAG 工程的核心能力。

- 一致性对账(✅ M2):一个定时任务,`SELECT count by source_pk` 对比源表与 doc_vectors,差异即漂移(对应"反查竞态""漏处理"等隐患的兜底监控)。改动小,先做。
- 检索质量回归(✅ M2):建 golden set(query→期望命中的 doc),改 chunk 策略/换 embedding 模型前后跑 recall@k、MRR。**这是换模型决策的客观依据**,否则"换了感觉更好"全凭主观。
- 生成质量(✅ M2):接 RAGAS 评估引用忠实度、幻觉率。依赖 `/ask` 真 key 先跑通。

> 这三项建议合并成一个 `eval/` 模块,作为项目"学习目标"的**收尾高地**——前面建的整条管道,最终要靠它证明"质量可量化、可回归"。

---

### 领域六:生命周期管理

| 项 | 现状 | 目标 | 判定 | 梯队 |
|---|---|---|---|---|
| 模型/索引版本化 + 蓝绿切换 | 换模型=全量重建,无版本 | 双写新旧 collection → 验证 → 切流量 | ✅ 该做 | M2 |
| 数据保留与删除合规 | DELETE 删向量,Kafka 留存内仍有事件 | 彻底删除(含 Kafka)、保留策略 | 🟡 按需 | M3 |

**关键判断**:

- 蓝绿索引切换(✅ M2):换 embedding 模型时,新模型写 `doc_vectors_v2` collection,跑评估(领域五)对比,达标后 rag 切 `VECTOR_BACKEND`/collection 指向。**和领域五评估天然配套**,学到生产级模型升级范式,建议做。
- 删除合规(🟡 M3):GDPR 式彻底删除涉及 Kafka compaction/tombstone、备份清理,合规驱动,无明确需求前不做。

---

### 领域七:DDL 演进(从 §1 领域一拆出细化)

| 项 | 现状 | 目标 | 判定 | 梯队 |
|---|---|---|---|---|
| 字段映射热加载 | 硬编码 + TABLES_JSON 重启生效 | 配置变更不重启 | 🟡 按需 | M3 |
| 加列容忍 | after 多字段自动忽略(已容忍) | 无需动作 | ✅ 已具备 | — |
| 改列/删列检测 | 静默用错字段 | 启动时校验 table schema vs 配置 | ✅ 该做 | M2 |

**关键判断**:全套 Schema Registry 是 🟡(领域一),但**轻量防御**值得做——worker 启动时查 `information_schema.columns` 校验配置里的字段确实存在,改列/删列时**快速失败**而非静默用错数据。改动小,挡掉一类隐蔽数据错误。

---

## 2. 落地路线(按梯队 + 判定筛选后)

只列 ✅ 和值得做的 🟡。🔴 项不进路线,见 §3 知识卡片。

### M1 · 上线阻断项(给真实用户前必须)

> 主题:**安全 + 可审计**。这批不做,系统不能对外。

1. **租户隔离强制化**(领域三)— pgvector RLS:`doc_vectors` 建 policy,rag 连接按 token 里的 tenant 设 `app.tenant`,越权查询物理不可能。
2. **rag API 认证**(领域三)— API key / JWT,token 绑定 tenant_id,与 1 形成闭环。
3. **Secrets 外置 + PG 最小权限账号**(领域三)— Debezium 专用账号(REPLICATION+SELECT),密码走 .env/Secret 注入,移出 compose 明文。
4. **处理账本**(领域一)— `processed_offsets` 表与向量写入同事务,审计"处理一次"可查。

**M1 验收**:租户 A 的 token 查不到租户 B 数据(SQL 强制);无 token 拒绝;DB 账号最小权限;任一条 offset 可追溯处理记录。

### M2 · 生产必需(上线后稳定运行)

> 主题:**质量 + 可运维 + 弹性**。这批是把"能跑"变成"能长期跑"。

5. **Embedding 拆独立服务**(领域二)— 换 TEI/Infinity,worker 改 HTTP 调用 + 限流重试。【最高学习价值】
6. **检索/生成质量评估 + 一致性对账**(领域五)— `eval/` 模块:golden set + recall@k/MRR + RAGAS + 漂移检测任务。【项目收尾高地】
7. **告警规则 + SLO**(领域四)— Prometheus alert rules,定义同步延迟/DLQ/slot 的 SLO。
8. **DLQ 工具链增强**(领域四)— 重投次数上限 + 死信归档;选择性重放接 Debezium incremental snapshot。
9. **蓝绿索引切换**(领域六)— 换模型双写新 collection → 评估达标 → 切流量。
10. **worker 水平扩展验证 + schema 校验**(领域二/七)— topic 加分区起多实例跑一次;启动校验字段存在。

**M2 验收**:换 embedding 模型有客观跑分依据并能蓝绿切换;延迟超标自动告警;DLQ 不会无限重投;多实例并行消费正确。

### M3 · 规模化/合规(用户量或合规要求驱动)

> 主题:**HA + 合规**。大多是 🔴/🟡,本项目**默认不做**,仅文档留存(§3)。

- Kafka 3 节点 / Connect distributed / PG failover(🔴)
- Debezium exactly-once、全套 Schema Registry(🟡)
- 传输加密、OTel、ELK、删除合规(🟡)

---

## 3. 知识卡片:🔴 越界项"生产应如何做"

> 这些**本项目不实现**(做了就变运维平台,违背 DESIGN.md §1.2),但写清生产做法,作为知识留存——面试/真实项目时能讲清楚。

**Kafka HA**:3 节点 KRaft,`replication.factor=3`、`min.insync.replicas=2`,生产端 `acks=all`。容忍单节点故障不丢数据。

**Connect distributed**:多 Connect worker 共享 config/offset/status topic,connector task 自动在 worker 间 rebalance;单 worker 挂了 task 迁移到存活节点。

**PG HA**:primary + 同步/异步 standby,Patroni + etcd 做自动 failover;Debezium 的 slot 需在 failover 后重建或用 `failover slots`(PG 17+ 逻辑复制 failover slot)避免丢位点——**这是 CDC+HA 的真正难点**。

**Embedding GPU 池**:推理服务多副本挂 GPU,前置负载均衡 + 动态批处理(TEI 自带);worker 只发 HTTP,推理层独立伸缩。

---

## 4. 一句话总结

- **M1(4 项)**:安全闭环,是上线的最低门槛,优先级最高。
- **M2(6 项)**:质量评估 + Embedding 拆服务是**学习价值最高**的两块,建议即使不上生产也做,作为项目高地。
- **M3**:HA/合规大多**显式不做**,保持项目"学技术栈、不造运维平台"的定位,知识以文档形式留存。

> 取舍原则(贯穿全文):**业务逻辑与质量评估永远自己做,基础设施 HA 尽量用现成或不做。** 凡是会把项目重心拖向"运维平台"的,一律推迟或砍掉——这与 DESIGN.md §1.2 的范围边界一脉相承。
