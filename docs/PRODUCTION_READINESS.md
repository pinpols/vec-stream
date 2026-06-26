# 生产准入与成熟度评审

> 日期:2026-06-25  
> 结论:当前是 **M2+ / pre-production reference**。功能链路、隔离、安全、观测、评估、lakehouse 旁路已经成体系;但本仓库仍定位为本地可复现的生产级样板,不是直接承诺 HA / 合规 / 托管运维的平台。

## 1. 当前成熟度

| 领域 | 评级 | 结论 |
|---|---:|---|
| 架构边界 | A | 单 CDC 事实流 + 多 sink 扇出清晰;向量、Hudi、Iceberg 独立消费,互不阻塞 |
| CDC 语义 | A- | Debezium JSON 统一复用;insert/update/delete/snapshot 均有落地路径;保留 schema governance 缺口 |
| 向量/RAG | A | 确定性 ID、hash 去重、metadata 刷新、RLS、API key 租户绑定、rerank、/ask 出境门闸、评估模块已形成闭环 |
| Lakehouse | B+ | Spark 统一写 Hudi/Iceberg,批量+连续流+checkpoint+维护脚本具备;仍是 local Spark runner,不是托管计算集群 |
| 可靠性 | B+ | at-least-once + 幂等写、processed_offsets、DLQ 上限归档、slot lag 告警路径具备;生产 HA 依赖外部托管或独立部署 |
| 可观测 | B+ | Prometheus/Grafana/alerts/OTel 可选链路已具备;生产还需 Alertmanager、日志采集和告警值校准 |
| 安全 | A- | 最小权限账号、RLS、API key->tenant、loopback 端口、生产防呆、Qdrant API key 透传具备;生产还需 TLS/SASL、secret manager、密钥轮换 |
| 工程化 | B+ | Python lint/test CI 已有;新增结构校验后 compose/shell/lake 脚本纳入门禁 |
| 运维手册 | A- | M2、lakehouse、Flink/Paimon、topic 边界均有文档;生产部署手册仍需按目标环境落地 |

## 2. 准入门槛

下面这些必须绿,才算一次可发布变更:

```bash
bash scripts/validate.sh

cd worker && uv sync --group dev && uv run --group dev python -m pytest -q
cd ../rag && uv sync --group dev && uv run --group dev python -m pytest -q
cd ../eval && uv sync --group dev && uv run --group dev python -m pytest -q
cd ../embed-service && uv sync --group dev && uv run --group dev python -m pytest -q
```

本地端到端发布前再跑:

```bash
bash scripts/smoke.sh
bash scripts/hudi-smoke.sh
bash scripts/iceberg-smoke.sh
docker compose -f docker-compose.yml -f docker-compose.lake.yml up -d spark-lake-hudi-stream spark-lake-iceberg-stream
bash scripts/stream-smoke.sh
```

CI 门禁:

- `.github/workflows/ci.yml` 的 `structure` job:compose 合并、bash 语法、Spark lake Python 语法、shellcheck。
- `lint-and-test` matrix:worker / rag / eval / embed-service 的 ruff + pytest。
- `.pre-commit-config.yaml`:提交前跑 ruff、基础文件检查、`scripts/validate.sh`。

## 3. 已闭环的生产能力

- 多 sink 架构:单 Debezium connector / 单复制槽 / `cdc.public.*` 统一 topic,向量与 lakehouse 各自独立消费。
- 幂等与一致性:向量侧确定性 `vector_id`,处理账本与 PG 写入同事务;lakehouse 按 PK upsert/delete。
- 安全隔离:源库最小权限账号,RAG key 推导 tenant,PG RLS 做强制隔离;`/ask` 需显式允许 LLM 数据出境。
- 暴露面收敛:本地 compose 端口默认只绑定 `127.0.0.1`;CI 会拒绝新增未绑定 loopback 的端口映射。
- 生产防呆:`APP_ENV=production` 会拒绝 dev key、弱 DSN、Qdrant 无 API key。
- 成本控制:文本 hash 去重,metadata-only 更新不重新 embedding,embedding service 可独立扩展。
- 质量度量:retrieval / generation / reconcile 三类评估,蓝绿索引切换有客观验收。
- 可观测:worker 指标、slot lag、DLQ backlog、Grafana dashboard、Spark stream Prometheus、OTel tracing。
- Lakehouse:Spark Hudi + Spark Iceberg 双腿,批量回填、连续流、Iceberg table service、Hudi cleaner/compaction 配置。
- 环境复用:可自包含运行,也可通过 `docker-compose.reuse-batch.yml` 复用 file-batch-system 的 Kafka/MinIO。

## 4. 生产部署边界

本仓库不内建下面这些重运维能力,生产使用时必须由目标平台提供:

- Kafka HA:3 节点以上、RF>=3、min ISR、容量规划、磁盘告警。
- Connect HA:distributed mode、多 worker、内部 topic RF、connector 配置备份。
- Postgres HA:主备/failover、逻辑复制槽迁移策略、备份恢复演练。
- Secret manager:Vault/KMS/云 Secret Manager,替换 `.env` 明文文件。
- 网络安全:Kafka SASL/TLS、PG TLS、服务间 mTLS 或内网访问控制。
- 管理面:Connect/Qdrant/MinIO/Iceberg REST/Prometheus/Grafana/Jaeger/Spark UI 不允许直接暴露公网。
- 日志与告警:集中日志、Alertmanager/通知路由、按真实负载校准 SLO 阈值。
- Spark 运行环境:生产不要依赖单容器 local mode;迁移到 Spark on Kubernetes/YARN/托管 Spark 后复用 `spark-lake` 作业逻辑。
- 数据治理:Schema Registry 或 DDL 兼容流程、数据血缘、权限审计、合规删除。

## 5. 剩余高价值改进

这些不是“功能没做”,而是让项目更像可交付平台:

1. **Schema contract test**:从 `db/init/01-init.sql` 或 live `information_schema` 生成表字段契约,校验 worker `TABLES_JSON` 和 `spark-lake` schema 不漂移。
2. **Runbook drill**:增加 WAL lag、DLQ 归档、Kafka retention 丢 offset、Iceberg 维护失败的演练脚本或检查清单。
3. **Release checklist**:把本文件 §2 的准入命令固化成版本发布清单,记录 smoke 输出和镜像 tag。
4. **Dependency audit**:给 Python、Docker base image、Spark jar 依赖加定期安全扫描。
5. **Production overlays**:为真实部署新增单独 overlay,明确 TLS/SASL/secret manager/外部 Kafka/外部 MinIO/S3 的变量契约。

## 6. 最终判断

这个项目现在不该再改名为单一 `cdc-stream` 或 `vector-rag`:它已经是 **CDC 多 sink 分发底座**。`vec-stream` 这个名字可以保留,但对外描述应固定为:

> CDC-driven multi-sink streaming platform for vector search/RAG and lakehouse materialization.

生产级完善的下一步不是继续堆 sink,而是把 schema 契约、故障演练、发布准入和生产 overlay 做硬。否则功能越多,回归面越大。
