# Lakehouse 腿 · 运行手册(统一 Spark 引擎)

两条湖腿统一到 **Spark**(各自主流引擎):Spark→Hudi(Hudi 主场)、Spark→Iceberg(一等集成)。
与向量腿共用同一份 `cdc.public.*` JSON 流(单 connector / 单复制槽,见 `docs/TOPICS.md`)。

> **两种运行模式**:① **连续流**(Spark Structured Streaming,常驻,~10-20s 延迟)——默认日常用;
> ② **批量**(run 一次读全 topic,幂等)——回填 / 一次性。两模式共用同一段解析+写入逻辑。
> 延迟:trigger 默认 10s(`TRIGGER_SECONDS` 可调),端到端 ~10-20s;向量腿是连续消费,亚秒~秒级(更快,by-design)。

## 组成

| 组件 | 作用 |
|---|---|
| cdc connector(`vec-stream-pg`,`debezium/register.sh`)| 产 `cdc.public.*` JSON(三 sink 共用)|
| `iceberg-rest`(apache/iceberg-rest-fixture)| Iceberg REST catalog |
| MinIO `warehouse` 桶 | Hudi `hudi/<t>` + Iceberg 数据/元数据 |
| `spark-lake`(`spark-lake/`)| 一个 Spark 镜像写两种格式:`cdc_to_hudi.py` / `cdc_to_iceberg.py`(MERGE INTO)|

脚本走挂载(compose volume),改脚本免重建镜像。

## 一键验证

```bash
bash scripts/hudi-smoke.sh       # 批量:insert/update/delete → Hudi
bash scripts/iceberg-smoke.sh    # 批量:insert/update/delete → Iceberg

# 连续流:先起常驻流,再验"自动捡变更"(不手动跑写作业)
docker compose -f docker-compose.yml -f docker-compose.lake.yml up -d \
  spark-lake-hudi-stream spark-lake-iceberg-stream
bash scripts/stream-smoke.sh
```

## 手动跑

```bash
COMPOSE="docker compose -f docker-compose.yml -f docker-compose.lake.yml"
$COMPOSE up -d postgres kafka connect minio minio-init iceberg-rest
./debezium/register.sh                          # cdc connector(已存在则跳过)
$COMPOSE build spark-lake

# 连续流(常驻,默认日常;多表 all = article/product/comment 一作业全覆盖)
$COMPOSE up -d spark-lake-hudi-stream spark-lake-iceberg-stream
# 批量(回填 / 一次性,幂等);<table> = all | article | product | comment
$COMPOSE run --rm spark-lake hudi all
$COMPOSE run --rm spark-lake iceberg all
# 查询(单表 + 可选 id)
$COMPOSE run --rm spark-lake query-hudi product          # COUNT=
$COMPOSE run --rm spark-lake query-iceberg article default 1  # RESULT=<status|ABSENT>
```

## 关键设计

- **Hudi**(`cdc_to_hudi.py`):扁平→`df.write.format("hudi")` upsert;删除用 `_hoodie_is_deleted`;MOR + GLOBAL_SIMPLE 索引;recordkey=`tenant_id,id`、precombine 用零填充 `(source.lsn 或 ts_ms, offset)` 字符串、partition=`tenant_id`。**删除依赖 `REPLICA IDENTITY FULL`**(before 完整;smoke 自动设)。
- **Iceberg**(`cdc_to_iceberg.py`):每 `tenant_id,id` 取最新变更→`MERGE INTO`(`op=d` DELETE / 其余 UPSERT);Spark Iceberg v2,REST catalog + S3FileIO。
- **CDC 顺序边界**:优先使用 Debezium Postgres `source.lsn` 排序,offset 只作同位点 tie-breaker;微批内校验同一 `tenant_id,id` 不能跨 Kafka partition,否则作业响亮失败。生产仍必须保持 Debezium Kafka key=PK 且 topic 扩分区历史稳定。
- **批量 / 流共用逻辑**:`STREAM_MODE=true` 走 `readStream`+`foreachBatch`(checkpoint 在 `s3a://warehouse/_chk/<engine>-<table>`),否则批量读全 topic;每微批走同一段解析+写入。
- **幂等**:都按主键合并、重放结果一致。

### Hudi record key 升级边界

旧版本如果已经写出 `recordkey=id` 的 Hudi 表,不能直接用当前 `recordkey=tenant_id,id` 配置续写同一路径。作业会读取 `.hoodie/hoodie.properties` 并拒绝旧 key 表。迁移方式二选一:

- 本地/可重放环境:停止 Hudi 流,删除 `s3a://warehouse/hudi/<table>` 和对应 `s3a://warehouse/_chk/hudi-*` checkpoint,从 Kafka earliest 重建。
- 生产环境:写入新 base path / 新表名,全量重放校验后切换读侧,再下线旧表。

### tenant_id 变更清理(已知边界)

tenant_id 业务上视为不可变(DESIGN.md 边界段);若真发生 t1→t2,Hudi
(recordkey 含 tenant)与 Iceberg(MERGE ON tenant,id)都会把新行当**新 key**
写入,t1 旧行残留(Paimon 的 retraction 正确撤回,无此问题)。参考级不改写入
逻辑,发现残留用 SQL 手工清理(以 id=42 从 t1 迁走为例):

```sql
-- Iceberg(spark-sql,catalog 名按环境)
DELETE FROM lake.db.article_iceberg WHERE tenant_id = 't1' AND id = 42;

-- Hudi(spark-sql;删除经 Hudi 写路径,产生 delete 记录)
DELETE FROM article_hudi WHERE tenant_id = 't1' AND id = 42;
```

批量核对可与向量侧 `eval reconcile` 同思路:按 (tenant_id, id) 对比源表与湖表,
湖表多出的 (旧租户, id) 即残留。

## 表维护(后台 table service · 生产必备)

各引擎机制不同:

- **Iceberg**(无 inline,必须定期显式跑):`run --rm spark-lake maintain-iceberg <all|table>` —— `rewrite_data_files`(小文件合并)+ `rewrite_manifests` + `expire_snapshots`(保留最近 5,生产按 time-travel 窗口设 older_than)。生产用 cron 定时跑。实测:article 76 数据文件→1、76 快照→5、数据不丢。
- **Hudi**(inline 自维护):MOR 每 5 delta commit 合并 log→base;cleaner `KEEP_LATEST_COMMITS` 留最近 10(`cdc_to_hudi.py` 已配),写时自动跑。
- **Paimon**(流内自维护):snapshot 保留 `num-retained.max=20`/`time-retained=1h` + `full-compaction.delta-commits=5`(`cdc_to_paimon.sql` 表 DDL 已配)。

## 实时性 / 分层

- **连续流已实现**(默认);延迟 ~10-20s(trigger 10s 可调 `TRIGGER_SECONDS`)。
- 向量腿连续逐条消费、亚秒~秒级(更快,by-design 分层:核心实时、湖准实时)。真·亚秒需 Flink(已退役)。
- 选型对比见 `docs/DESIGN.md`。

## 可观测(批3 · Prometheus)

连续流用 Spark 原生 **PrometheusServlet**(无额外 jar),driver UI 暴露指标;Prometheus 直接抓:

- Hudi 流:`http://localhost:4040/metrics/prometheus/`;Iceberg 流:`http://localhost:4041/metrics/prometheus/`
- 关键流指标(`spark_lake` 命名空间):`*_inputRate_total`(输入速率)、`*_processingRate_total`(处理速率)、`*_latency`(批延迟)、`*_eventTime_watermark`、`*_states_rowsTotal`;另有 JVM/BlockManager/executor 指标。
- 接入:`docker compose -f docker-compose.monitoring.yml up -d`(已含 `spark-lake-*-stream` 与 `flink-paimon-*` 抓取 job);Prometheus `/targets` 可见。
- Paimon(Flink)用 Flink 自带 **PrometheusReporter**(镜像已从 `opt/` 挪到 `lib/`),jobmanager `:9249` / taskmanager `:9250`。

## 可靠性 + 配置/安全(批4)

- **崩溃自愈**:checkpoint 在 `s3a://warehouse/_chk/<engine>-<table>`;容器 `restart: unless-stopped`;`spark.streaming.stopGracefullyOnShutdown=true` 让 SIGTERM 时当前微批落完再退。配合 Hudi/Iceberg 主键合并 = 重放幂等。
- **背压 / 有界恢复**:`MAX_OFFSETS_PER_TRIGGER`(留空=无界)限制单微批拉取量;`FAIL_ON_DATA_LOSS`(默认 `true`,丢 offset 即响亮失败)。两者经 `.env` 透传(见 `.env.example`)。
- **单 writer 约束(Hudi)**:Hudi 默认无锁,多 writer 写同一表会损坏。本架构**流是唯一写者**,
  **批量回填 `run.sh hudi <table>` 务必在流停止时跑**。真要多 writer 才设 `HUDI_LOCK_ZK_URL=<host:port>`
  起 ZooKeeper 锁(S3/MinIO 不支持零依赖文件锁)。故障注入实测见 `docs/test-plan-fault-injection.md`。
- **凭据安全**:S3/MinIO secret **不进** spark-submit 命令行 / Spark UI Environment 页 —— Hadoop 走 `EnvironmentVariableCredentialsProvider`、Iceberg S3FileIO 走默认凭据链,均从 `AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY` 环境变量读。生产务必改掉 `.env` 里的 MinIO 默认弱口令。

## 复用 file-batch-system 的 Kafka / MinIO(本机资源紧张时)

默认 vec-stream 自包含(一条命令跑通)。本机紧张时用 `docker-compose.reuse-batch.yml` overlay
**只复用 batch 的 Kafka + MinIO**,vec 仍保留自己的 Postgres(`wal_level=logical`)+ Debezium Connect
(batch PG 是 `wal_level=replica`、schema/角色/RLS 边界不同,不适合作 CDC 源库)。

```bash
# 1) 起 batch 基础设施(在 file-batch-system 目录)
docker compose up -d kafka minio
# 2) 查 batch 网络真实名(项目名前缀,因人而异)
docker network ls | grep batch          # 形如 <project>_batch-network
export BATCH_NETWORK=batch-local_batch-network    # 按实际改
# 3) 起 vec 复用栈(显式不列 vec 自己的 kafka/minio)
docker compose -f docker-compose.yml -f docker-compose.lake.yml -f docker-compose.reuse-batch.yml \
  up -d postgres connect iceberg-rest minio-init-batch spark-lake-hudi-stream spark-lake-iceberg-stream
bash debezium/register.sh
```

- **边界干净**:topic 不撞(batch `batch.*` / vec `cdc.public.*`,auto-create=on 自动建);桶不撞
  (batch `batch-dev` / vec 独立 `warehouse`,`minio-init-batch` 自动建);凭据一致(均 minioadmin)。
- **已实测**:vec Postgres+Debezium → batch-kafka → spark-lake-hudi-stream → Hudi(batch-minio)
  端到端 `RESULT=published`,且 vec 自己的 kafka/minio 全程不启动。
- 宿主机 worker 复用:`KAFKA_BOOTSTRAP=localhost:<batch KAFKA_HOST_PORT>`(走 batch-kafka 的
  PLAINTEXT_HOST 监听器)。overlay 头部注释有完整说明。
