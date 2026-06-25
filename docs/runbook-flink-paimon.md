# Flink → Paimon · 参考样板(Paimon 原生引擎)

> 定位:**参考样板**,不是常驻一等组件。湖腿日常用 Spark(`spark-lake`,见 `docs/runbook-lakehouse.md`);
> 本样板演示 **Paimon**——Flink 原生、为流式 CDC 而生的湖表(国内实时数仓首选)。

## 为什么这条用 Flink

- **Paimon 是 Flink 原生**(原 Flink Table Store),Flink 是它的主场引擎——所以"要 Paimon = 该用 Flink",不像 Flink→Iceberg 那样别扭。
- **CDC 链路不变**:Flink 读现有 `cdc.public.*`(`format='debezium-json'`,方式 A),只是又一个消费者;不是 Flink CDC 直连库(方式 B,会再开复制槽破坏统一)。
- Paimon **主键表原生吃 changelog**(+I/-U/+U/-D),upsert/delete 自动传播,无需自写合并。
- **依赖最干净**:`paimon-s3` 自包含 S3 支持,不用 Flink→Hudi 那套 hadoop-aws/commons-logging/lock-provider 折腾(只需补 `flink-shaded-hadoop`)。

## 跑起来

```bash
COMPOSE="docker compose -f docker-compose.yml -f docker-compose.lake.yml -f docker-compose.paimon.yml"
# 1) 起集群 + 依赖(复用 cdc connector / minio;warehouse 桶)
$COMPOSE up -d postgres kafka connect minio minio-init \
  flink-paimon-jobmanager flink-paimon-taskmanager
#    Flink Web UI: http://localhost:8086
./debezium/register.sh                         # cdc connector(已存在则跳过)

# 2) 提交流式作业(debezium-json → Paimon upsert,常驻在集群)
$COMPOSE run --rm flink-paimon-sql

# 3) 批量读回验证(合并当前态 == 源表)
$COMPOSE run --rm -e PAIMON_SQL=verify_paimon.sql flink-paimon-sql
```

## 已验证(本地)

- Flink 1.19.3 作业 RUNNING,消费 `cdc.public.article`(debezium-json)→ Paimon 主键表 `paimon.lake.article`(warehouse `s3://warehouse/paimon`);
- changelog 原生 upsert/delete,checkpoint 提交;**Flink 批量读回合并当前态行数 == 源表(11==11)**。

## 踩的坑

1. `ClassNotFoundException: org.apache.hadoop.conf.Configuration` → Paimon catalog 框架要 un-shaded hadoop;补 `flink-shaded-hadoop-2-uber`(同 Flink→Iceberg)。
2. sql-client `Connection refused` → entrypoint 覆写绕过 FLINK_PROPERTIES,默认 `rest.address=0.0.0.0`;sed `config.yaml` 指 `flink-paimon-jobmanager`(同其他 Flink 样板)。

## 收尾(参考样板不常驻)

```bash
curl -s -X PATCH "http://localhost:8086/jobs/<job-id>?mode=cancel"
$COMPOSE stop flink-paimon-jobmanager flink-paimon-taskmanager
```

要把 Paimon 提为常驻一等(streaming-native 实时数仓),或 Flink CDC→Paimon 整库同步,照此扩;**按需做**。
