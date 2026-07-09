"""CDC(cdc.public.* JSON envelope)→ Hudi(Spark,批量 / 连续流,多表)。

table=all(默认):一个作业 subscribePattern 订阅三表 topic,foreachBatch 按 topic 路由到各表。
table=<name>:单表。STREAM_MODE=true 走 Structured Streaming,否则批量。
"""

import os
import sys

from lakehouse_logic import EVENT_POS_FORMAT, UNAVAILABLE_VALUES, normalize_hudi_record_key
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, countDistinct, format_string, from_json, lit, when
from pyspark.sql.types import LongType, StringType, StructField, StructType

TABLES = {
    "article": [
        ("id", LongType()),
        ("tenant_id", StringType()),
        ("title", StringType()),
        ("body", StringType()),
        ("status", StringType()),
        ("updated_at", StringType()),
    ],
    "product": [
        ("id", LongType()),
        ("tenant_id", StringType()),
        ("name", StringType()),
        ("description", StringType()),
        ("status", StringType()),
        ("updated_at", StringType()),
    ],
    "comment": [
        ("id", LongType()),
        ("tenant_id", StringType()),
        ("article_id", LongType()),
        ("body", StringType()),
        ("updated_at", StringType()),
    ],
}


def _envelope(cols):
    row = StructType([StructField(n, t, True) for n, t in cols])
    source = StructType([StructField("lsn", LongType(), True)])
    return StructType(
        [
            StructField("before", row, True),
            StructField("after", row, True),
            StructField("op", StringType(), True),
            StructField("ts_ms", LongType(), True),
            StructField("source", source, True),
        ]
    )


def _value(is_del, name, dtype):
    before_v = col("e.before").getField(name)
    after_v = col("e.after").getField(name)
    # TOAST 边界:pgoutput 对 UPDATE 中未变更的 TOAST 大列在 after 里填占位符
    # __debezium_unavailable_value(bytea 列经 JSON base64 后是其 base64 形态);
    # REPLICA IDENTITY FULL 保证 before 镜像有真值 → 占位符回退 before,
    # 否则占位符会当成"新值"写进湖表,静默损坏大文本列。至少覆盖全部字符串列。
    if isinstance(dtype, StringType):
        after_v = when(after_v.isin(*UNAVAILABLE_VALUES), before_v).otherwise(after_v)
    v = when(is_del, before_v).otherwise(after_v)
    # tenant_id 是分区路径,null 会落 __HIVE_DEFAULT_PARTITION__ 且与 GLOBAL_SIMPLE 跨分区
    # 移动打架,统一兜底到固定占位符,保证分区路径稳定。
    if name == "tenant_id":
        v = coalesce(v, lit("__unknown__"))
    return v.alias(name)


def _flatten(df_kafka, cols, envelope):
    parsed = (
        df_kafka.selectExpr(
            "CAST(value AS STRING) AS json", "partition AS _partition", "offset AS _off"
        )
        .where(col("json").isNotNull())
        .select(from_json(col("json"), envelope).alias("e"), col("_partition"), col("_off"))
        .where(col("e").isNotNull() & col("e.op").isNotNull())
    )
    is_del = col("e.op") == "d"
    flat = parsed.select(
        *[_value(is_del, n, t) for n, t in cols],
        col("_partition"),
        col("_off"),
        # precombine 优先用 Postgres Debezium source.lsn;老消息/非 PG 回退 ts_ms。
        # Hudi 只能配置单字段 precombine,用零填充字符串拼 (lsn_or_ts,offset),
        # 保留 tie-breaker 且避免 lsn*1e6+offset 产生 long overflow。
        format_string(
            EVENT_POS_FORMAT, coalesce(col("e.source.lsn"), col("e.ts_ms"), lit(0)), col("_off")
        ).alias("_event_pos"),
        is_del.alias("_hoodie_is_deleted"),
    ).where(col("id").isNotNull())
    offenders = (
        flat.groupBy("tenant_id", "id")
        .agg(countDistinct("_partition").alias("_partitions"))
        .where(col("_partitions") > 1)
        .limit(1)
        .collect()
    )
    if offenders:
        row = offenders[0]
        raise RuntimeError(
            "CDC ordering requires each tenant_id/id to stay in one Kafka partition; "
            f"found tenant_id={row['tenant_id']!r}, id={row['id']!r} in "
            f"{row['_partitions']} partitions. Verify Debezium Kafka key=PK and topic partition history."
        )
    return flat.drop("_partition", "_off")


def _opts(target_table):
    opts = {
        "hoodie.table.name": target_table,
        "hoodie.datasource.write.table.name": target_table,
        "hoodie.datasource.write.recordkey.field": "tenant_id,id",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
        "hoodie.datasource.write.precombine.field": "_event_pos",
        "hoodie.datasource.write.partitionpath.field": "tenant_id",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.table.type": "MERGE_ON_READ",
        "hoodie.metadata.enable": "true",
        # 表维护(inline 自维护):MOR 每 5 个 delta commit 合并 log→base;
        # cleaner 自动清理旧版本只留最近 10 个 commit(控制存储/小文件)。
        "hoodie.compact.inline": "true",
        "hoodie.compact.inline.max.delta.commits": "5",
        "hoodie.clean.automatic": "true",
        "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS",
        "hoodie.cleaner.commits.retained": "10",
        "hoodie.index.type": "GLOBAL_SIMPLE",
        "hoodie.global.simple.index.parallelism": "1",
        "hoodie.simple.index.update.partition.path": "true",
    }
    # 并发安全:Hudi 默认无锁,多 writer 写同一表会损坏。本架构**流是唯一写者**(单 writer),
    # 默认无需锁;批量回填须在流停止时跑(见 runbook)。
    # 真要多 writer(如流 + 并发回填)再开 OCC——但 **S3/MinIO 不支持零依赖文件锁**
    # (FileSystemBasedLockProvider 需原子 create,s3a 实测报 "Unsupported scheme :s3a"),
    # 只能用 ZooKeeper 跨进程锁:设 HUDI_LOCK_ZK_URL=<host:port> 起用(需另起 ZK)。
    zk = os.getenv("HUDI_LOCK_ZK_URL")
    if zk:
        host, _, port = zk.partition(":")
        opts.update(
            {
                "hoodie.write.concurrency.mode": "optimistic_concurrency_control",
                "hoodie.write.lock.provider": "org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider",
                "hoodie.write.lock.zookeeper.url": host,
                "hoodie.write.lock.zookeeper.port": port or "2181",
                "hoodie.write.lock.zookeeper.lock_key": target_table,
                "hoodie.write.lock.zookeeper.base_path": "/hudi/locks",
                "hoodie.cleaner.policy.failed.writes": "LAZY",
            }
        )
    return opts


def _hudi_table_props(spark, base_path):
    """读取已有 Hudi table config;路径不存在则视为新表。"""
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(f"{base_path}/.hoodie/hoodie.properties")
    fs = path.getFileSystem(conf)
    if not fs.exists(path):
        return {}

    stream = fs.open(path)
    props = jvm.java.util.Properties()
    try:
        props.load(stream)
    finally:
        stream.close()
    names = list(props.stringPropertyNames().toArray())
    return {name: props.getProperty(name) for name in names}


def _assert_hudi_table_compatible(spark, base_path, opts):
    props = _hudi_table_props(spark, base_path)
    if not props:
        return
    expected_key = opts["hoodie.datasource.write.recordkey.field"]
    existing_key = (
        props.get("hoodie.table.recordkey.fields")
        or props.get("hoodie.datasource.write.recordkey.field")
        or ""
    )
    normalized_key = normalize_hudi_record_key(existing_key)
    if not normalized_key:
        raise RuntimeError(
            f"Existing Hudi table at {base_path} has no readable record key in "
            ".hoodie/hoodie.properties. Refuse to write until the table layout is "
            "confirmed or rebuilt/migrated."
        )
    if normalized_key != expected_key:
        raise RuntimeError(
            f"Existing Hudi table at {base_path} uses record key {existing_key!r}, "
            f"but this job requires {expected_key!r}. Rebuild/migrate the table and checkpoint "
            "instead of writing the new key layout into the old table."
        )


def _handler(table, bucket):
    cols = TABLES[table]
    envelope = _envelope(cols)
    base_path = f"s3a://{bucket}/hudi/{table}"
    opts = _opts(f"{table}_hudi")
    compatible_checked = False

    def apply(sub_kafka):
        nonlocal compatible_checked
        flat = _flatten(sub_kafka, cols, envelope)
        if flat.rdd.isEmpty():
            return
        if not compatible_checked:
            _assert_hudi_table_compatible(flat.sparkSession, base_path, opts)
            compatible_checked = True
        flat.write.format("hudi").options(**opts).mode("append").save(base_path)

    return apply


def _reliable(stream_reader):
    """可靠性(批4):显式 failOnDataLoss(默认 true 丢数据即响亮失败),
    可选 maxOffsetsPerTrigger 限制单微批拉取量(背压 + 崩溃后有界恢复)。"""
    sr = stream_reader.option("failOnDataLoss", os.getenv("FAIL_ON_DATA_LOSS", "true"))
    max_off = os.getenv("MAX_OFFSETS_PER_TRIGGER")
    if max_off:
        sr = sr.option("maxOffsetsPerTrigger", max_off)
    return sr


def main() -> None:
    arg = sys.argv[1] if len(sys.argv) > 1 else "all"
    tables = list(TABLES) if arg == "all" else [arg]
    for t in tables:
        if t not in TABLES:
            raise SystemExit(f"usage: cdc_to_hudi.py <all|{'|'.join(TABLES)}>")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
    bucket = os.getenv("HUDI_BUCKET", "warehouse")
    stream = os.getenv("STREAM_MODE", "false").lower() == "true"
    pattern = "cdc\\.public\\.(" + "|".join(tables) + ")"

    spark = SparkSession.builder.appName(f"cdc-to-hudi-{arg}").getOrCreate()
    handlers = {t: _handler(t, bucket) for t in tables}

    def process(batch_kafka):
        for t, apply in handlers.items():
            apply(batch_kafka.where(col("topic") == f"cdc.public.{t}"))

    def reader(fmt):
        return (
            getattr(spark, fmt)
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribePattern", pattern)
        )

    if stream:
        chk = f"s3a://{bucket}/_chk/hudi-{arg}"
        interval = os.getenv("TRIGGER_SECONDS", "10")
        raw = _reliable(reader("readStream").option("startingOffsets", "earliest")).load()
        # queryName 固定:让 streaming 指标名稳定为 spark_lake.driver.hudi-<arg>.*,
        # 否则默认用每次重启都变的 query runId(UUID),Grafana 面板会断、死时序堆积。
        q = (
            raw.writeStream.queryName(f"hudi-{arg}")
            .foreachBatch(lambda bdf, _e: process(bdf))
            .option("checkpointLocation", chk)
            .trigger(processingTime=f"{interval} seconds")
            .start()
        )
        print(f"[cdc_to_hudi] STREAM {pattern} -> hudi/* (chk={chk}, trigger={interval}s)")
        q.awaitTermination()
    else:
        raw = (
            reader("read")
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )
        process(raw)
        print(f"[cdc_to_hudi] BATCH {pattern} -> hudi/* 完成 (tables={tables})")
        spark.stop()


if __name__ == "__main__":
    main()
