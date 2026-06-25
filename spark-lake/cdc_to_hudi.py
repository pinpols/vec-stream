"""CDC(cdc.public.* JSON envelope)→ Hudi(Spark,批量 / 连续流,多表)。

table=all(默认):一个作业 subscribePattern 订阅三表 topic,foreachBatch 按 topic 路由到各表。
table=<name>:单表。STREAM_MODE=true 走 Structured Streaming,否则批量。
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, from_json, lit, when
from pyspark.sql.types import LongType, StringType, StructField, StructType

TABLES = {
    "article": [("id", LongType()), ("tenant_id", StringType()), ("title", StringType()),
                ("body", StringType()), ("status", StringType()), ("updated_at", StringType())],
    "product": [("id", LongType()), ("tenant_id", StringType()), ("name", StringType()),
                ("description", StringType()), ("status", StringType()), ("updated_at", StringType())],
    "comment": [("id", LongType()), ("tenant_id", StringType()), ("article_id", LongType()),
                ("body", StringType()), ("updated_at", StringType())],
}


def _envelope(cols):
    row = StructType([StructField(n, t, True) for n, t in cols])
    return StructType([
        StructField("before", row, True), StructField("after", row, True),
        StructField("op", StringType(), True), StructField("ts_ms", LongType(), True),
    ])


def _value(src, name):
    # tenant_id 是分区路径,null 会落 __HIVE_DEFAULT_PARTITION__ 且与 GLOBAL_SIMPLE 跨分区
    # 移动打架,统一兜底到固定占位符,保证分区路径稳定。
    if name == "tenant_id":
        return coalesce(src.getField(name), lit("__unknown__")).alias(name)
    return src.getField(name).alias(name)


def _flatten(df_kafka, cols, envelope):
    parsed = (df_kafka.selectExpr("CAST(value AS STRING) AS json", "offset AS _off")
              .where(col("json").isNotNull())
              .select(from_json(col("json"), envelope).alias("e"), col("_off"))
              .where(col("e").isNotNull() & col("e.op").isNotNull()))
    is_del = col("e.op") == "d"
    src = when(is_del, col("e.before")).otherwise(col("e.after"))
    return parsed.select(
        *[_value(src, n) for n, _ in cols],
        # precombine 用 ts_ms*1e6+offset 合成单调序:ts_ms 同毫秒/为 null 时仍按 offset
        #(同 id 同分区 offset 严格单调=真实顺序)定胜者,避免旧事件覆盖新事件(与 Iceberg 侧一致)。
        (coalesce(col("e.ts_ms"), lit(0)) * lit(1000000) + col("_off")).alias("_ts"),
        is_del.alias("_hoodie_is_deleted"),
    ).where(col("id").isNotNull())


def _opts(target_table):
    return {
        "hoodie.table.name": target_table,
        "hoodie.datasource.write.table.name": target_table,
        "hoodie.datasource.write.recordkey.field": "id",
        "hoodie.datasource.write.precombine.field": "_ts",
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
        # 并发安全:乐观并发控制(OCC)+ 跨进程文件锁。Hudi 默认无锁,多 writer(如流 +
        # 批量回填同时跑)写同一张表会损坏;OCC 让并发提交在表的 .hoodie/.locks 上串行化,
        # 冲突方干净 abort 而非破坏数据。FileSystemBasedLockProvider 直接用 S3/MinIO 上的
        # 表目录,零额外基础设施。OCC 要求失败写清理为 LAZY(否则会删并发方在途文件)。
        "hoodie.write.concurrency.mode": "optimistic_concurrency_control",
        "hoodie.write.lock.provider": "org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider",
        "hoodie.cleaner.policy.failed.writes": "LAZY",
    }


def _handler(table, bucket):
    cols = TABLES[table]
    envelope = _envelope(cols)
    base_path = f"s3a://{bucket}/hudi/{table}"
    opts = _opts(f"{table}_hudi")

    def apply(sub_kafka):
        flat = _flatten(sub_kafka, cols, envelope)
        if flat.rdd.isEmpty():
            return
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

    reader = (lambda fmt: getattr(spark, fmt).format("kafka")
              .option("kafka.bootstrap.servers", bootstrap)
              .option("subscribePattern", pattern))
    if stream:
        chk = f"s3a://{bucket}/_chk/hudi-{arg}"
        interval = os.getenv("TRIGGER_SECONDS", "10")
        raw = _reliable(reader("readStream").option("startingOffsets", "earliest")).load()
        # queryName 固定:让 streaming 指标名稳定为 spark_lake.driver.hudi-<arg>.*,
        # 否则默认用每次重启都变的 query runId(UUID),Grafana 面板会断、死时序堆积。
        q = (raw.writeStream.queryName(f"hudi-{arg}").foreachBatch(lambda bdf, _e: process(bdf))
             .option("checkpointLocation", chk).trigger(processingTime=f"{interval} seconds").start())
        print(f"[cdc_to_hudi] STREAM {pattern} -> hudi/* (chk={chk}, trigger={interval}s)")
        q.awaitTermination()
    else:
        raw = reader("read").option("startingOffsets", "earliest").option("endingOffsets", "latest").load()
        process(raw)
        print(f"[cdc_to_hudi] BATCH {pattern} -> hudi/* 完成 (tables={tables})")
        spark.stop()


if __name__ == "__main__":
    main()
