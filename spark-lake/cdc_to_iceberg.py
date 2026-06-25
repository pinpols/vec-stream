"""CDC(cdc.public.* JSON envelope)→ Apache Iceberg(Spark,批量 / 连续流,多表)。

table=all(默认):一个作业 subscribePattern 订阅三表 topic,foreachBatch 按 topic 路由到各表。
table=<name>:单表。STREAM_MODE=true 走 Structured Streaming,否则批量。
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, row_number, when
from pyspark.sql.types import LongType, StringType, StructField, StructType
from pyspark.sql.window import Window

TABLES = {
    "article": [("id", "bigint"), ("tenant_id", "string"), ("title", "string"),
                ("body", "string"), ("status", "string"), ("updated_at", "string")],
    "product": [("id", "bigint"), ("tenant_id", "string"), ("name", "string"),
                ("description", "string"), ("status", "string"), ("updated_at", "string")],
    "comment": [("id", "bigint"), ("tenant_id", "string"), ("article_id", "bigint"),
                ("body", "string"), ("updated_at", "string")],
}
_SPARK = {"bigint": LongType(), "string": StringType()}


def _envelope(cols):
    row = StructType([StructField(c, _SPARK[t], True) for c, t in cols])
    return StructType([
        StructField("before", row, True), StructField("after", row, True),
        StructField("op", StringType(), True), StructField("ts_ms", LongType(), True),
    ])


def _latest(df_kafka, names, envelope):
    parsed = (df_kafka.selectExpr("CAST(value AS STRING) AS json", "offset")
              .where(col("json").isNotNull())
              .select(from_json(col("json"), envelope).alias("e"), col("offset"))
              .where(col("e").isNotNull() & col("e.op").isNotNull()))
    is_del = col("e.op") == "d"
    src = when(is_del, col("e.before")).otherwise(col("e.after"))
    flat = parsed.select(
        *[src.getField(c).alias(c) for c in names],
        col("e.op").alias("_op"), col("e.ts_ms").alias("_ts"), col("offset").alias("_off"),
    ).where(col("id").isNotNull())
    # 同一 id 的所有事件在同一 Kafka 分区,offset 严格单调=事件真实顺序;以 offset 为主键
    # 比 ts_ms 更稳(ts_ms 同事务内会并列、个别版本可能为 null,desc_nulls_last 会误选旧事件)。
    w = Window.partitionBy("id").orderBy(col("_off").desc())
    return flat.withColumn("_rn", row_number().over(w)).where(col("_rn") == 1).drop("_rn", "_ts", "_off")


def _handler(spark, table):
    """每表:建表 + 返回一个把该表的 kafka 子批 MERGE 进 Iceberg 的函数。"""
    cols = TABLES[table]
    names = [c for c, _ in cols]
    ident = f"ice.lake.{table}"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {ident} ({', '.join(f'{c} {t}' for c, t in cols)}) USING iceberg")
    envelope = _envelope(cols)
    set_clause = ", ".join(f"t.{c}=s.{c}" for c in names if c != "id")
    ins_cols, ins_vals = ", ".join(names), ", ".join(f"s.{c}" for c in names)

    def apply(sub_kafka):
        latest = _latest(sub_kafka, names, envelope)
        if latest.rdd.isEmpty():
            return
        latest.createOrReplaceTempView("changes")
        latest.sparkSession.sql(f"""
            MERGE INTO {ident} t USING changes s ON t.id = s.id
            WHEN MATCHED AND s._op = 'd' THEN DELETE
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED AND s._op <> 'd' THEN INSERT ({ins_cols}) VALUES ({ins_vals})
        """)
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
            raise SystemExit(f"usage: cdc_to_iceberg.py <all|{'|'.join(TABLES)}>")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
    bucket = os.getenv("HUDI_BUCKET", "warehouse")
    stream = os.getenv("STREAM_MODE", "false").lower() == "true"
    pattern = "cdc\\.public\\.(" + "|".join(tables) + ")"

    spark = SparkSession.builder.appName(f"cdc-to-iceberg-{arg}").getOrCreate()
    spark.sql("CREATE DATABASE IF NOT EXISTS ice.lake")
    handlers = {t: _handler(spark, t) for t in tables}

    def process(batch_kafka):  # batch 带 topic 列,按表路由
        for t, apply in handlers.items():
            apply(batch_kafka.where(col("topic") == f"cdc.public.{t}"))

    reader = (lambda fmt: getattr(spark, fmt).format("kafka")
              .option("kafka.bootstrap.servers", bootstrap)
              .option("subscribePattern", pattern))
    if stream:
        chk = f"s3a://{bucket}/_chk/iceberg-{arg}"
        interval = os.getenv("TRIGGER_SECONDS", "10")
        raw = _reliable(reader("readStream").option("startingOffsets", "earliest")).load()
        q = (raw.writeStream.foreachBatch(lambda bdf, _e: process(bdf))
             .option("checkpointLocation", chk).trigger(processingTime=f"{interval} seconds").start())
        print(f"[cdc_to_iceberg] STREAM {pattern} -> ice.lake.* (chk={chk}, trigger={interval}s)")
        q.awaitTermination()
    else:
        raw = reader("read").option("startingOffsets", "earliest").option("endingOffsets", "latest").load()
        process(raw)
        print(f"[cdc_to_iceberg] BATCH {pattern} -> ice.lake.* 完成 (tables={tables})")
        spark.stop()


if __name__ == "__main__":
    main()
