"""读 Hudi / Iceberg 表做校验(给 smoke / 人工核对)。

  query.py <hudi|iceberg> <table> [id]
  query.py <hudi|iceberg> <table> <tenant_id> <id>

无 id:打印 COUNT=<行数>;有 id:打印 RESULT=<status|ABSENT>。
兼容旧调用:只传 id 时默认 tenant_id=default。
catalog/conf 由 run.sh 按引擎注入。
"""

import os
import sys

from pyspark.sql import SparkSession


def main() -> None:
    engine = sys.argv[1]
    table = sys.argv[2]
    tenant_id = "default"
    pk = None
    if len(sys.argv) == 4:
        pk = sys.argv[3]
    elif len(sys.argv) > 4:
        tenant_id = sys.argv[3]
        pk = sys.argv[4]
    spark = SparkSession.builder.appName(f"query-{engine}-{table}").getOrCreate()

    if engine == "iceberg":
        df = spark.table(f"ice.lake.{table}")
    else:  # hudi
        bucket = os.getenv("HUDI_BUCKET", "warehouse")
        df = spark.read.format("hudi").load(f"s3a://{bucket}/hudi/{table}")

    if pk:
        # 不假设有 status 列(comment 表没有);有则回它的值,否则回 PRESENT 表示行在
        probe = "status" if "status" in df.columns else "id"
        rows = df.filter((df.tenant_id == tenant_id) & (df.id == int(pk))).select(probe).collect()
        if not rows:
            print("RESULT=ABSENT")
        else:
            print("RESULT=" + (str(rows[0][probe]) if probe == "status" else "PRESENT"))
    else:
        print(f"COUNT={df.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
