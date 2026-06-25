"""读 Hudi / Iceberg 表做校验(给 smoke / 人工核对)。

  query.py <hudi|iceberg> <table> [id]

无 id:打印 COUNT=<行数>;有 id:打印 RESULT=<status|ABSENT>。
catalog/conf 由 run.sh 按引擎注入。
"""

import os
import sys

from pyspark.sql import SparkSession


def main() -> None:
    engine = sys.argv[1]
    table = sys.argv[2]
    pk = sys.argv[3] if len(sys.argv) > 3 else None
    spark = SparkSession.builder.appName(f"query-{engine}-{table}").getOrCreate()

    if engine == "iceberg":
        df = spark.table(f"ice.lake.{table}")
    else:  # hudi
        bucket = os.getenv("HUDI_BUCKET", "warehouse")
        df = spark.read.format("hudi").load(f"s3a://{bucket}/hudi/{table}")

    if pk:
        # 不假设有 status 列(comment 表没有);有则回它的值,否则回 PRESENT 表示行在
        probe = "status" if "status" in df.columns else "id"
        rows = df.filter(df.id == int(pk)).select(probe).collect()
        if not rows:
            print("RESULT=ABSENT")
        else:
            print("RESULT=" + (str(rows[0][probe]) if probe == "status" else "PRESENT"))
    else:
        print(f"COUNT={df.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
