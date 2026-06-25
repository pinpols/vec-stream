"""Iceberg 表维护(后台 table service):小文件合并 + manifest 重写 + 快照过期。

Iceberg 没有 inline 维护,写入会累积小文件 + 快照,生产必须定期跑本作业(cron / 调度)。
用法:run.sh maintain-iceberg <all|article|product|comment>
"""

import sys

from pyspark.sql import SparkSession

TABLES = ["article", "product", "comment"]
RETAIN_SNAPSHOTS = 5


def main() -> None:
    arg = sys.argv[1] if len(sys.argv) > 1 else "all"
    tables = TABLES if arg == "all" else [arg]
    spark = SparkSession.builder.appName(f"iceberg-maintain-{arg}").getOrCreate()
    # 过期截止时间(字面量;过程不接受 current_timestamp() 函数)。生产改 -INTERVAL '7' DAYS。
    older_than = spark.sql("SELECT date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS t").collect()[0]["t"]
    for t in tables:
        ident = f"lake.{t}"
        try:
            # 小文件合并(bin-pack;把 CDC 高频小写攒成大文件)
            spark.sql(f"CALL ice.system.rewrite_data_files(table => '{ident}')")
            # manifest 重写(元数据合并,加速 planning)
            spark.sql(f"CALL ice.system.rewrite_manifests('{ident}')")
            # 快照过期:older_than 默认是 5 天前(生产按 time-travel 窗口设);这里 now 以演示真过期,
            # retain_last 保证至少留最近 N 个。生产改成 current_timestamp() - INTERVAL '7' DAYS。
            spark.sql(
                f"CALL ice.system.expire_snapshots("
                f"table => '{ident}', older_than => TIMESTAMP '{older_than}', retain_last => {RETAIN_SNAPSHOTS})"
            )
            print(f"[maintain] {ident} ok")
        except Exception as e:  # noqa: BLE001
            print(f"[maintain] {ident} 跳过/失败: {type(e).__name__}: {str(e)[:200]}")
    spark.stop()


if __name__ == "__main__":
    main()
