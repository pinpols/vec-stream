"""Iceberg 表维护(后台 table service):小文件合并 + manifest 重写 + 快照过期。

Iceberg 没有 inline 维护,写入会累积小文件 + 快照,生产必须定期跑本作业(cron / 调度)。
用法:run.sh maintain-iceberg <all|article|product|comment>
"""

import os
import sys

from pyspark.sql import SparkSession

TABLES = ["article", "product", "comment"]
RETAIN_SNAPSHOTS = 5
# 过期安全窗口(小时):只过期早于 now-N 小时的快照,默认 7 天,避免误删并发写入/读取
# 仍在引用的近期快照。冒烟/演示要"立刻见到过期"可设 EXPIRE_OLDER_THAN_HOURS=0。
EXPIRE_OLDER_THAN_HOURS = os.getenv("EXPIRE_OLDER_THAN_HOURS", "168")


def main() -> None:
    arg = sys.argv[1] if len(sys.argv) > 1 else "all"
    tables = TABLES if arg == "all" else [arg]
    spark = SparkSession.builder.appName(f"iceberg-maintain-{arg}").getOrCreate()
    # 字面量截止时间(过程不接受 current_timestamp() 函数);带安全窗口
    older_than = spark.sql(
        f"SELECT date_format(current_timestamp() - INTERVAL '{EXPIRE_OLDER_THAN_HOURS}' HOURS,"
        f" 'yyyy-MM-dd HH:mm:ss') AS t"
    ).collect()[0]["t"]
    failed = []
    for t in tables:
        ident = f"lake.{t}"
        try:
            # 小文件合并(bin-pack;把 CDC 高频小写攒成大文件)
            spark.sql(f"CALL ice.system.rewrite_data_files(table => '{ident}')")
            # manifest 重写(元数据合并,加速 planning)
            spark.sql(f"CALL ice.system.rewrite_manifests('{ident}')")
            # 快照过期:retain_last 保证至少留最近 N 个,older_than 之前的才删
            spark.sql(
                f"CALL ice.system.expire_snapshots("
                f"table => '{ident}', older_than => TIMESTAMP '{older_than}', retain_last => {RETAIN_SNAPSHOTS})"
            )
            print(f"[maintain] {ident} ok")
        except Exception as e:  # noqa: BLE001
            # 不能静默跳过:维护失败比未维护更危险(造成"已维护"假象,快照无限累积撑爆存储)
            print(f"[maintain] {ident} 失败: {type(e).__name__}: {str(e)[:200]}", file=sys.stderr)
            failed.append(ident)
    spark.stop()
    if failed:
        raise SystemExit(f"[maintain] 失败表: {failed}(非零退出,让调度感知)")


if __name__ == "__main__":
    main()
