"""健康监控线程(DESIGN.md §5):
- replication slot lag:slot 不被消费时 PG 会无限堆 WAL 直到磁盘爆,超阈值告警;
- DLQ 积压:high watermark 与 replay group 已 commit 位点的差值。
两者同时写日志与 Prometheus Gauge。"""
import logging
import threading
import time

import psycopg
from confluent_kafka import Consumer, TopicPartition

from .metrics import DLQ_BACKLOG, SLOT_ACTIVE, SLOT_LAG_BYTES

log = logging.getLogger("monitor")

LAG_SQL = """
SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0) AS lag_bytes,
       active
FROM pg_replication_slots
WHERE slot_name = %s
"""


def check_slot_lag(conn, slot_name: str) -> tuple[int, bool] | None:
    """返回 (lag_bytes, active);slot 不存在返回 None。"""
    with conn.cursor() as cur:
        cur.execute(LAG_SQL, (slot_name,))
        row = cur.fetchone()
    return (int(row[0]), bool(row[1])) if row else None


def check_dlq_backlog(bootstrap: str, group_id: str, dlq_topic: str) -> int | None:
    """DLQ 积压 = Σ(high watermark - replay group 已 commit 位点)。topic 不存在返回 None。
    注意:replay group 按 worker group 派生({group}-dlq-replay),DLQ topic 是共享的,
    所以每个 worker 的 backlog 是「本管线 replay 视角」的积压,各 worker 读数可不同。
    用临时 Consumer 查询,用完即关,不留长连接。"""
    consumer = Consumer(
        {"bootstrap.servers": bootstrap, "group.id": group_id, "enable.auto.commit": False}
    )
    try:
        meta = consumer.list_topics(dlq_topic, timeout=10)
        topic_meta = meta.topics.get(dlq_topic)
        if topic_meta is None or topic_meta.error is not None:
            return None
        tps = [TopicPartition(dlq_topic, p) for p in topic_meta.partitions]
        committed = {tp.partition: tp.offset for tp in consumer.committed(tps, timeout=10)}
        backlog = 0
        for tp in tps:
            _, hi = consumer.get_watermark_offsets(tp, timeout=10)
            offset = committed.get(tp.partition, -1001)
            backlog += hi - (offset if offset >= 0 else 0)
        return backlog
    finally:
        consumer.close()


def start_monitor(cfg) -> threading.Thread:
    def loop():
        conn = None
        while True:
            try:
                if conn is None or conn.closed:
                    conn = psycopg.connect(cfg.pg_dsn, autocommit=True)
                result = check_slot_lag(conn, cfg.slot_name)
                if result is None:
                    log.warning("slot %s not found (connector 未注册?)", cfg.slot_name)
                else:
                    lag_bytes, active = result
                    SLOT_LAG_BYTES.set(lag_bytes)
                    SLOT_ACTIVE.set(1 if active else 0)
                    lag_mb = lag_bytes / 1024 / 1024
                    if not active:
                        log.warning("slot %s INACTIVE, lag=%.1fMB — connector 掉线,WAL 正在累积!",
                                    cfg.slot_name, lag_mb)
                    elif lag_mb >= cfg.slot_lag_warn_mb:
                        log.warning("slot %s lag=%.1fMB 超过阈值 %dMB",
                                    cfg.slot_name, lag_mb, cfg.slot_lag_warn_mb)
                    else:
                        log.info("slot %s lag=%.1fMB active=%s", cfg.slot_name, lag_mb, active)
            except Exception as e:  # noqa: BLE001 —— 监控不能拖垮主流程
                log.warning("slot check failed: %s", e)
                conn = None
            try:
                backlog = check_dlq_backlog(
                    cfg.kafka_bootstrap, f"{cfg.kafka_group_id}-dlq-replay", cfg.dlq_topic
                )
                if backlog is not None:
                    DLQ_BACKLOG.set(backlog)
                    if backlog > 0:
                        log.info("DLQ backlog=%d (replay 待处理)", backlog)
            except Exception as e:  # noqa: BLE001
                log.warning("dlq backlog check failed: %s", e)
            time.sleep(cfg.slot_check_interval_s)

    t = threading.Thread(target=loop, name="monitor", daemon=True)
    t.start()
    return t
