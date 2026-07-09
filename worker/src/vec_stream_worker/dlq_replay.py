"""DLQ 重投:把 cdc.dlq 中的消息按 source_topic header 投回源 topic。

幂等链路保证重放安全(确定性 vector_id upsert);修不好的消息会再次进 DLQ。
关键防护:启动时固定各分区 high watermark,只重投水位线**之前**的消息——
否则重投失败的消息回流 DLQ 又被本进程捡起,形成无限乒乓循环。

用法:
    uv run python -m vec_stream_worker.dlq_replay            # 重投全部存量
    uv run python -m vec_stream_worker.dlq_replay --dry-run  # 只看不投
    uv run python -m vec_stream_worker.dlq_replay --limit 10
"""

import argparse
import logging
import sys

import psycopg
from confluent_kafka import Consumer, Producer, TopicPartition

from .config import Config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dlq-replay")


def _archive(dsn: str, msg, source_topic: str | None, replay_count: int, error: str) -> None:
    """重投超限的死信落档 PG(M2:不无限重投,留待人工排查/选择性重放)。"""
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO dead_letter_archive "
            "(source_topic, dlq_partition, dlq_offset, replay_count, error, payload) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (source_topic, msg.partition(), msg.offset(), replay_count, error[:2000], msg.value()),
        )


def replay(cfg: Config, limit: int | None, dry_run: bool) -> int:
    consumer = Consumer(
        {
            "bootstrap.servers": cfg.kafka_bootstrap,
            "group.id": f"{cfg.kafka_group_id}-dlq-replay",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    # message.max.bytes 与 worker DLQ producer(main.py)对齐:死信本身可到 ~5MB,
    # 默认 1MB 上限会让 produce 同步抛 KafkaException 直接炸掉整个 replay 进程。
    producer = Producer(
        {"bootstrap.servers": cfg.kafka_bootstrap, "message.max.bytes": 5242880}
    )

    # 固定本次运行的处理上界(乒乓循环防护)
    meta = consumer.list_topics(cfg.dlq_topic, timeout=10)
    topic_meta = meta.topics.get(cfg.dlq_topic)
    if topic_meta is None or topic_meta.error is not None:
        log.info("DLQ topic %s 不存在,无可重投", cfg.dlq_topic)
        consumer.close()
        return 0
    high_watermarks: dict[int, int] = {}
    assignment = []
    for pid in topic_meta.partitions:
        tp = TopicPartition(cfg.dlq_topic, pid)
        _, hi = consumer.get_watermark_offsets(tp, timeout=10)
        high_watermarks[pid] = hi
        assignment.append(tp)
    consumer.assign(assignment)  # 从该 group 已 commit 的位置继续

    pending = {p for p, hi in high_watermarks.items() if hi > 0}
    replayed = 0
    idle_polls = 0
    try:
        while pending and idle_polls < 5 and (limit is None or replayed < limit):
            msg = consumer.poll(2.0)
            if msg is None or msg.error():
                idle_polls += 1
                continue
            idle_polls = 0
            if msg.offset() >= high_watermarks[msg.partition()]:
                # 到达启动时的水位线:之后的消息(含本次重投回流的)留给下次运行
                pending.discard(msg.partition())
                continue
            headers = dict(msg.headers() or [])
            source_topic = (headers.get("source_topic") or b"").decode() or None
            error = (headers.get("error") or b"").decode()
            replay_count = int((headers.get("replay_count") or b"0").decode() or "0")
            if source_topic is None:
                log.warning("skip message without source_topic header @ offset %s", msg.offset())
                consumer.commit(msg)
                continue
            # 重投次数上限:超限不再回投源 topic,落档 dead_letter_archive 待人工排查
            if replay_count >= cfg.dlq_max_replays:
                if dry_run:
                    log.info(
                        "[dry-run] would ARCHIVE (replay_count=%d ≥ %d) error=%s",
                        replay_count,
                        cfg.dlq_max_replays,
                        error,
                    )
                else:
                    try:
                        _archive(cfg.pg_dsn, msg, source_topic, replay_count, error)
                    except Exception as e:  # noqa: BLE001
                        # 归档失败(表缺失/DB 故障)不能拖垮整个 replay 进程;不 commit
                        # 该消息(留待下次重试归档),跳过继续处理其余消息。
                        log.error("archive failed offset=%s,跳过不 commit: %s", msg.offset(), e)
                        continue
                    consumer.commit(msg)
                    log.warning(
                        "archived offset=%s (重投 %d 次仍失败) error=%s",
                        msg.offset(),
                        replay_count,
                        error,
                    )
                replayed += 1
                if msg.offset() + 1 >= high_watermarks[msg.partition()]:
                    pending.discard(msg.partition())
                continue
            if dry_run:
                log.info(
                    "[dry-run] would replay(#%d) → %s (error was: %s) value=%.80s",
                    replay_count + 1,
                    source_topic,
                    error,
                    msg.value(),
                )
            else:
                # 带递增的 replay_count:消息再次进 DLQ 时计数累加,最终触发归档
                new_headers = [(k, v) for k, v in (msg.headers() or []) if k != "replay_count"]
                new_headers.append(("replay_count", str(replay_count + 1).encode()))
                # 重投必须校验 delivery 结果(与 worker DLQ 投递同款防护):broker 拒收/
                # 超时若不校验,DLQ offset 照常 commit → 死信既不在源 topic 也不再被
                # 消费,静默永久丢失。失败抛异常不 commit,下次运行重投。
                delivery_failures: list = []

                def _on_delivery(err, _m, _sink=delivery_failures):
                    if err is not None:
                        _sink.append(err)

                producer.produce(
                    source_topic,
                    key=msg.key(),
                    value=msg.value(),
                    headers=new_headers,
                    on_delivery=_on_delivery,
                )
                remaining = producer.flush(10)
                if delivery_failures or remaining:
                    raise RuntimeError(
                        f"replay produce to {source_topic} failed "
                        f"(failures={[str(e) for e in delivery_failures]}, "
                        f"un-flushed={remaining}); DLQ offset 不 commit,下次运行重投"
                    )
                consumer.commit(msg)
                log.info(
                    "replayed(#%d) offset=%s → %s (error was: %s)",
                    replay_count + 1,
                    msg.offset(),
                    source_topic,
                    error,
                )
            replayed += 1
            if msg.offset() + 1 >= high_watermarks[msg.partition()]:
                pending.discard(msg.partition())
    finally:
        consumer.close()
    log.info("done, %s %d message(s)", "would replay" if dry_run else "replayed", replayed)
    return replayed


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay DLQ messages to source topics")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    replay(Config(), args.limit, args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
