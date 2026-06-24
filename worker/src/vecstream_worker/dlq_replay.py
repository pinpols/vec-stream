"""DLQ 重投:把 cdc.dlq 中的消息按 source_topic header 投回源 topic。

幂等链路保证重放安全(确定性 vector_id upsert);修不好的消息会再次进 DLQ。
关键防护:启动时固定各分区 high watermark,只重投水位线**之前**的消息——
否则重投失败的消息回流 DLQ 又被本进程捡起,形成无限乒乓循环。

用法:
    uv run python -m vecstream_worker.dlq_replay            # 重投全部存量
    uv run python -m vecstream_worker.dlq_replay --dry-run  # 只看不投
    uv run python -m vecstream_worker.dlq_replay --limit 10
"""
import argparse
import logging
import sys

from confluent_kafka import Consumer, Producer, TopicPartition

from .config import Config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dlq-replay")


def replay(cfg: Config, limit: int | None, dry_run: bool) -> int:
    consumer = Consumer(
        {
            "bootstrap.servers": cfg.kafka_bootstrap,
            "group.id": f"{cfg.kafka_group_id}-dlq-replay",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer({"bootstrap.servers": cfg.kafka_bootstrap})

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
            if source_topic is None:
                log.warning("skip message without source_topic header @ offset %s", msg.offset())
                consumer.commit(msg)
                continue
            if dry_run:
                log.info("[dry-run] would replay → %s (error was: %s) value=%.80s",
                         source_topic, error, msg.value())
            else:
                producer.produce(source_topic, key=msg.key(), value=msg.value())
                producer.flush(10)
                consumer.commit(msg)
                log.info("replayed offset=%s → %s (error was: %s)", msg.offset(), source_topic, error)
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
