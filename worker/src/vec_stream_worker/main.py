"""Vector Sync Worker 入口:消费 Debezium CDC 事件 → embedding → 写 pgvector。

完整 CDC 语义(DESIGN.md §3.3a)+ 多表:
  c / r        → 构建文档 → hash 比对 → embed → upsert
  u            → 同上;文本 hash 未变只刷 metadata(不重 embed)
  d            → 按 PK 删除该行全部 chunk
  tombstone    → 忽略(delete 已由 op=d 处理)

表路由:正则订阅 cdc.public.*,topic 尾段即表名,字段映射见 config.tables;
未配置的表跳过(进 Debezium include.list 但没配映射 = 显式不同步)。

投递语义:至少一次 —— 处理成功后才 commit offset;处理失败退避重试,
重试耗尽发 DLQ 后 commit(确定性 vector_id 保证重放幂等)。
"""
import json
import logging
import signal
import sys
import time

import psycopg
from confluent_kafka import Consumer, KafkaError, Producer
from qdrant_client.http.exceptions import ResponseHandlingException

# 基础设施瞬时故障:无限重试不进 DLQ(进了也修不好,且会放大故障);
# 数据性错误(解析失败/缺字段等):有限重试后进 DLQ。
TRANSIENT_ERRORS = (
    psycopg.OperationalError,
    psycopg.InterfaceError,
    ResponseHandlingException,
    ConnectionError,
    OSError,
)

from .chunker import split_text
from .config import Config
from .embedder import make_embedder
from .ids import text_hash
from .metrics import CHUNKS_EMBEDDED, DLQ_SENT, EVENTS, SYNC_DELAY, start_metrics
from .schema_check import check_schema
from .sink import make_sink
from .slot_monitor import start_monitor
from .source_db import SourceDB

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
log = logging.getLogger("worker")

UPSERT_OPS = {"c", "r", "u"}


def table_from_topic(topic: str) -> str:
    """cdc.public.article → article"""
    return topic.rsplit(".", 1)[-1]


def build_source_text(after: dict, fields: list[str]) -> str:
    return "\n".join(str(after[f]) for f in fields if after.get(f))


def process_event(
    event: dict, table: str, cfg: Config, embedder, sink, source_db=None,
    offset_ref: tuple[str, int, int] | None = None,
) -> str:
    """处理单条 CDC 事件,返回动作标签
    (upserted/skipped/metadata_refreshed/deleted/ignored),便于测试与统计。

    offset_ref=(topic, partition, offset):当前消息的 Kafka 坐标,透传给 sink
    与向量写入同事务写进 processed_offsets 处理账本(M1 审计闭环)。"""
    table_cfg = cfg.tables.get(table)
    if table_cfg is None:
        log.info("table %s not configured, ignore", table)
        return "ignored"

    # 子表:自身不进向量库,任何变更(含删除)都触发父行重新 embed
    parent = table_cfg.get("reembed_parent")
    if parent:
        if source_db is None:
            log.warning("%s 配置了 reembed_parent 但无 source_db,ignore", table)
            return "ignored"
        row_image = event.get("after") or event.get("before") or {}
        fk = row_image.get(parent["fk"])
        if fk is None:
            return "ignored"
        parent_table = parent["table"]
        parent_pk_field = cfg.tables.get(parent_table, {}).get("pk", "id")
        parent_row = source_db.fetch_row(parent_table, parent_pk_field, fk)
        if parent_row is None:
            # 父行已删:父表自己的 op=d 事件负责清理向量
            return "ignored"
        log.info("%s 变更 → 重建父文档 %s pk=%s", table, parent_table, fk)
        return process_event(
            {"op": "u", "after": parent_row}, parent_table, cfg, embedder, sink, source_db,
            offset_ref=offset_ref,
        )

    pk_field = table_cfg.get("pk", "id")
    op = event.get("op")

    if op in UPSERT_OPS:
        after = event.get("after") or {}
        pk = after.get(pk_field)
        if pk is None:
            log.warning("op=%s without pk, ignore", op)
            return "ignored"
        # 以源库当前态为准重建文档,CDC 事件只当触发器:
        # 同时解决 a) DLQ 重投旧 after 镜像覆盖新状态 b) 跨表反查与本表事件的乱序竞态
        # ——无论事件新旧,落库结果始终收敛到源库当前状态。
        if source_db is not None:
            current = source_db.fetch_row(table, pk_field, pk)
            if current is None:
                tenant = after.get("tenant_id", "default")
                deleted = sink.delete_row(tenant, table, str(pk), offset_ref=offset_ref)
                log.info("%s pk=%s 源行已不存在,清理向量 %d 条", table, pk, deleted)
                return "deleted"
            after = current
        tenant = after.get("tenant_id", "default")
        source_text = build_source_text(after, table_cfg["fields"])
        if not source_text:
            log.info("%s pk=%s empty source_text, ignore", table, pk)
            return "ignored"
        # 跨表反查:关联文本追加进 source_text(参与 hash,关联数据变了也会重 embed);
        # 反查必须带 tenant 条件,防跨租户数据混入文档
        enrich_sql = table_cfg.get("enrich_sql")
        if enrich_sql and source_db is not None:
            extra = source_db.query_text(enrich_sql, {"pk": pk, "tenant": tenant})
            if extra:
                source_text = f"{source_text}\n{extra}"
        if len(source_text) > cfg.max_doc_chars:
            log.warning("%s pk=%s 文本过长 %d→%d chars 截断", table, pk,
                        len(source_text), cfg.max_doc_chars)
            source_text = source_text[: cfg.max_doc_chars]
        new_hash = text_hash(source_text)
        metadata = {
            "status": after.get("status"),
            "title": after.get(table_cfg.get("title_field", table_cfg["fields"][0])),
        }
        # hash 去重:文本未变只刷 metadata(status 等过滤字段必须跟上),跳过最贵的 embedding
        if sink.get_text_hash(tenant, table, str(pk)) == new_hash:
            sink.update_metadata(tenant, table, str(pk), metadata, offset_ref=offset_ref)
            log.info("%s pk=%s hash unchanged, metadata refreshed (op=%s)", table, pk, op)
            return "metadata_refreshed"
        chunks = split_text(source_text, cfg.chunk_size, cfg.chunk_overlap)
        embeddings = embedder.embed_passages(chunks)
        CHUNKS_EMBEDDED.inc(len(chunks))
        sink.upsert_row(
            tenant_id=tenant,
            source_table=table,
            source_pk=str(pk),
            text_hash=new_hash,
            chunks=chunks,
            embeddings=embeddings,
            metadata=metadata,
            offset_ref=offset_ref,
        )
        log.info("upserted %s pk=%s chunks=%d op=%s", table, pk, len(chunks), op)
        return "upserted"

    if op == "d":
        before = event.get("before") or {}
        pk = before.get(pk_field)
        if pk is None:
            log.warning("op=d without before image, ignore: %s", event)
            return "ignored"
        tenant = before.get("tenant_id", "default")
        deleted = sink.delete_row(tenant, table, str(pk), offset_ref=offset_ref)
        log.info("deleted %s pk=%s chunks=%d", table, pk, deleted)
        return "deleted"

    log.info("op=%s not handled, ignore", op)
    return "ignored"


def handle_message(msg, cfg: Config, embedder, sink, dlq: Producer, source_db) -> None:
    """解析 + 处理 + 重试;数据性错误重试耗尽发 DLQ,基础设施瞬时故障无限退避重试
    (阻塞本分区是正确行为:至少一次 + 有序消费;max.poll.interval 已调大配合)。
    任何路径结束后调用方 commit。"""
    table = table_from_topic(msg.topic())
    # 当前消息的 Kafka 坐标 → 与向量写入同事务写进处理账本(PG 侧审计真相源)
    offset_ref = (msg.topic(), msg.partition(), msg.offset())
    last_err: Exception | None = None
    data_attempts = 0
    transient_attempts = 0
    while True:
        try:
            event = json.loads(msg.value())
            if event is not None:
                action = process_event(
                    event, table, cfg, embedder, sink, source_db, offset_ref=offset_ref
                )
                EVENTS.labels(table=table, action=action).inc()
                if event.get("ts_ms"):
                    SYNC_DELAY.observe(max(0.0, time.time() - event["ts_ms"] / 1000))
            return
        except TRANSIENT_ERRORS as e:
            transient_attempts += 1
            wait = min(30.0, cfg.retry_backoff_s * (2 ** min(transient_attempts, 5)))
            log.warning("transient infra error (attempt %d), retry in %.0fs: %s",
                        transient_attempts, wait, e)
            time.sleep(wait)
        except Exception as e:  # noqa: BLE001 —— 数据性错误,有限重试
            last_err = e
            data_attempts += 1
            log.warning("attempt %d/%d failed: %s", data_attempts, cfg.max_retries, e)
            if data_attempts >= cfg.max_retries:
                break
            time.sleep(cfg.retry_backoff_s * (2 ** (data_attempts - 1)))
    log.error("retries exhausted, send to DLQ %s: %s", cfg.dlq_topic, last_err)
    DLQ_SENT.inc()
    # 透传 replay_count:若本条是 dlq_replay 回投的消息(带 replay_count header),
    # 失败再进 DLQ 时计数随之累加,达上限后由 dlq_replay 归档,不再无限重投。
    incoming = dict(msg.headers() or [])
    replay_count = (incoming.get("replay_count") or b"0").decode() or "0"
    dlq.produce(
        cfg.dlq_topic,
        key=msg.key(),
        value=msg.value(),
        headers={
            "source_topic": msg.topic(),
            "source_partition": str(msg.partition()),
            "source_offset": str(msg.offset()),
            "error": str(last_err)[:500],
            "replay_count": replay_count,
        },
    )
    dlq.flush(10)


def run() -> None:
    cfg = Config()
    # M2:启动期 schema 校验(改列/删列快速失败)。可用 SCHEMA_CHECK=false 跳过。
    if cfg.schema_check:
        check_schema(cfg.pg_dsn, cfg.tables)
    embedder = make_embedder(cfg)  # EMBED_SERVICE_URL 非空走 HTTP,否则进程内
    sink = make_sink(cfg)
    source_db = SourceDB(cfg.pg_dsn)
    log.info("vector backend: %s", cfg.vector_backend)
    start_metrics(cfg.metrics_port)
    log.info("metrics on :%d/metrics", cfg.metrics_port)
    start_monitor(cfg)
    consumer = Consumer(
        {
            "bootstrap.servers": cfg.kafka_bootstrap,
            "group.id": cfg.kafka_group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            # 瞬时故障无限退避 + 大文本 embedding 都会拉长单条处理时间,
            # 调大避免被踢出 group 触发 rebalance 循环
            "max.poll.interval.ms": 1800000,
        }
    )
    dlq = Producer(
        {"bootstrap.servers": cfg.kafka_bootstrap, "message.max.bytes": 5242880}
    )
    consumer.subscribe([cfg.kafka_topic_pattern])
    log.info(
        "consuming pattern %s from %s, tables=%s",
        cfg.kafka_topic_pattern, cfg.kafka_bootstrap, list(cfg.tables),
    )

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("kafka error: %s", msg.error())
                continue
            if msg.value() is None:
                # tombstone:delete 已由 op=d 事件处理,直接跳过
                consumer.commit(msg)
                continue
            handle_message(msg, cfg, embedder, sink, dlq, source_db)
            consumer.commit(msg)
    finally:
        consumer.close()
        sink.close()
        source_db.close()
        log.info("worker stopped")


if __name__ == "__main__":
    sys.exit(run())
