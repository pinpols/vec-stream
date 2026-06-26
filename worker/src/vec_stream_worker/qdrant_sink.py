"""Qdrant 后端:与 PgVectorSink 同接口(upsert_row / get_text_hash /
update_metadata / delete_row),靠确定性 uuid5 point id 保持幂等。

处理账本:Qdrant 写入不是 PG 事务,无法与向量写入原子提交。按 ENTERPRISE.md
「Qdrant 后端则记在 PG 里」——写完 Qdrant 后,单独 best-effort upsert
processed_offsets(失败只 warn,不影响主流程,账本仅作审计参考非强一致真相)。"""

import logging

from qdrant_client import QdrantClient, models

from . import ledger
from .ids import qdrant_point_id

log = logging.getLogger("qdrant-sink")


def _row_filter(tenant_id: str, source_table: str, source_pk: str) -> models.Filter:
    return models.Filter(
        must=[
            models.FieldCondition(key="tenant_id", match=models.MatchValue(value=tenant_id)),
            models.FieldCondition(key="source_table", match=models.MatchValue(value=source_table)),
            models.FieldCondition(key="source_pk", match=models.MatchValue(value=source_pk)),
        ]
    )


class QdrantSink:
    def __init__(
        self,
        url: str,
        collection: str,
        dim: int,
        ledger_dsn: str | None = None,
        api_key: str | None = None,
    ):
        self.client = QdrantClient(url=url, api_key=api_key or None)
        self.collection = collection
        # 账本写在 PG 里(Qdrant 无事务):best-effort,无 dsn 则不记账本
        self._ledger_dsn = ledger_dsn
        if not self.client.collection_exists(collection):
            self.client.create_collection(
                collection_name=collection,
                vectors_config=models.VectorParams(size=dim, distance=models.Distance.COSINE),
            )
            # 过滤字段建 payload 索引(先过滤后召回的工程基础)
            for field in ("tenant_id", "source_table", "source_pk", "status"):
                self.client.create_payload_index(
                    collection_name=collection,
                    field_name=field,
                    field_schema=models.PayloadSchemaType.KEYWORD,
                )
            log.info("created qdrant collection %s (dim=%d, cosine)", collection, dim)
        else:
            # 已存在的 collection 也要校验维度:换模型/维度后旧 collection 会静默接受
            # 错维向量、召回错误且无报错。dim 不一致直接 fail-fast,逼蓝绿重建。
            existing = self.client.get_collection(collection).config.params.vectors.size
            if existing != dim:
                raise RuntimeError(
                    f"Qdrant collection {collection} 维度={existing} 与 EMBED_DIM={dim} 不一致;"
                    f"换模型/维度需蓝绿重建新 collection,不能在旧索引上切"
                )

    def _record_offset(self, offset_ref: tuple[str, int, int] | None) -> None:
        """best-effort 把已处理 offset 记进 PG 账本(SQL 共享自 ledger,单调推进)。"""
        ledger.record_best_effort(self._ledger_dsn, offset_ref, log)

    def last_processed_offset(self, topic: str, partition: int) -> int | None:
        """账本里该 (topic, partition) 的最新已处理 offset,供测试/审计查询。
        无 ledger dsn 或查询失败返回 None。"""
        if self._ledger_dsn is None:
            return None
        try:
            import psycopg

            with psycopg.connect(self._ledger_dsn, autocommit=True) as conn:
                row = conn.execute(
                    ledger.READ_SQL,
                    (topic, partition),
                ).fetchone()
            return row[0] if row else None
        except Exception as e:  # noqa: BLE001
            log.warning("ledger read failed (best-effort): %s", e)
            return None

    def get_text_hash(self, tenant_id: str, source_table: str, source_pk: str) -> str | None:
        points, _ = self.client.scroll(
            collection_name=self.collection,
            scroll_filter=_row_filter(tenant_id, source_table, source_pk),
            limit=1,
            with_payload=["text_hash"],
            with_vectors=False,
        )
        return points[0].payload.get("text_hash") if points else None

    def upsert_row(
        self,
        tenant_id: str,
        source_table: str,
        source_pk: str,
        text_hash: str,
        chunks: list[str],
        embeddings: list[list[float]],
        metadata: dict,
        offset_ref: tuple[str, int, int] | None = None,
    ) -> None:
        # 先删该行全部旧 chunk(chunk 数变化不留孤儿),再写新的
        self.client.delete(
            collection_name=self.collection,
            points_selector=models.FilterSelector(
                filter=_row_filter(tenant_id, source_table, source_pk)
            ),
            wait=True,
        )
        points = [
            models.PointStruct(
                id=qdrant_point_id(tenant_id, source_table, source_pk, i),
                vector=emb,
                payload={
                    "tenant_id": tenant_id,
                    "source_table": source_table,
                    "source_pk": source_pk,
                    "chunk_index": i,
                    "text_hash": text_hash,
                    "content": chunk,
                    "status": metadata.get("status"),
                    "title": metadata.get("title"),
                },
            )
            for i, (chunk, emb) in enumerate(zip(chunks, embeddings, strict=False))
        ]
        self.client.upsert(collection_name=self.collection, points=points, wait=True)
        self._record_offset(offset_ref)

    def update_metadata(
        self,
        tenant_id: str,
        source_table: str,
        source_pk: str,
        metadata: dict,
        offset_ref: tuple[str, int, int] | None = None,
    ) -> int:
        # set_payload 对 0 命中是静默 no-op;先 count,无匹配 point 时返回 0,
        # 让调用方回退到全量 upsert,避免"刷新成功"假象掩盖向量缺失。
        flt = _row_filter(tenant_id, source_table, source_pk)
        cnt = self.client.count(self.collection, count_filter=flt, exact=True).count
        if cnt == 0:
            return 0
        self.client.set_payload(
            collection_name=self.collection,
            payload={"status": metadata.get("status"), "title": metadata.get("title")},
            points=flt,
            wait=True,
        )
        self._record_offset(offset_ref)
        return cnt

    def delete_row(
        self,
        tenant_id: str,
        source_table: str,
        source_pk: str,
        offset_ref: tuple[str, int, int] | None = None,
    ) -> int:
        self.client.delete(
            collection_name=self.collection,
            points_selector=models.FilterSelector(
                filter=_row_filter(tenant_id, source_table, source_pk)
            ),
            wait=True,
        )
        self._record_offset(offset_ref)
        return -1  # qdrant delete 不返回条数(-1 = 后端不提供,调用方据此显示)

    def close(self) -> None:
        self.client.close()
