"""Qdrant 后端:与 PgVectorSink 同接口(upsert_row / get_text_hash /
update_metadata / delete_row),靠确定性 uuid5 point id 保持幂等。"""
import logging

from qdrant_client import QdrantClient, models

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
    def __init__(self, url: str, collection: str, dim: int):
        self.client = QdrantClient(url=url)
        self.collection = collection
        if not self.client.collection_exists(collection):
            self.client.create_collection(
                collection_name=collection,
                vectors_config=models.VectorParams(size=dim, distance=models.Distance.COSINE),
            )
            # 过滤字段建 payload 索引(先过滤后召回的工程基础)
            for field in ("tenant_id", "source_table", "source_pk", "status"):
                self.client.create_payload_index(
                    collection_name=collection, field_name=field,
                    field_schema=models.PayloadSchemaType.KEYWORD,
                )
            log.info("created qdrant collection %s (dim=%d, cosine)", collection, dim)

    def get_text_hash(self, tenant_id: str, source_table: str, source_pk: str) -> str | None:
        points, _ = self.client.scroll(
            collection_name=self.collection,
            scroll_filter=_row_filter(tenant_id, source_table, source_pk),
            limit=1, with_payload=["text_hash"], with_vectors=False,
        )
        return points[0].payload.get("text_hash") if points else None

    def upsert_row(
        self, tenant_id: str, source_table: str, source_pk: str,
        text_hash: str, chunks: list[str], embeddings: list[list[float]], metadata: dict,
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
            for i, (chunk, emb) in enumerate(zip(chunks, embeddings))
        ]
        self.client.upsert(collection_name=self.collection, points=points, wait=True)

    def update_metadata(
        self, tenant_id: str, source_table: str, source_pk: str, metadata: dict
    ) -> int:
        self.client.set_payload(
            collection_name=self.collection,
            payload={"status": metadata.get("status"), "title": metadata.get("title")},
            points=_row_filter(tenant_id, source_table, source_pk),
            wait=True,
        )
        return 1

    def delete_row(self, tenant_id: str, source_table: str, source_pk: str) -> int:
        self.client.delete(
            collection_name=self.collection,
            points_selector=models.FilterSelector(
                filter=_row_filter(tenant_id, source_table, source_pk)
            ),
            wait=True,
        )
        return 1  # qdrant delete 不返回条数

    def close(self) -> None:
        self.client.close()
