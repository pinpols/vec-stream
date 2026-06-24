"""pgvector 写入。按 DESIGN.md §3.3(b):先删该行全部旧 chunk,再写新 chunk,
同一事务内完成,保证 chunk 数量变化时不留孤儿。

连接韧性:任何 SQL 异常后必须 rollback(否则连接停在 aborted 状态,
后续所有操作报 "current transaction is aborted");连接坏死则丢弃,
下次操作懒重建——异常向上抛,交给消费侧的重试逻辑。

make_sink() 按 cfg.vector_backend 选择 pgvector / qdrant 后端,两者同接口。"""

import json

import psycopg

from .ids import vector_id


def make_sink(cfg):
    if cfg.vector_backend == "qdrant":
        from .qdrant_sink import QdrantSink

        # Qdrant 无 PG 事务,账本 best-effort 记进 PG(用 worker 的 pg_dsn)
        return QdrantSink(
            cfg.qdrant_url, cfg.qdrant_collection, cfg.embed_dim, ledger_dsn=cfg.pg_dsn
        )
    return VectorSink(cfg.pg_dsn)


class VectorSink:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self._conn = None

    def _connection(self):
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self._dsn, autocommit=False)
        return self._conn

    def _recover(self) -> None:
        """异常后恢复连接可用状态:能 rollback 则 rollback,不能则丢弃重建。"""
        try:
            if self._conn is not None and not self._conn.closed:
                self._conn.rollback()
        except Exception:  # noqa: BLE001 —— 连接已坏死
            self._conn = None

    @staticmethod
    def _record_offset(cur, offset_ref: tuple[str, int, int] | None) -> None:
        """在向量写入同一事务内 upsert 处理账本(commit 之前)。
        单调推进:WHERE 守卫只在 offset 前进时更新,乱序/重投的旧 offset 不回退。
        offset_ref 为空则 no-op(调用方不带 offset 时退化为纯向量写入)。"""
        if offset_ref is None:
            return
        topic, partition, last_offset = offset_ref
        cur.execute(
            """
            INSERT INTO processed_offsets(topic, partition, last_offset, processed_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (topic, partition) DO UPDATE
              SET last_offset = EXCLUDED.last_offset, processed_at = now()
              WHERE EXCLUDED.last_offset > processed_offsets.last_offset
            """,
            (topic, partition, last_offset),
        )

    def last_processed_offset(self, topic: str, partition: int) -> int | None:
        """账本里该 (topic, partition) 的最新已处理 offset,供测试/审计查询。"""
        try:
            conn = self._connection()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT last_offset FROM processed_offsets " "WHERE topic=%s AND partition=%s",
                    (topic, partition),
                )
                row = cur.fetchone()
            conn.commit()
            return row[0] if row else None
        except Exception:
            self._recover()
            raise

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
        meta_json = json.dumps(metadata, ensure_ascii=False, default=str)
        try:
            conn = self._connection()
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM doc_vectors WHERE tenant_id=%s AND source_table=%s AND source_pk=%s",
                    (tenant_id, source_table, source_pk),
                )
                for i, (chunk, emb) in enumerate(zip(chunks, embeddings, strict=False)):
                    cur.execute(
                        """
                        INSERT INTO doc_vectors
                            (vector_id, tenant_id, source_table, source_pk,
                             chunk_index, text_hash, content, metadata, embedding, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                        ON CONFLICT (vector_id) DO UPDATE SET
                            text_hash = EXCLUDED.text_hash,
                            content = EXCLUDED.content,
                            metadata = EXCLUDED.metadata,
                            embedding = EXCLUDED.embedding,
                            updated_at = now()
                        """,
                        (
                            vector_id(tenant_id, source_table, source_pk, i),
                            tenant_id,
                            source_table,
                            source_pk,
                            i,
                            text_hash,
                            chunk,
                            meta_json,
                            str(emb),
                        ),
                    )
                # 账本与向量写入同事务提交,形成"处理一次"审计真相源
                self._record_offset(cur, offset_ref)
            conn.commit()
        except Exception:
            self._recover()
            raise

    def get_text_hash(self, tenant_id: str, source_table: str, source_pk: str) -> str | None:
        """该行当前存储的 text_hash(任一 chunk 即可,同行所有 chunk 同 hash)。
        用于去重:hash 未变则跳过 embedding 与写入(DESIGN.md §3.3 c)。"""
        try:
            conn = self._connection()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT text_hash FROM doc_vectors "
                    "WHERE tenant_id=%s AND source_table=%s AND source_pk=%s LIMIT 1",
                    (tenant_id, source_table, source_pk),
                )
                row = cur.fetchone()
            conn.commit()
            return row[0] if row else None
        except Exception:
            self._recover()
            raise

    def update_metadata(
        self,
        tenant_id: str,
        source_table: str,
        source_pk: str,
        metadata: dict,
        offset_ref: tuple[str, int, int] | None = None,
    ) -> int:
        """文本未变(hash 命中)但结构化字段可能变了:只刷 metadata,不重 embed。
        否则 status 等过滤字段会停留在旧值,过滤检索出错。"""
        try:
            conn = self._connection()
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE doc_vectors SET metadata=%s, updated_at=now() "
                    "WHERE tenant_id=%s AND source_table=%s AND source_pk=%s",
                    (
                        json.dumps(metadata, ensure_ascii=False, default=str),
                        tenant_id,
                        source_table,
                        source_pk,
                    ),
                )
                updated = cur.rowcount
                self._record_offset(cur, offset_ref)
            conn.commit()
            return updated
        except Exception:
            self._recover()
            raise

    def delete_row(
        self,
        tenant_id: str,
        source_table: str,
        source_pk: str,
        offset_ref: tuple[str, int, int] | None = None,
    ) -> int:
        """删除该行全部 chunk(op=d),返回删除条数。"""
        try:
            conn = self._connection()
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM doc_vectors WHERE tenant_id=%s AND source_table=%s AND source_pk=%s",
                    (tenant_id, source_table, source_pk),
                )
                deleted = cur.rowcount
                self._record_offset(cur, offset_ref)
            conn.commit()
            return deleted
        except Exception:
            self._recover()
            raise

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
