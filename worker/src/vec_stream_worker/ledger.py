"""processed_offsets 处理账本:SQL 单一来源,避免两个 sink 漏改其一。

两种写入路径(语义不同,刻意保留):
- pgvector sink:在向量写入**同一事务**内调 `UPSERT_SQL`(原子,失败随事务 rollback);
- Qdrant sink:无事务,用 `record_best_effort` 独立短连接写,失败只 warn(不拖累主写入)。

单调推进:`WHERE EXCLUDED.last_offset > ...` 守卫只在 offset 前进时更新,乱序/重投的旧 offset 不回退。
"""

import psycopg

UPSERT_SQL = """
    INSERT INTO processed_offsets(topic, partition, last_offset, processed_at)
    VALUES (%s, %s, %s, now())
    ON CONFLICT (topic, partition) DO UPDATE
      SET last_offset = EXCLUDED.last_offset, processed_at = now()
      WHERE EXCLUDED.last_offset > processed_offsets.last_offset
"""

READ_SQL = "SELECT last_offset FROM processed_offsets WHERE topic=%s AND partition=%s"


def record_best_effort(dsn: str | None, offset_ref: tuple[str, int, int] | None, log) -> None:
    """独立短连接 upsert 账本,失败只 warn(Qdrant 后端;账本是审计参考,不能拖累主写入)。"""
    if offset_ref is None or dsn is None:
        return
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(UPSERT_SQL, offset_ref)
    except Exception as e:  # noqa: BLE001 —— 账本失败不影响主流程
        log.warning("ledger upsert failed (best-effort, ignored): %s", e)
