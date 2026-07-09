"""processed_offsets 处理账本:SQL 单一来源,避免两个 sink 漏改其一。

两种写入路径(语义不同,刻意保留):
- pgvector sink:在向量写入**同一事务**内调 `UPSERT_SQL`(原子,失败随事务 rollback);
- Qdrant sink:无事务,用 `BestEffortLedger` 常驻连接写,失败只 warn(不拖累主写入)。

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


class BestEffortLedger:
    """best-effort 账本写入器,持有懒重建的常驻 PG 连接(照 VectorSink 的连接韧性模式)。

    此前每条事件 psycopg.connect 一次,高吞吐下连接建立(TCP/auth 往返)是
    Qdrant 后端的隐性开销。现在:连接懒创建、跨事件复用;写失败丢弃连接
    重连再试一次(断连自愈),仍失败只 warn——账本是审计参考,不拖累主写入。"""

    def __init__(self, dsn: str | None):
        self._dsn = dsn
        self._conn = None

    def _connection(self):
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self._dsn, autocommit=True)
        return self._conn

    def _discard(self) -> None:
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception:  # noqa: BLE001 —— 连接已坏死,丢弃即可
            pass
        self._conn = None

    def record(self, offset_ref: tuple[str, int, int] | None, log) -> None:
        """upsert 账本;失败重连重试一次,仍失败只 warn(不抛,不影响主流程)。"""
        if offset_ref is None or self._dsn is None:
            return
        last_err: Exception | None = None
        for _attempt in range(2):
            try:
                self._connection().execute(UPSERT_SQL, offset_ref)
                return
            except Exception as e:  # noqa: BLE001 —— 账本失败不影响主流程
                last_err = e
                self._discard()  # 丢弃坏连接,下一轮/下一事件懒重建
        log.warning("ledger upsert failed (best-effort, ignored): %s", last_err)

    def close(self) -> None:
        self._discard()
