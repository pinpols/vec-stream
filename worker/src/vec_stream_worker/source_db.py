"""源库反查(DESIGN.md §3.3d):收到变更后查一次 DB 拉关联数据。
MVP 简化:源库 = 业务 PG;吞吐压力大再评估 Flink CDC(架构已解耦)。"""

import psycopg
from psycopg import sql as pgsql
from psycopg.rows import dict_row


class SourceDB:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self._conn = None

    def _connection(self):
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self._dsn, autocommit=True, row_factory=dict_row)
        return self._conn

    def fetch_row(self, table: str, pk_field: str, pk) -> dict | None:
        """按主键取整行(父文档重建用)。table/pk_field 来自可信配置。"""
        q = pgsql.SQL("SELECT * FROM {} WHERE {} = %s").format(
            pgsql.Identifier(table), pgsql.Identifier(pk_field)
        )
        with self._connection().cursor() as cur:
            cur.execute(q, (pk,))
            return cur.fetchone()

    def query_text(self, sql: str, params: tuple) -> str:
        """执行反查 SQL,把所有行的所有非空列拼成文本(追加进 source_text)。"""
        with self._connection().cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return "\n".join(str(v) for row in rows for v in row.values() if v not in (None, ""))

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
