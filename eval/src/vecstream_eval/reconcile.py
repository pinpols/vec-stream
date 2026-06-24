"""一致性对账:doc_vectors 已索引的 (tenant, table, source_pk) 数 vs 源表行数。

差异 = 漂移:漏处理 / 反查竞态兜底监控。
  missing  = 源表有、向量库没有(漏 embed,正向漂移,通常该报警)
  orphan   = 向量库有、源表没有(源行删了但向量没清,负向漂移)

源表只有 article/product/comment 是 CDC 监听对象;doc_vectors.source_table
取值即这些表名。DSN 从 env(RAG_PG_DSN / PG_DSN)。
"""
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass

# doc_vectors.source_table -> 源表名(此处一致;留映射点以防将来改名)
SOURCE_TABLES = ("article", "product", "comment")


@dataclass
class DriftRow:
    tenant_id: str
    source_table: str
    source_rows: int       # 源表该 (tenant, table) 行数
    indexed_pks: int       # doc_vectors 中 distinct source_pk 数
    missing: int           # 源有向量无(漏处理)
    orphan: int            # 向量有源无(残留)

    @property
    def drift(self) -> int:
        return self.missing + self.orphan


@dataclass
class ReconcileReport:
    num_groups: int
    total_missing: int
    total_orphan: int
    in_sync: bool
    rows: list[DriftRow]

    def to_json(self) -> str:
        d = asdict(self)
        # @property 不进 asdict,补回 drift 便于消费方
        for r, src in zip(d["rows"], self.rows):
            r["drift"] = src.drift
        return json.dumps(d, ensure_ascii=False, indent=2)

    def to_table(self) -> str:
        status = "IN SYNC ✓" if self.in_sync else "DRIFT ✗"
        lines = [
            f"一致性对账  {status}  groups={self.num_groups}  "
            f"missing={self.total_missing}  orphan={self.total_orphan}",
            "",
            f"{'tenant':>10}  {'table':>10}  {'src':>6}  {'idx':>6}  "
            f"{'missing':>7}  {'orphan':>6}",
        ]
        for r in self.rows:
            lines.append(
                f"{r.tenant_id:>10}  {r.source_table:>10}  {r.source_rows:>6}  "
                f"{r.indexed_pks:>6}  {r.missing:>7}  {r.orphan:>6}"
            )
        return "\n".join(lines)


def reconcile(
    source_counts: dict[tuple[str, str], int],
    indexed_pks: dict[tuple[str, str], set[str]] | None = None,
    indexed_counts: dict[tuple[str, str], int] | None = None,
    source_pks: dict[tuple[str, str], set[str]] | None = None,
) -> ReconcileReport:
    """纯计算核心。键统一为 (tenant_id, source_table)。

    两种精度:
      - 精确(传 source_pks + indexed_pks 两个 set):missing/orphan 按集合差精确算。
      - 近似(只有 source_counts + indexed_counts 两个计数):
          missing = max(src - idx, 0),orphan = max(idx - src, 0)。

    覆盖两侧出现的全部 (tenant, table) 组合(任一侧缺失按 0 计)。
    """
    keys: set[tuple[str, str]] = set(source_counts)
    if indexed_pks:
        keys |= set(indexed_pks)
    if indexed_counts:
        keys |= set(indexed_counts)

    rows: list[DriftRow] = []
    for key in sorted(keys):
        tenant, table = key
        src_n = source_counts.get(key, 0)

        if source_pks is not None and indexed_pks is not None:
            src_set = source_pks.get(key, set())
            idx_set = indexed_pks.get(key, set())
            idx_n = len(idx_set)
            missing = len(src_set - idx_set)
            orphan = len(idx_set - src_set)
            # source_counts 可能与 source_pks 大小不同,以 set 为准更准
            src_n = len(src_set)
        else:
            if indexed_pks is not None:
                idx_n = len(indexed_pks.get(key, set()))
            else:
                idx_n = (indexed_counts or {}).get(key, 0)
            missing = max(src_n - idx_n, 0)
            orphan = max(idx_n - src_n, 0)

        rows.append(
            DriftRow(
                tenant_id=tenant,
                source_table=table,
                source_rows=src_n,
                indexed_pks=idx_n,
                missing=missing,
                orphan=orphan,
            )
        )

    total_missing = sum(r.missing for r in rows)
    total_orphan = sum(r.orphan for r in rows)
    return ReconcileReport(
        num_groups=len(rows),
        total_missing=total_missing,
        total_orphan=total_orphan,
        in_sync=(total_missing == 0 and total_orphan == 0),
        rows=rows,
    )


def _query_db(dsn: str):
    """连 PG(只读),拉源表与 doc_vectors 的 (tenant, table) -> set(source_pk)。"""
    import psycopg

    indexed_pks: dict[tuple[str, str], set[str]] = {}
    source_pks: dict[tuple[str, str], set[str]] = {}
    source_counts: dict[tuple[str, str], int] = {}

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        # 向量侧:每个 (tenant, table) 已索引的 distinct source_pk 集合
        cur.execute(
            "SELECT tenant_id, source_table, source_pk "
            "FROM doc_vectors GROUP BY tenant_id, source_table, source_pk"
        )
        for tenant, table, pk in cur.fetchall():
            indexed_pks.setdefault((tenant, table), set()).add(str(pk))

        # 源侧:逐源表拉 (tenant_id, id)
        for table in SOURCE_TABLES:
            try:
                cur.execute(f"SELECT tenant_id, id FROM {table}")  # noqa: S608 (固定白名单)
            except psycopg.errors.UndefinedTable:
                continue
            for tenant, pk in cur.fetchall():
                key = (tenant, table)
                source_pks.setdefault(key, set()).add(str(pk))
            for key, s in source_pks.items():
                if key[1] == table:
                    source_counts[key] = len(s)

    return source_counts, source_pks, indexed_pks


def run(dsn: str | None = None, json_out: str | None = None) -> ReconcileReport:
    """CLI 入口:从 env DSN 连 PG,精确对账,打印漂移报告。"""
    dsn = dsn or os.getenv("RAG_PG_DSN") or os.getenv("PG_DSN")
    if not dsn:
        raise SystemExit("缺少 DSN:设置 RAG_PG_DSN 或 PG_DSN(或 --dsn)")
    source_counts, source_pks, indexed_pks = _query_db(dsn)
    report = reconcile(
        source_counts=source_counts,
        indexed_pks=indexed_pks,
        source_pks=source_pks,
    )
    print(report.to_table())
    if json_out:
        with open(json_out, "w", encoding="utf-8") as f:
            f.write(report.to_json())
        print(f"\nJSON 已写入 {json_out}")
    return report
