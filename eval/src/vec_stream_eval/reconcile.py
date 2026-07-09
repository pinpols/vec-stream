"""一致性对账:doc_vectors 已索引的 (tenant, table, source_pk) 数 vs 源表行数。

差异 = 漂移:漏处理 / 反查竞态兜底监控。
  missing  = 源表有、向量库没有(漏 embed,正向漂移,通常该报警)
  orphan   = 向量库有、源表没有(源行删了但向量没清,负向漂移)

对账范围 = 「应索引表」:与 worker/config.py 的 TABLES 语义同源——reembed_parent
子表(如 comment)自身永不进 doc_vectors(变更只触发父表重 embed),把它算进
源表会恒报 missing=N 假漂移,必须排除。支持与 worker 相同的 TABLES_JSON 环境
变量整体覆盖(两边共用同一配置源);未设置时回退到 worker DEFAULT_TABLES 的
镜像清单 ("article", "product")。DSN 从 env(RAG_PG_DSN / PG_DSN)。
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass

# 回退清单:worker/config.py DEFAULT_TABLES 中非 reembed_parent 的表
# (article/product;comment 是 reembed_parent 子表,不进向量库)
_DEFAULT_INDEXABLE = ("article", "product")

# 表名要拼进 SQL(psycopg 参数化不支持标识符),白名单校验防注入/误配
_IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]*$", re.IGNORECASE)


def source_tables() -> tuple[str, ...]:
    """派生应对账的源表清单(排除 reembed_parent 子表)。

    优先读 worker 同款 TABLES_JSON(配置同源,worker 加表/换表时对账自动跟随);
    未设置时回退 _DEFAULT_INDEXABLE。
    """
    raw = os.getenv("TABLES_JSON", "")
    if not raw:
        return _DEFAULT_INDEXABLE
    tables = json.loads(raw)
    names = tuple(sorted(n for n, c in tables.items() if not c.get("reembed_parent")))
    for n in names:
        if not _IDENT_RE.match(n):
            raise ValueError(f"TABLES_JSON 表名非法(只允许字母/数字/下划线):{n!r}")
    return names


@dataclass
class DriftRow:
    tenant_id: str
    source_table: str
    source_rows: int  # 源表该 (tenant, table) 行数
    indexed_pks: int  # doc_vectors 中 distinct source_pk 数
    missing: int  # 源有向量无(漏处理)
    orphan: int  # 向量有源无(残留)

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
        for r, src in zip(d["rows"], self.rows, strict=False):
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


def _preflight(conn, tables: tuple[str, ...]) -> None:
    """启动自检:权限 / RLS 问题明确报错,不静默产出假报告。

    两类陷阱(随箱 compose 曾用 vs_rag 跑对账,两个都踩):
      - 源表 SELECT 权限不足 → 崩在查询半路;这里一次性报全,并提示用 vs_eval
        角色(db/init/02-security.sh;已有库需手动重放该脚本才有该角色)。
      - 非 BYPASSRLS 角色读 doc_vectors:未 set app.tenant 时 RLS 静默返 0 行 →
        对账输出「全部 missing」的假报告,比崩溃更危险,必须 fail loud。
    """
    with conn.cursor() as cur:
        denied: list[str] = []
        for t in (*tables, "doc_vectors"):
            cur.execute("SELECT to_regclass('public.' || %s)", (t,))
            if cur.fetchone()[0] is None:
                continue  # 表不存在由查询侧跳过,不算权限问题
            cur.execute("SELECT has_table_privilege(current_user, %s, 'SELECT')", (t,))
            if not cur.fetchone()[0]:
                denied.append(t)
        if denied:
            raise SystemExit(
                f"当前角色对表 {denied} 无 SELECT 权限,对账无法进行。"
                "请用 vs_eval 角色连接(EVAL_PG_DSN;角色由 db/init/02-security.sh 创建,"
                "已有库需手动重放该脚本)。"
            )
        cur.execute(
            "SELECT r.rolbypassrls, c.relrowsecurity FROM pg_roles r, pg_class c "
            "WHERE r.rolname = current_user AND c.oid = to_regclass('public.doc_vectors')"
        )
        row = cur.fetchone()
        if row is not None and row[1] and not row[0]:
            raise SystemExit(
                "doc_vectors 启用了 RLS 且当前角色无 BYPASSRLS:未 set app.tenant 时"
                "查询静默返 0 行,对账会产出「全部 missing」的假报告。"
                "请用 vs_eval(BYPASSRLS,运维态只读工具)连接,见 db/init/02-security.sh。"
            )


def _query_db(dsn: str):
    """连 PG(只读),拉源表与 doc_vectors 的 (tenant, table) -> set(source_pk)。"""
    import psycopg

    tables = source_tables()
    indexed_pks: dict[tuple[str, str], set[str]] = {}
    source_pks: dict[tuple[str, str], set[str]] = {}
    source_counts: dict[tuple[str, str], int] = {}

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        _preflight(conn, tables)
        # 向量侧:每个 (tenant, table) 已索引的 distinct source_pk 集合
        cur.execute(
            "SELECT tenant_id, source_table, source_pk "
            "FROM doc_vectors GROUP BY tenant_id, source_table, source_pk"
        )
        for tenant, table, pk in cur.fetchall():
            indexed_pks.setdefault((tenant, table), set()).add(str(pk))

        # 源侧:逐应索引表拉 (tenant_id, id)(表名过 _IDENT_RE 白名单校验)
        for table in tables:
            try:
                cur.execute(f"SELECT tenant_id, id FROM {table}")  # noqa: S608
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
