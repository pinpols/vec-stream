"""启动期 schema 校验(M2 领域七:轻量 DDL 防御)。

上游改列/删列时,配置里写死的字段会**静默用错数据**(after 里没这个 key → 拼出空文本
或漏字段)。启动时查 information_schema.columns 比对配置声明的字段确实存在,缺失则
**快速失败**而非带病运行。改动小,挡掉一类隐蔽数据错误。

只读校验;reembed_parent 子表自身不进向量库、无 fields,跳过其字段检查(只验外键列)。
"""

import logging

import psycopg

log = logging.getLogger("schema-check")


def _existing_columns(conn, table: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = %s",
            (table,),
        )
        return {r[0] for r in cur.fetchall()}


def check_schema(dsn: str, tables: dict) -> None:
    """校验每张配置表的 pk / fields / title_field(及子表外键)都存在。

    抛 RuntimeError 列出全部缺失项(一次性报全,而非逐个 trial-and-error)。
    """
    problems: list[str] = []
    with psycopg.connect(dsn) as conn:
        for table, tcfg in tables.items():
            parent = tcfg.get("reembed_parent")
            if parent:
                # 子表:只需外键列存在(变更触发父行重建)
                cols = _existing_columns(conn, table)
                if not cols:
                    problems.append(f"子表 {table} 不存在")
                elif parent.get("fk") and parent["fk"] not in cols:
                    problems.append(f"{table}.{parent['fk']}(reembed_parent.fk)缺失")
                continue

            cols = _existing_columns(conn, table)
            if not cols:
                problems.append(f"表 {table} 不存在(配置声明了它)")
                continue
            required = set(tcfg.get("fields", []))
            pk = tcfg.get("pk", "id")
            required.add(pk)
            title = tcfg.get("title_field")
            if title:
                required.add(title)
            missing = sorted(required - cols)
            if missing:
                problems.append(f"表 {table} 缺字段 {missing}(已有 {sorted(cols)})")

    if problems:
        raise RuntimeError(
            "schema 校验失败,配置与源表不一致(改列/删列?):\n  - " + "\n  - ".join(problems)
        )
    log.info("schema 校验通过:%s", list(tables))
