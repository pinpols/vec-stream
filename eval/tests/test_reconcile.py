"""一致性对账纯函数单测:漂移 diff 算法(精确 set 模式 + 近似计数模式)。"""

from vec_stream_eval.reconcile import reconcile


def test_in_sync_exact():
    source_pks = {("default", "article"): {"1", "2"}}
    indexed_pks = {("default", "article"): {"1", "2"}}
    rep = reconcile(
        source_counts={("default", "article"): 2},
        source_pks=source_pks,
        indexed_pks=indexed_pks,
    )
    assert rep.in_sync
    assert rep.total_missing == 0
    assert rep.total_orphan == 0


def test_missing_exact():
    # 源有 1,2,3,向量只有 1,2 -> missing=1(漏 embed)
    rep = reconcile(
        source_counts={("default", "article"): 3},
        source_pks={("default", "article"): {"1", "2", "3"}},
        indexed_pks={("default", "article"): {"1", "2"}},
    )
    assert not rep.in_sync
    assert rep.total_missing == 1
    assert rep.total_orphan == 0
    assert rep.rows[0].missing == 1


def test_orphan_exact():
    # 向量有 1,2,3,源只有 1,2 -> orphan=1(源删了向量没清)
    rep = reconcile(
        source_counts={("default", "article"): 2},
        source_pks={("default", "article"): {"1", "2"}},
        indexed_pks={("default", "article"): {"1", "2", "3"}},
    )
    assert rep.total_orphan == 1
    assert rep.total_missing == 0


def test_disjoint_sets_both_directions():
    # 源 {1,2} 向量 {2,3} -> missing={1}, orphan={3}
    rep = reconcile(
        source_counts={("default", "article"): 2},
        source_pks={("default", "article"): {"1", "2"}},
        indexed_pks={("default", "article"): {"2", "3"}},
    )
    assert rep.total_missing == 1
    assert rep.total_orphan == 1
    assert rep.rows[0].drift == 2


def test_approx_count_mode():
    # 只有计数(无 set):missing=max(src-idx,0), orphan=max(idx-src,0)
    rep = reconcile(
        source_counts={("default", "article"): 5, ("default", "product"): 2},
        indexed_counts={("default", "article"): 3, ("default", "product"): 4},
    )
    rows = {(r.tenant_id, r.source_table): r for r in rep.rows}
    assert rows[("default", "article")].missing == 2
    assert rows[("default", "article")].orphan == 0
    assert rows[("default", "product")].missing == 0
    assert rows[("default", "product")].orphan == 2
    assert rep.total_missing == 2
    assert rep.total_orphan == 2


def test_group_only_in_indexed():
    # 某 (tenant, table) 只在向量侧出现(源侧 0)-> 全 orphan
    rep = reconcile(
        source_counts={},
        source_pks={},
        indexed_pks={("t2", "comment"): {"7"}},
    )
    assert rep.num_groups == 1
    assert rep.total_orphan == 1
    assert rep.rows[0].source_rows == 0


def test_multi_tenant_table_aggregate():
    rep = reconcile(
        source_counts={("a", "article"): 2, ("b", "product"): 1},
        source_pks={("a", "article"): {"1", "2"}, ("b", "product"): {"9"}},
        indexed_pks={("a", "article"): {"1"}, ("b", "product"): {"9"}},
    )
    assert rep.num_groups == 2
    assert rep.total_missing == 1  # a/article 漏了 2
    assert not rep.in_sync


def test_json_includes_drift():
    rep = reconcile(
        source_counts={("default", "article"): 2},
        source_pks={("default", "article"): {"1", "2"}},
        indexed_pks={("default", "article"): {"1"}},
    )
    assert '"drift"' in rep.to_json()


# ── P1:对账范围必须与 worker「应索引表」同源;权限/RLS 自检 fail loud ──


class _FakeCursor:
    """按 SQL 关键词路由返回值的 fake cursor(驱动 _preflight 单测)。"""

    def __init__(self, privileges=None, bypassrls=True, rls_enabled=True, missing=()):
        self._priv = privileges or {}
        self._bypass, self._rls, self._missing = bypassrls, rls_enabled, missing
        self._last = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        if "rolbypassrls" in sql:
            self._last = (self._bypass, self._rls)
        elif "to_regclass" in sql:
            self._last = (None,) if params[0] in self._missing else (params[0],)
        elif "has_table_privilege" in sql:
            self._last = (self._priv.get(params[0], True),)

    def fetchone(self):
        return self._last


class _FakeConn:
    def __init__(self, cursor):
        self._cur = cursor

    def cursor(self):
        return self._cur


def test_source_tables_excludes_reembed_parent(monkeypatch):
    # comment 是 reembed_parent 子表,自身永不进 doc_vectors——对账把它算进源表
    # 会恒报 missing=N 假漂移
    monkeypatch.delenv("TABLES_JSON", raising=False)
    from vec_stream_eval.reconcile import source_tables

    assert source_tables() == ("article", "product")


def test_source_tables_from_tables_json(monkeypatch):
    import json as _json

    from vec_stream_eval.reconcile import source_tables

    monkeypatch.setenv(
        "TABLES_JSON",
        _json.dumps(
            {
                "doc": {"fields": ["body"], "pk": "id"},
                "note": {"reembed_parent": {"table": "doc", "fk": "doc_id"}},
            }
        ),
    )
    assert source_tables() == ("doc",)


def test_source_tables_rejects_bad_identifier(monkeypatch):
    import pytest

    from vec_stream_eval.reconcile import source_tables

    monkeypatch.setenv("TABLES_JSON", '{"bad;table": {"fields": ["x"], "pk": "id"}}')
    with pytest.raises(ValueError):
        source_tables()


def test_comment_rows_do_not_cause_drift(monkeypatch):
    # 源库有 comment 行,但 comment 不在应索引表 → 不进对账,in_sync=True
    monkeypatch.delenv("TABLES_JSON", raising=False)
    from vec_stream_eval.reconcile import source_tables

    tables = source_tables()
    assert "comment" not in tables
    rep = reconcile(
        source_counts={("t1", t): 1 for t in tables},
        source_pks={("t1", t): {"1"} for t in tables},
        indexed_pks={("t1", t): {"1"} for t in tables},
    )
    assert rep.in_sync


def test_preflight_ok():
    from vec_stream_eval.reconcile import _preflight

    _preflight(_FakeConn(_FakeCursor()), ("article", "product"))  # 不抛即过


def test_preflight_missing_select_privilege_fails_loud():
    import pytest

    from vec_stream_eval.reconcile import _preflight

    conn = _FakeConn(_FakeCursor(privileges={"article": False}))
    with pytest.raises(SystemExit, match="article"):
        _preflight(conn, ("article", "product"))


def test_preflight_rls_without_bypass_fails_loud():
    # 非 BYPASSRLS 角色读 doc_vectors:RLS 静默 0 行 → "全部 missing" 假报告,必须 fail loud
    import pytest

    from vec_stream_eval.reconcile import _preflight

    conn = _FakeConn(_FakeCursor(bypassrls=False, rls_enabled=True))
    with pytest.raises(SystemExit, match="RLS"):
        _preflight(conn, ("article",))


def test_preflight_skips_missing_tables():
    from vec_stream_eval.reconcile import _preflight

    # 表不存在(如可选源表)不算权限问题,由查询侧跳过
    _preflight(_FakeConn(_FakeCursor(missing=("product",))), ("article", "product"))
