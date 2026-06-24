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
