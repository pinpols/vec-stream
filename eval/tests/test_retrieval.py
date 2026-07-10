"""检索指标纯函数单测:用构造的召回结果验证 recall@k / MRR 算法正确。"""

from vec_stream_eval.golden import GoldenQuery
from vec_stream_eval.retrieval import (
    evaluate_retrieval,
    recall_at_k,
    reciprocal_rank,
)


def hit(table, pk):
    # 模拟一条 /search 命中(只用到 source_table / source_pk)
    return {"source_table": table, "source_pk": pk, "score": 0.9}


def test_recall_at_k_full_hit():
    retrieved = [hit("article", "1"), hit("article", "2")]
    expected = {("article", "1"), ("article", "2")}
    assert recall_at_k(retrieved, expected, k=5) == 1.0


def test_recall_at_k_partial():
    retrieved = [hit("article", "1"), hit("product", "9")]
    expected = {("article", "1"), ("article", "2")}
    # 命中 1/2
    assert recall_at_k(retrieved, expected, k=5) == 0.5


def test_recall_at_k_cutoff():
    # 期望文档排在第 3 位,但 k=2 截断后看不到 -> recall=0
    retrieved = [hit("x", "1"), hit("x", "2"), hit("article", "1")]
    expected = {("article", "1")}
    assert recall_at_k(retrieved, expected, k=2) == 0.0
    assert recall_at_k(retrieved, expected, k=3) == 1.0


def test_recall_dedup_chunks():
    # 同一期望文档对应多个 chunk,不重复计数,recall 不超过 1
    retrieved = [hit("article", "1"), hit("article", "1"), hit("article", "1")]
    expected = {("article", "1")}
    assert recall_at_k(retrieved, expected, k=5) == 1.0


def test_recall_pk_type_normalized():
    # golden 写数字 1,召回返回字符串 "1",归一后应命中
    retrieved = [hit("article", "1")]
    expected = {("article", "1")}
    assert recall_at_k(retrieved, expected, k=5) == 1.0


def test_recall_empty_expected():
    assert recall_at_k([hit("a", "1")], set(), k=5) == 0.0


def test_reciprocal_rank_first_position():
    retrieved = [hit("article", "1"), hit("x", "2")]
    assert reciprocal_rank(retrieved, {("article", "1")}) == 1.0


def test_reciprocal_rank_third_position():
    retrieved = [hit("x", "1"), hit("y", "2"), hit("article", "1")]
    assert reciprocal_rank(retrieved, {("article", "1")}) == 1.0 / 3.0


def test_reciprocal_rank_no_hit():
    retrieved = [hit("x", "1"), hit("y", "2")]
    assert reciprocal_rank(retrieved, {("article", "1")}) == 0.0


def test_reciprocal_rank_takes_first_of_multiple():
    # 多个期望文档,RR 取第一个命中的名次(第 2 位命中 -> 0.5)
    retrieved = [hit("x", "9"), hit("article", "2"), hit("article", "1")]
    expected = {("article", "1"), ("article", "2")}
    assert reciprocal_rank(retrieved, expected) == 0.5


def test_evaluate_retrieval_aggregate():
    queries = [
        GoldenQuery(
            query="q1",
            tenant_id="default",
            expected_pks=[{"source_table": "article", "source_pk": "1"}],
        ),
        GoldenQuery(
            query="q2",
            tenant_id="default",
            expected_pks=[{"source_table": "article", "source_pk": "2"}],
        ),
    ]

    def fake_search(query, tenant_id, k, rerank):
        if query == "q1":
            # 命中第 1 位:recall=1, rr=1
            return [hit("article", "1")]
        # q2 命中第 2 位:recall=1, rr=0.5
        return [hit("x", "9"), hit("article", "2")]

    rep = evaluate_retrieval(queries, fake_search, k=5)
    assert rep.num_queries == 2
    assert rep.mean_recall_at_k == 1.0
    assert rep.mrr == (1.0 + 0.5) / 2
    # JSON 可序列化
    assert '"mrr"' in rep.to_json()


def test_evaluate_retrieval_miss_lowers_mean():
    queries = [
        GoldenQuery(query="hit", expected_pks=[{"source_table": "a", "source_pk": "1"}]),
        GoldenQuery(query="miss", expected_pks=[{"source_table": "a", "source_pk": "2"}]),
    ]

    def fake_search(query, tenant_id, k, rerank):
        return [hit("a", "1")] if query == "hit" else [hit("z", "99")]

    rep = evaluate_retrieval(queries, fake_search, k=5)
    assert rep.mean_recall_at_k == 0.5
    assert rep.mrr == 0.5


# ── P2:golden 的 tenant_id 不参与请求(tenant 由 API key 决定),报告须标注;
#        可选 per-tenant API key 映射(复用 rag 的 RAG_API_KEYS 形状:key→tenant)──


def test_report_annotates_tenant_source():
    rep = evaluate_retrieval([], lambda *a: [])
    assert "API key" in rep.tenant_note
    assert '"tenant_note"' in rep.to_json()


def test_invert_api_keys():
    from vec_stream_eval.retrieval import invert_api_keys

    # rag 的 RAG_API_KEYS 是 key→tenant;倒排成 tenant→key,首个生效
    assert invert_api_keys('{"k1": "t1", "k2": "t2", "k3": "t1"}') == {"t1": "k1", "t2": "k2"}
    assert invert_api_keys("") == {}


def test_key_for_tenant_prefers_mapping_then_default():
    from vec_stream_eval.retrieval import key_for_tenant

    assert key_for_tenant("t1", "default-key", {"t1": "k1"}) == "k1"
    assert key_for_tenant("t2", "default-key", {"t1": "k1"}) == "default-key"


def test_key_for_tenant_fails_loud_without_any_key():
    import pytest

    from vec_stream_eval.retrieval import key_for_tenant

    with pytest.raises(SystemExit, match="t2"):
        key_for_tenant("t2", None, {"t1": "k1"})
