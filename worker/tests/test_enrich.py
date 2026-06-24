"""跨表反查 + 子表触发父文档重建 + 「以源库当前态为准」语义的测试。"""

import pytest

from vec_stream_worker.config import Config
from vec_stream_worker.ids import text_hash
from vec_stream_worker.main import process_event

from .test_process_event import FakeEmbedder, FakeSink, ev


class FakeSourceDB:
    """rows: {(table, pk): row_dict}"""

    def __init__(self, rows=None, enrich_text=""):
        self.rows = rows or {}
        self.enrich_text = enrich_text
        self.enrich_params = None

    def fetch_row(self, table, pk_field, pk):
        return self.rows.get((table, pk))

    def query_text(self, sql, params):
        self.enrich_params = params
        return self.enrich_text


CFG = Config()
ARTICLE_1 = {
    "id": 1,
    "tenant_id": "default",
    "title": "标题",
    "body": "正文",
    "status": "published",
}


def test_enrich_appends_related_text_and_affects_hash():
    sink = FakeSink()
    db = FakeSourceDB(rows={("article", 1): ARTICLE_1}, enrich_text="评论:讲得很清楚")
    process_event(ev("c", after={"id": 1}), "article", CFG, FakeEmbedder(), sink, db)
    assert sink.upserts[0]["chunks"][0] == "标题\n正文\n评论:讲得很清楚"
    # 关联文本参与 hash:评论变化必须触发重 embed
    assert sink.upserts[0]["text_hash"] == text_hash("标题\n正文\n评论:讲得很清楚")


def test_enrich_query_carries_tenant():
    db = FakeSourceDB(rows={("article", 1): ARTICLE_1}, enrich_text="x")
    process_event(ev("c", after={"id": 1}), "article", CFG, FakeEmbedder(), FakeSink(), db)
    assert db.enrich_params == {"pk": 1, "tenant": "default"}


def test_upsert_uses_current_db_state_not_stale_event():
    """DLQ 重投旧消息 / 乱序事件:落库必须收敛到源库当前态。"""
    current = {**ARTICLE_1, "title": "新标题", "body": "新正文"}
    sink = FakeSink()
    db = FakeSourceDB(rows={("article", 1): current})
    process_event(
        ev("u", after={"id": 1, "title": "旧标题", "body": "旧正文"}),
        "article",
        CFG,
        FakeEmbedder(),
        sink,
        db,
    )
    assert "新标题" in sink.upserts[0]["chunks"][0]
    assert "旧标题" not in sink.upserts[0]["chunks"][0]


def test_upsert_with_row_gone_deletes_vectors():
    """重投/迟到的 upsert 事件,但源行已被删除 → 清理向量而不是复活旧数据。"""
    sink = FakeSink()
    db = FakeSourceDB(rows={})
    action = process_event(
        ev("c", after={"id": 9, "tenant_id": "t1", "title": "x", "body": "y"}),
        "article",
        CFG,
        FakeEmbedder(),
        sink,
        db,
    )
    assert action == "deleted"
    assert sink.deletes == [("t1", "article", "9")]
    assert sink.upserts == []


def test_comment_event_rebuilds_parent_article():
    parent = {
        "id": 5,
        "tenant_id": "default",
        "title": "HNSW",
        "body": "图算法",
        "status": "published",
    }
    sink = FakeSink()
    db = FakeSourceDB(rows={("article", 5): parent}, enrich_text="新评论")
    action = process_event(
        ev("c", after={"id": 100, "article_id": 5, "body": "新评论"}),
        "comment",
        CFG,
        FakeEmbedder(),
        sink,
        db,
    )
    assert action == "upserted"
    assert sink.upserts[0]["source_table"] == "article"
    assert sink.upserts[0]["source_pk"] == "5"
    assert "新评论" in sink.upserts[0]["chunks"][0]


def test_comment_delete_also_rebuilds_parent_via_before():
    parent = {"id": 5, "tenant_id": "default", "title": "HNSW", "body": "图算法"}
    sink = FakeSink()
    db = FakeSourceDB(rows={("article", 5): parent}, enrich_text="")
    action = process_event(
        ev("d", before={"id": 100, "article_id": 5, "body": "被删评论"}),
        "comment",
        CFG,
        FakeEmbedder(),
        sink,
        db,
    )
    assert action == "upserted"
    assert sink.upserts[0]["source_pk"] == "5"
    assert "被删评论" not in sink.upserts[0]["chunks"][0]


def test_comment_with_missing_parent_ignored():
    action = process_event(
        ev("c", after={"id": 100, "article_id": 999, "body": "孤儿评论"}),
        "comment",
        CFG,
        FakeEmbedder(),
        FakeSink(),
        FakeSourceDB(),
    )
    assert action == "ignored"


def test_comment_without_source_db_ignored():
    action = process_event(
        ev("c", after={"id": 100, "article_id": 5, "body": "x"}),
        "comment",
        CFG,
        FakeEmbedder(),
        FakeSink(),
        None,
    )
    assert action == "ignored"


def test_oversize_text_truncated():
    big = {"id": 1, "tenant_id": "default", "title": "T", "body": "x" * 10000}
    sink = FakeSink()
    db = FakeSourceDB(rows={("article", 1): big})
    cfg = Config(max_doc_chars=500)
    process_event(ev("c", after={"id": 1}), "article", cfg, FakeEmbedder(), sink, db)
    total = sum(len(c) for c in sink.upserts[0]["chunks"])
    # 截断后(500)+ overlap 冗余,远小于原文 10000+
    assert total < 1000


def test_config_validation():
    with pytest.raises(ValueError):
        Config(chunk_size=100, chunk_overlap=100)
    with pytest.raises(ValueError):
        Config(vector_backend="milvus")
