"""process_event 分流逻辑测试:c/r/u/d、hash 去重 + metadata 刷新、多表路由。"""
from vecstream_worker.config import Config
from vecstream_worker.ids import text_hash
from vecstream_worker.main import process_event, table_from_topic


class FakeEmbedder:
    def embed_passages(self, texts):
        return [[0.0] * 4 for _ in texts]


class FakeSink:
    def __init__(self, stored_hash=None):
        self.stored_hash = stored_hash
        self.upserts = []
        self.deletes = []
        self.metadata_updates = []

    def get_text_hash(self, tenant_id, source_table, source_pk):
        return self.stored_hash

    def upsert_row(self, **kw):
        self.upserts.append(kw)

    def update_metadata(self, tenant_id, source_table, source_pk, metadata, offset_ref=None):
        self.metadata_updates.append((tenant_id, source_table, source_pk, metadata))
        return 1

    def delete_row(self, tenant_id, source_table, source_pk, offset_ref=None):
        self.deletes.append((tenant_id, source_table, source_pk))
        return 2


CFG = Config()


def ev(op, after=None, before=None):
    return {"op": op, "after": after, "before": before}


def test_table_from_topic():
    assert table_from_topic("cdc.public.article") == "article"
    assert table_from_topic("cdc.public.product") == "product"


def test_insert_upserts():
    sink = FakeSink()
    action = process_event(
        ev("c", after={"id": 1, "tenant_id": "t1", "title": "标题", "body": "正文"}),
        "article", CFG, FakeEmbedder(), sink,
    )
    assert action == "upserted"
    assert sink.upserts[0]["source_pk"] == "1"
    assert sink.upserts[0]["tenant_id"] == "t1"
    assert sink.upserts[0]["metadata"]["title"] == "标题"


def test_product_table_uses_its_own_fields():
    sink = FakeSink()
    action = process_event(
        ev("c", after={"id": 9, "name": "商品A", "description": "描述文本"}),
        "product", CFG, FakeEmbedder(), sink,
    )
    assert action == "upserted"
    assert sink.upserts[0]["source_table"] == "product"
    assert sink.upserts[0]["metadata"]["title"] == "商品A"


def test_unconfigured_table_ignored():
    sink = FakeSink()
    action = process_event(
        ev("c", after={"id": 1, "title": "x", "body": "y"}),
        "unknown_table", CFG, FakeEmbedder(), sink,
    )
    assert action == "ignored"
    assert sink.upserts == []


def test_update_with_same_hash_refreshes_metadata_only():
    same = text_hash("标题\n正文")
    sink = FakeSink(stored_hash=same)
    action = process_event(
        ev("u", after={"id": 1, "title": "标题", "body": "正文", "status": "archived"}),
        "article", CFG, FakeEmbedder(), sink,
    )
    assert action == "metadata_refreshed"
    assert sink.upserts == []
    # metadata(如 status)必须刷新,否则过滤检索用旧值
    assert sink.metadata_updates[0][3]["status"] == "archived"


def test_update_with_changed_text_reembeds():
    sink = FakeSink(stored_hash=text_hash("旧文本"))
    action = process_event(
        ev("u", after={"id": 1, "title": "标题", "body": "新正文"}),
        "article", CFG, FakeEmbedder(), sink,
    )
    assert action == "upserted"
    assert len(sink.upserts) == 1


def test_delete_removes_row_chunks():
    sink = FakeSink()
    action = process_event(
        ev("d", before={"id": 7, "tenant_id": "t1"}), "article", CFG, FakeEmbedder(), sink
    )
    assert action == "deleted"
    assert sink.deletes == [("t1", "article", "7")]


def test_delete_without_before_ignored():
    sink = FakeSink()
    assert process_event(ev("d"), "article", CFG, FakeEmbedder(), sink) == "ignored"
    assert sink.deletes == []


def test_unknown_op_ignored():
    assert process_event(ev("x"), "article", CFG, FakeEmbedder(), FakeSink()) == "ignored"


def test_upsert_without_pk_ignored():
    assert process_event(
        ev("c", after={"title": "无主键"}), "article", CFG, FakeEmbedder(), FakeSink()
    ) == "ignored"
