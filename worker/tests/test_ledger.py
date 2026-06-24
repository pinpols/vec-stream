"""M1 处理账本测试:
1) process_event 把当前消息坐标 (topic, partition, offset) 作为 offset_ref
   透传给 sink 的每个写入分支(upsert / metadata 刷新 / delete / 源行已删 delete);
2) VectorSink 在同一事务里执行账本 upsert SQL,且带单调推进的 WHERE 守卫。

psycopg 只连真 PG,故用 mock cursor 断言"账本 SQL 被执行 + 单调条件",
不起真实 DB(参考 test_process_event.py 的 mock 风格)。"""
from unittest.mock import MagicMock

from vecstream_worker.config import Config
from vecstream_worker.ids import text_hash
from vecstream_worker.main import process_event
from vecstream_worker.sink import VectorSink


class FakeEmbedder:
    def embed_passages(self, texts):
        return [[0.0] * 4 for _ in texts]


class RecordingSink:
    """记录每个写入调用收到的 offset_ref,验证透传。"""

    def __init__(self, stored_hash=None):
        self.stored_hash = stored_hash
        self.upserts = []
        self.metadata_updates = []
        self.deletes = []

    def get_text_hash(self, tenant_id, source_table, source_pk):
        return self.stored_hash

    def upsert_row(self, **kw):
        self.upserts.append(kw)

    def update_metadata(self, tenant_id, source_table, source_pk, metadata, offset_ref=None):
        self.metadata_updates.append(offset_ref)
        return 1

    def delete_row(self, tenant_id, source_table, source_pk, offset_ref=None):
        self.deletes.append(offset_ref)
        return 2


CFG = Config()
REF = ("cdc.public.article", 3, 4242)


def ev(op, after=None, before=None):
    return {"op": op, "after": after, "before": before}


# ── 1) offset_ref 透传 ───────────────────────────────────────────────

def test_upsert_passes_offset_ref():
    sink = RecordingSink()
    process_event(
        ev("c", after={"id": 1, "tenant_id": "t1", "title": "标题", "body": "正文"}),
        "article", CFG, FakeEmbedder(), sink, offset_ref=REF,
    )
    assert sink.upserts[0]["offset_ref"] == REF


def test_metadata_refresh_passes_offset_ref():
    sink = RecordingSink(stored_hash=text_hash("标题\n正文"))
    process_event(
        ev("u", after={"id": 1, "title": "标题", "body": "正文", "status": "archived"}),
        "article", CFG, FakeEmbedder(), sink, offset_ref=REF,
    )
    assert sink.metadata_updates == [REF]


def test_delete_passes_offset_ref():
    sink = RecordingSink()
    process_event(
        ev("d", before={"id": 7, "tenant_id": "t1"}),
        "article", CFG, FakeEmbedder(), sink, offset_ref=REF,
    )
    assert sink.deletes == [REF]


def test_source_row_gone_delete_passes_offset_ref():
    """op=c/u 但源行已删 → delete_row 分支也要带 offset_ref。"""

    class GoneDB:
        def fetch_row(self, *a, **k):
            return None

    sink = RecordingSink()
    action = process_event(
        ev("u", after={"id": 9, "tenant_id": "t1", "title": "x", "body": "y"}),
        "article", CFG, FakeEmbedder(), sink, source_db=GoneDB(), offset_ref=REF,
    )
    assert action == "deleted"
    assert sink.deletes == [REF]


def test_no_offset_ref_defaults_none():
    """不带 offset_ref 调用(老路径)退化为 None,不报错。"""
    sink = RecordingSink()
    process_event(
        ev("c", after={"id": 1, "tenant_id": "t1", "title": "标题", "body": "正文"}),
        "article", CFG, FakeEmbedder(), sink,
    )
    assert sink.upserts[0]["offset_ref"] is None


# ── 2) VectorSink 同事务账本 SQL ────────────────────────────────────

def _mock_sink():
    """造一个 cursor 被 mock 的 VectorSink,捕获其 execute 调用。"""
    sink = VectorSink("postgresql://ignored")
    cur = MagicMock()
    conn = MagicMock()
    conn.closed = False
    conn.cursor.return_value.__enter__.return_value = cur
    sink._conn = conn
    return sink, conn, cur


def test_vectorsink_upsert_records_ledger_in_same_txn():
    sink, conn, cur = _mock_sink()
    sink.upsert_row(
        tenant_id="t1", source_table="article", source_pk="1", text_hash="h",
        chunks=["c"], embeddings=[[0.0]], metadata={}, offset_ref=REF,
    )
    sqls = " ".join(call.args[0] for call in cur.execute.call_args_list)
    assert "INSERT INTO processed_offsets" in sqls
    # 单调守卫:只在 offset 前进时更新
    assert "EXCLUDED.last_offset > processed_offsets.last_offset" in sqls
    # 账本写在 commit 之前的同一事务
    conn.commit.assert_called_once()
    ledger_call = [c for c in cur.execute.call_args_list
                   if "processed_offsets" in c.args[0]][0]
    assert ledger_call.args[1] == ("cdc.public.article", 3, 4242)


def test_vectorsink_no_offset_ref_skips_ledger():
    sink, conn, cur = _mock_sink()
    sink.delete_row("t1", "article", "1")  # offset_ref=None
    sqls = " ".join(call.args[0] for call in cur.execute.call_args_list)
    assert "processed_offsets" not in sqls


def test_vectorsink_delete_records_ledger():
    sink, conn, cur = _mock_sink()
    sink.delete_row("t1", "article", "1", offset_ref=REF)
    sqls = " ".join(call.args[0] for call in cur.execute.call_args_list)
    assert "INSERT INTO processed_offsets" in sqls


def test_vectorsink_metadata_records_ledger():
    sink, conn, cur = _mock_sink()
    sink.update_metadata("t1", "article", "1", {"status": "x"}, offset_ref=REF)
    sqls = " ".join(call.args[0] for call in cur.execute.call_args_list)
    assert "INSERT INTO processed_offsets" in sqls
