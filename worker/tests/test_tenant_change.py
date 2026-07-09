"""P2-3:tenant_id 变更 → 旧租户孤儿向量兜底清理。

vector_id 含 tenant_id,UPDATE 改 tenant_id 后新向量写在新租户下,
旧租户下的向量无人认领(delete/upsert 都按新租户过滤不到)。
兜底:upsert 路径发现事件 before.tenant_id 与即将写入的租户不同时,
先按旧租户删一次该行向量(RI FULL 保证 before 完整;删除幂等,
重放旧事件误删不存在的行无副作用)。事件丢失场景仍需离线对账(见 DESIGN.md)。
"""

from vec_stream_worker.config import Config
from vec_stream_worker.main import process_event

from .test_process_event import FakeEmbedder, FakeSink, ev

CFG = Config()


def test_tenant_change_deletes_old_tenant_vectors():
    sink = FakeSink()
    action = process_event(
        ev(
            "u",
            after={"id": 1, "tenant_id": "t2", "title": "标题", "body": "正文"},
            before={"id": 1, "tenant_id": "t1", "title": "标题", "body": "正文"},
        ),
        "article",
        CFG,
        FakeEmbedder(),
        sink,
    )
    assert action == "upserted"
    assert ("t1", "article", "1") in sink.deletes  # 旧租户孤儿被清
    assert sink.upserts[0]["tenant_id"] == "t2"  # 新向量写在新租户下


def test_tenant_change_with_source_db_uses_current_tenant():
    """反查源库后以当前租户为准写入,旧租户(before)仍要清。"""

    class DB:
        def fetch_row(self, table, pk_field, pk):
            return {"id": 1, "tenant_id": "t2", "title": "标题", "body": "正文"}

        def query_text(self, sql, params):
            return None

    sink = FakeSink()
    process_event(
        ev(
            "u",
            after={"id": 1, "tenant_id": "t2", "title": "标题", "body": "正文"},
            before={"id": 1, "tenant_id": "t1", "title": "标题", "body": "正文"},
        ),
        "article",
        CFG,
        FakeEmbedder(),
        sink,
        source_db=DB(),
    )
    assert ("t1", "article", "1") in sink.deletes
    assert sink.upserts[0]["tenant_id"] == "t2"


def test_tenant_unchanged_no_extra_delete():
    sink = FakeSink()
    process_event(
        ev(
            "u",
            after={"id": 1, "tenant_id": "t1", "title": "标题", "body": "正文"},
            before={"id": 1, "tenant_id": "t1", "title": "旧标题", "body": "旧正文"},
        ),
        "article",
        CFG,
        FakeEmbedder(),
        sink,
    )
    assert sink.deletes == []


def test_tenant_change_source_row_gone_cleans_both_tenants():
    """源行已删 + 事件带租户变更:两个租户下的向量都要清。"""

    class GoneDB:
        def fetch_row(self, *a, **k):
            return None

    sink = FakeSink()
    action = process_event(
        ev(
            "u",
            after={"id": 1, "tenant_id": "t2", "title": "x", "body": "y"},
            before={"id": 1, "tenant_id": "t1", "title": "x", "body": "y"},
        ),
        "article",
        CFG,
        FakeEmbedder(),
        sink,
        source_db=GoneDB(),
    )
    assert action == "deleted"
    assert ("t1", "article", "1") in sink.deletes
    assert ("t2", "article", "1") in sink.deletes
