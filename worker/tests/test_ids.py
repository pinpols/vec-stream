from vecstream_worker.ids import text_hash, vector_id


def test_vector_id_deterministic():
    a = vector_id("t1", "article", "42", 0)
    b = vector_id("t1", "article", "42", 0)
    assert a == b
    assert len(a) == 64


def test_vector_id_varies_by_each_component():
    base = vector_id("t1", "article", "42", 0)
    assert vector_id("t2", "article", "42", 0) != base
    assert vector_id("t1", "other", "42", 0) != base
    assert vector_id("t1", "article", "43", 0) != base
    assert vector_id("t1", "article", "42", 1) != base


def test_text_hash_changes_with_content():
    assert text_hash("abc") != text_hash("abd")
    assert text_hash("中文内容") == text_hash("中文内容")


def test_qdrant_point_id_deterministic_and_uuid():
    import uuid

    from vecstream_worker.ids import qdrant_point_id

    a = qdrant_point_id("t1", "article", "42", 0)
    assert a == qdrant_point_id("t1", "article", "42", 0)
    assert a != qdrant_point_id("t1", "article", "42", 1)
    uuid.UUID(a)  # 必须是合法 UUID(Qdrant 强制)
