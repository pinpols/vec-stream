import pytest

from vec_stream_rag.index_metadata import expected_metadata, metadata_name, validate_index_metadata


def test_expected_metadata_from_env(monkeypatch):
    monkeypatch.setenv("EMBED_MODEL", "model-b")
    monkeypatch.setenv("EMBED_DIM", "1024")
    monkeypatch.setenv("CHUNK_SIZE", "512")
    monkeypatch.setenv("CHUNK_OVERLAP", "64")
    monkeypatch.setenv("VECTOR_BACKEND", "qdrant")
    monkeypatch.setenv("QDRANT_COLLECTION", "docs_v2")
    meta = expected_metadata()
    assert meta["embed_model"] == "model-b"
    assert meta["embed_dim"] == 1024
    assert meta["chunk_size"] == 512
    assert meta["chunk_overlap"] == 64
    assert meta["vector_backend"] == "qdrant"
    assert meta["qdrant_collection"] == "docs_v2"


def test_validate_index_metadata_passes_when_equal():
    meta = expected_metadata()
    validate_index_metadata(dict(meta), meta)


def test_validate_index_metadata_fails_when_missing():
    with pytest.raises(RuntimeError, match="缺少 pgvector:doc_vectors"):
        validate_index_metadata(None, expected_metadata())


def test_validate_index_metadata_fails_on_mismatch():
    expected = expected_metadata()
    actual = dict(expected)
    actual["embed_dim"] = expected["embed_dim"] + 1
    with pytest.raises(RuntimeError, match="embed_dim"):
        validate_index_metadata(actual, expected)


def test_validate_ignores_non_semantic_routing_fields_for_same_index():
    expected = expected_metadata()
    actual = dict(expected)
    actual["embed_service_url"] = "http://different-host:8200"
    actual["embed_openai_base_url"] = "http://different-host/v1"
    validate_index_metadata(actual, expected)


def test_qdrant_metadata_name_uses_collection(monkeypatch):
    monkeypatch.setenv("VECTOR_BACKEND", "qdrant")
    monkeypatch.setenv("QDRANT_COLLECTION", "docs_v2")
    assert metadata_name(expected_metadata()) == "qdrant:docs_v2"
