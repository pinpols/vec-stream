from vec_stream_worker.config import Config
from vec_stream_worker.index_metadata import metadata_from_cfg, metadata_name_from_cfg


def test_metadata_from_cfg_records_embedding_and_backend():
    cfg = Config(
        embed_model="model-a",
        embed_dim=768,
        embed_service_url="http://embed:8200",
        embed_provider="local",
        chunk_size=300,
        chunk_overlap=30,
        vector_backend="qdrant",
        qdrant_collection="docs_v2",
    )
    meta = metadata_from_cfg(cfg)
    assert meta["embed_model"] == "model-a"
    assert meta["embed_dim"] == 768
    assert meta["embed_service_url"] == "http://embed:8200"
    assert meta["chunk_size"] == 300
    assert meta["chunk_overlap"] == 30
    assert meta["vector_backend"] == "qdrant"
    assert meta["qdrant_collection"] == "docs_v2"


def test_metadata_name_for_pgvector_and_qdrant():
    assert metadata_name_from_cfg(Config(vector_backend="pgvector")) == "pgvector:doc_vectors"
    assert (
        metadata_name_from_cfg(Config(vector_backend="qdrant", qdrant_collection="docs_v2"))
        == "qdrant:docs_v2"
    )
