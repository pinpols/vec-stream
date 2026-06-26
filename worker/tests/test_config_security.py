import pytest

from vec_stream_worker.config import Config


def test_production_rejects_weak_worker_dsn(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    with pytest.raises(ValueError, match="WORKER_PG_DSN"):
        Config(pg_dsn="postgresql://vs_worker:vs_worker@postgres:5432/vec_stream")


def test_production_qdrant_requires_api_key(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    with pytest.raises(ValueError, match="QDRANT_API_KEY"):
        Config(
            pg_dsn="postgresql://vs_worker:strong@postgres:5432/vec_stream",
            vector_backend="qdrant",
            qdrant_api_key="",
        )


def test_production_accepts_qdrant_with_api_key(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    cfg = Config(
        pg_dsn="postgresql://vs_worker:strong@postgres:5432/vec_stream",
        vector_backend="qdrant",
        qdrant_api_key="secret",
    )
    assert cfg.qdrant_api_key == "secret"


def test_production_openai_embed_requires_egress_allowance(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    with pytest.raises(ValueError, match="EMBED_EGRESS_ALLOWED"):
        Config(
            pg_dsn="postgresql://vs_worker:strong@postgres:5432/vec_stream",
            embed_provider="openai",
            embed_egress_allowed=False,
        )


def test_production_openai_embed_requires_api_key(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with pytest.raises(ValueError, match="OPENAI_API_KEY"):
        Config(
            pg_dsn="postgresql://vs_worker:strong@postgres:5432/vec_stream",
            embed_provider="openai",
            embed_egress_allowed=True,
        )
