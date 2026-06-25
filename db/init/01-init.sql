-- vec_stream Postgres 初始化
-- MVP 简化:同一个 PG 既当业务源库(被 CDC 监听),又当向量库(pgvector)。
-- 生产应拆分,这里为了阶段 0 快速跑通合并。

-- 0) 向量扩展
CREATE EXTENSION IF NOT EXISTS vector;

-- 注:最小权限角色(debezium/worker/rag)、行级安全(RLS)、处理账本、授权、
-- CDC publication 全部在 02-security.sh —— 它能从环境变量安全注入密码
-- (psql -v 注入,SQL 无法直接读 bash 变量),本文件只负责 schema 与样例数据。

-- 2) 业务源表示例:article(会被 Debezium 监听)
CREATE TABLE IF NOT EXISTS article (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   TEXT        NOT NULL DEFAULT 'default',
    title       TEXT        NOT NULL,
    body        TEXT        NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'published',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_article_tenant_id ON article (tenant_id, id);

-- Debezium 逻辑复制需要:对 UPDATE/DELETE 输出完整 before 镜像
ALTER TABLE article REPLICA IDENTITY FULL;

-- 第二张业务源表:product(多表同步示例)
CREATE TABLE IF NOT EXISTS product (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   TEXT        NOT NULL DEFAULT 'default',
    name        TEXT        NOT NULL,
    description TEXT        NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'published',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE product REPLICA IDENTITY FULL;

-- 子表:comment(跨表反查示例 —— comment 变更触发所属 article 重新 embed)
CREATE TABLE IF NOT EXISTS comment (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   TEXT        NOT NULL DEFAULT 'default',
    article_id  BIGINT      NOT NULL,
    body        TEXT        NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_comment_article_tenant'
    ) THEN
        ALTER TABLE comment
            ADD CONSTRAINT fk_comment_article_tenant
            FOREIGN KEY (tenant_id, article_id)
            REFERENCES article (tenant_id, id)
            ON DELETE CASCADE;
    END IF;
END
$$;
ALTER TABLE comment REPLICA IDENTITY FULL;

-- 3) 向量表:存 chunk 向量(512 维,对应 BAAI/bge-small-zh-v1.5)
CREATE TABLE IF NOT EXISTS doc_vectors (
    vector_id    TEXT PRIMARY KEY,          -- 确定性 ID: sha256(tenant:table:pk:chunk_index)
    tenant_id    TEXT        NOT NULL,
    source_table TEXT        NOT NULL,
    source_pk    TEXT        NOT NULL,
    chunk_index  INT         NOT NULL,
    text_hash    TEXT        NOT NULL,       -- 源文本 hash,用于去重跳过
    content      TEXT        NOT NULL,       -- 原文 chunk(召回展示用)
    metadata     JSONB,                      -- 业务过滤字段
    embedding    vector(512),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_doc_vectors_hnsw
    ON doc_vectors USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS idx_doc_vectors_source
    ON doc_vectors (tenant_id, source_table, source_pk);

-- 3b) 索引元数据:worker 写入当前 embedding/chunk/backend 配置;
--     rag 启动时读取并校验,避免换模型/维度后悄悄用错检索向量。
CREATE TABLE IF NOT EXISTS index_metadata (
    name       TEXT PRIMARY KEY,
    metadata   JSONB       NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 4) 几条示例数据
INSERT INTO article (tenant_id, title, body) VALUES
    ('default', 'pgvector 入门', 'pgvector 是 PostgreSQL 的向量检索扩展,支持 HNSW 与 IVFFlat 索引。'),
    ('default', 'Debezium 是什么', 'Debezium 基于数据库 redo/WAL 日志做 CDC,捕获行级变更并发往 Kafka。')
ON CONFLICT DO NOTHING;

-- 角色 / 授权 / RLS / 处理账本 / publication —— 见 02-security.sh(在本文件之后执行)。
