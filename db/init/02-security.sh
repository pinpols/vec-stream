#!/bin/bash
# ============================================================================
# M1 安全加固:最小权限角色 + doc_vectors 行级安全(RLS)+ 处理账本表。
#
# 由 Postgres 官方镜像的 docker-entrypoint 在 01-init.sql 之后自动执行
# (仅 fresh volume 首次初始化时跑)。对**已有库**手动应用:
#   docker exec -e DEBEZIUM_PASSWORD=... -e WORKER_PASSWORD=... -e RAG_PASSWORD=... \
#     vecstream-postgres bash /docker-entrypoint-initdb.d/02-security.sh
# 全部幂等(角色 IF NOT EXISTS / 表 IF NOT EXISTS / 策略 DROP+CREATE),可重复跑。
#
# 设计要点(对应 ENTERPRISE.md 领域一/三 的 M1 项):
#   - vs_debezium:LOGIN+REPLICATION,只 SELECT 源表 —— CDC 不再用超级账号。
#   - vs_worker  :doc_vectors/processed_offsets 全 DML,BYPASSRLS(可信写入端,
#                 一条事件可写任意租户;同时 SELECT 源表做反查)。
#   - vs_rag     :仅 doc_vectors SELECT,**不** BYPASSRLS —— 查询受 RLS 强制,
#                 即使 SQL 漏写 WHERE 也越权不了(机制,非纪律)。
#   - processed_offsets:与向量写入同事务提交的"处理一次"账本(审计闭环)。
# ============================================================================
set -euo pipefail

: "${DEBEZIUM_PASSWORD:?need DEBEZIUM_PASSWORD}"
: "${WORKER_PASSWORD:?need WORKER_PASSWORD}"
: "${RAG_PASSWORD:?need RAG_PASSWORD}"

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER:-vecstream}" --dbname "${POSTGRES_DB:-vecstream}" \
  -v debezium_pw="$DEBEZIUM_PASSWORD" \
  -v worker_pw="$WORKER_PASSWORD" \
  -v rag_pw="$RAG_PASSWORD" <<'EOSQL'
-- ── 1) 最小权限角色(幂等创建,密码在顶层 ALTER 注入)──
DO $do$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'vs_debezium') THEN
    CREATE ROLE vs_debezium WITH LOGIN REPLICATION;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'vs_worker') THEN
    CREATE ROLE vs_worker WITH LOGIN BYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'vs_rag') THEN
    CREATE ROLE vs_rag WITH LOGIN;
  END IF;
END
$do$;

-- 密码:顶层语句,psql :'var' 安全注入(不进 dollar-quoted 块)
ALTER ROLE vs_debezium PASSWORD :'debezium_pw';
ALTER ROLE vs_worker   PASSWORD :'worker_pw';
ALTER ROLE vs_rag      PASSWORD :'rag_pw';

-- ── 2) 处理账本:每 (topic, partition) 的最新已处理 offset,与向量写入同事务提交 ──
CREATE TABLE IF NOT EXISTS processed_offsets (
    topic        TEXT        NOT NULL,
    partition    INT         NOT NULL,
    last_offset  BIGINT      NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition)
);

-- ── 3) doc_vectors 行级安全:按 app.tenant 强制隔离 ──
ALTER TABLE doc_vectors ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON doc_vectors;
-- current_setting(..., true)=missing_ok:未设置 app.tenant 时返回 NULL → 命中 0 行(默认拒绝)
CREATE POLICY tenant_isolation ON doc_vectors
    USING (tenant_id = current_setting('app.tenant', true));

-- ── 4) 授权:各角色最小集 ──
-- schema 访问(PG15+ 默认已给 PUBLIC USAGE,这里显式声明更稳)
GRANT USAGE ON SCHEMA public TO vs_debezium, vs_worker, vs_rag;
-- 源表:debezium 复制读 + worker 反查读
GRANT SELECT ON article, product, comment TO vs_debezium, vs_worker;
-- 向量表:worker 全 DML;rag 只读(受 RLS)
GRANT SELECT, INSERT, UPDATE, DELETE ON doc_vectors TO vs_worker;
GRANT SELECT ON doc_vectors TO vs_rag;
-- 处理账本:仅 worker
GRANT SELECT, INSERT, UPDATE, DELETE ON processed_offsets TO vs_worker;

-- ── 5) CDC 发布:预建 publication(connector 设 publication.autocreate.mode=disabled),
--      这样 vs_debezium 无需 CREATE 权限/超级账号即可复制 ──
DO $do$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'vecstream_pub') THEN
    CREATE PUBLICATION vecstream_pub FOR TABLE article, product, comment;
  END IF;
END
$do$;
ALTER PUBLICATION vecstream_pub OWNER TO vs_debezium;
EOSQL

echo "[02-security] 角色 / RLS / 处理账本 / publication 就绪。"
