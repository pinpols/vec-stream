#!/usr/bin/env bash
# Iceberg lakehouse sink 冒烟(统一 cdc.public.* JSON → spark-lake iceberg → Iceberg 表)。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"; cd "$ROOT"
PG_CONTAINER="${PG_CONTAINER:-vec-stream-postgres}"
POSTGRES_USER="${POSTGRES_USER:-vec_stream}"; POSTGRES_DB="${POSTGRES_DB:-vec_stream}"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.lake.yml)

SMOKE_PK=""
cleanup() { [ -n "${SMOKE_PK:-}" ] && docker exec -i "$PG_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAqc "DELETE FROM article WHERE id=$SMOKE_PK;" >/dev/null 2>&1 || true; }
trap cleanup EXIT

write() { "${COMPOSE[@]}" run --rm spark-lake iceberg article >/dev/null 2>&1; }
status() { "${COMPOSE[@]}" run --rm spark-lake query-iceberg article "$1" 2>/dev/null | grep -oE 'RESULT=[^[:space:]]+' | tail -1 | cut -d= -f2; }
assert() { local got; got="$(status "$1")"; if [ "$got" = "$2" ]; then echo "  id=$1 -> $got ✓"; else echo "  id=$1 -> '$got'(期望 '$2')✗" >&2; exit 1; fi; }

echo "== 起基础设施 + iceberg-rest =="
"${COMPOSE[@]}" up -d postgres kafka connect minio minio-init iceberg-rest

echo "== 确保 cdc connector 存在 =="
for _ in $(seq 1 30); do curl -fsS "$CONNECT_URL/connectors" >/dev/null 2>&1 && break; sleep 2; done
curl -fsS "$CONNECT_URL/connectors/vec-stream-pg" >/dev/null 2>&1 && echo "  已存在" || CONNECT_URL="$CONNECT_URL" ./debezium/register.sh >/dev/null

echo "== 构建 spark-lake =="
"${COMPOSE[@]}" build spark-lake >/dev/null

echo "== insert =="
SMOKE_PK="$(docker exec -i "$PG_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAqc "INSERT INTO article (tenant_id,title,body,status) VALUES ('default','iceberg smoke','m','published') RETURNING id;" | tr -dc '0-9')"
echo "  inserted id=$SMOKE_PK"; write; assert "$SMOKE_PK" published

echo "== update -> archived =="
docker exec -i "$PG_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAqc "UPDATE article SET status='archived', updated_at=now() WHERE id=$SMOKE_PK;" >/dev/null
write; assert "$SMOKE_PK" archived

echo "== delete =="
docker exec -i "$PG_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAqc "DELETE FROM article WHERE id=$SMOKE_PK;" >/dev/null
write; assert "$SMOKE_PK" ABSENT
SMOKE_PK=""
echo "iceberg smoke OK"
