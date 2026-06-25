#!/usr/bin/env bash
# 连续流冒烟:验证常驻 Structured Streaming 自动捡起 CDC 变更(不手动跑写作业)。
# 前置:spark-lake-hudi-stream + spark-lake-iceberg-stream 已 up -d 在跑。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"; cd "$ROOT"
PG_CONTAINER="${PG_CONTAINER:-vec-stream-postgres}"
POSTGRES_USER="${POSTGRES_USER:-vec_stream}"; POSTGRES_DB="${POSTGRES_DB:-vec_stream}"
COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.lake.yml)
WAIT="${STREAM_WAIT:-22}"   # 等一个微批(trigger 10s + 处理)

SMOKE_PK=""
cleanup() { [ -n "${SMOKE_PK:-}" ] && docker exec -i "$PG_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAqc "DELETE FROM article WHERE id=$SMOKE_PK;" >/dev/null 2>&1 || true; }
trap cleanup EXIT

status() { "${COMPOSE[@]}" run --rm spark-lake "query-$1" article "$2" 2>/dev/null | grep -oE 'RESULT=[^[:space:]]+' | tail -1 | cut -d= -f2; }
assert() { # assert <engine> <id> <want>
  local got; got="$(status "$1" "$2")"
  if [ "$got" = "$3" ]; then echo "  $1 id=$2 -> $got ✓"; else echo "  $1 id=$2 -> '$got'(期望 '$3')✗" >&2; exit 1; fi
}

sql() { docker exec -i "$PG_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAqc "$1"; }

echo "== insert(不手动跑写作业,等流自动捡)=="
SMOKE_PK="$(sql "INSERT INTO article (tenant_id,title,body,status) VALUES ('default','stream smoke','m','published') RETURNING id;" | tr -dc '0-9')"
echo "  inserted id=$SMOKE_PK; 等 ${WAIT}s"; sleep "$WAIT"
assert hudi "$SMOKE_PK" published
assert iceberg "$SMOKE_PK" published

echo "== update -> archived =="
sql "UPDATE article SET status='archived', updated_at=now() WHERE id=$SMOKE_PK;" >/dev/null
echo "  等 ${WAIT}s"; sleep "$WAIT"
assert hudi "$SMOKE_PK" archived
assert iceberg "$SMOKE_PK" archived

echo "== delete =="
sql "DELETE FROM article WHERE id=$SMOKE_PK;" >/dev/null
echo "  等 ${WAIT}s"; sleep "$WAIT"
assert hudi "$SMOKE_PK" ABSENT
assert iceberg "$SMOKE_PK" ABSENT
SMOKE_PK=""
echo "stream smoke OK"
