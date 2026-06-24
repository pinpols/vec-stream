#!/usr/bin/env bash
# Minimal local smoke test for the running vec_stream stack.
#
# Prerequisites:
#   1. cp .env.example .env and replace change-me-* values
#   2. docker compose up -d
#   3. ./debezium/register.sh
#   4. worker and rag are running in separate terminals
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

API_KEY="${SMOKE_API_KEY:-dev-key-default}"
RAG_URL="${RAG_URL:-http://localhost:8000}"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
PG_CONTAINER="${PG_CONTAINER:-vec-stream-postgres}"
POSTGRES_USER="${POSTGRES_USER:-vec_stream}"
POSTGRES_DB="${POSTGRES_DB:-vec_stream}"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

need curl
need docker

echo "== healthz =="
curl -fsS "$RAG_URL/healthz"
echo

echo "== connector status =="
curl -fsS "$CONNECT_URL/connectors/vec-stream-pg/status"
echo

echo "== insert smoke article =="
docker exec -i "$PG_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'SQL'
INSERT INTO article (tenant_id, title, body, status)
VALUES (
  'default',
  'vec_stream smoke test',
  'smoke test marker: realtime vector sync through Debezium Kafka worker and RAG search',
  'published'
);
SQL

echo "== wait for CDC -> vector sync =="
sleep "${SMOKE_WAIT_SECONDS:-8}"

echo "== search =="
curl -fsS -X POST "$RAG_URL/search" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query":"smoke test marker realtime vector sync","top_k":5,"status":"published"}'
echo
