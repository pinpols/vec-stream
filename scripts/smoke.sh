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
SMOKE_PK=""
OTHER_PK=""

cleanup() {
  local ids=()
  [ -n "${SMOKE_PK:-}" ] && ids+=("$SMOKE_PK")
  [ -n "${OTHER_PK:-}" ] && ids+=("$OTHER_PK")
  [ "${#ids[@]}" -eq 0 ] && return 0
  local joined
  joined="$(IFS=,; echo "${ids[*]}")"
  docker exec -i "$PG_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null <<SQL || true
DELETE FROM article WHERE id IN ($joined);
SQL
}
trap cleanup EXIT

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

need curl
need docker
need python3

search() {
  local query="$1"
  local status="${2:-published}"
  python3 - "$query" "$status" "$RAG_URL" "$API_KEY" <<'PY'
import json, sys, urllib.error, urllib.request

query, status, rag_url, api_key = sys.argv[1:5]
payload = json.dumps({"query": query, "top_k": 10, "status": status}).encode()
req = urllib.request.Request(
    rag_url.rstrip("/") + "/search",
    data=payload,
    headers={"Content-Type": "application/json", "X-API-Key": api_key},
    method="POST",
)
with urllib.request.urlopen(req, timeout=30) as resp:
    print(resp.read().decode())
PY
}

wait_for_pk() {
  local query="$1"
  local status="$2"
  local pk="$3"
  local want="${4:-present}"
  local tries="${SMOKE_POLL_TRIES:-20}"
  local delay="${SMOKE_POLL_DELAY:-2}"
  local out
  for _ in $(seq 1 "$tries"); do
    out="$(search "$query" "$status")"
    if python3 - "$out" "$pk" "$want" <<'PY'
import json, sys
hits = json.loads(sys.argv[1])
pk = str(sys.argv[2])
want = sys.argv[3]
present = any(str(h.get("source_pk")) == pk for h in hits)
sys.exit(0 if ((want == "present" and present) or (want == "absent" and not present)) else 1)
PY
    then
      echo "$out"
      return 0
    fi
    sleep "$delay"
  done
  echo "timed out waiting for pk=$pk to be $want in status=$status query=$query" >&2
  echo "last response: $out" >&2
  return 1
}

wait_indexed_pk() {
  local pk="$1"
  local tenant="$2"
  local tries="${SMOKE_POLL_TRIES:-20}"
  local delay="${SMOKE_POLL_DELAY:-2}"
  local count
  for _ in $(seq 1 "$tries"); do
    count="$(
      docker exec -i "$PG_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA <<SQL
SELECT count(*) FROM doc_vectors
WHERE tenant_id = '$tenant' AND source_table = 'article' AND source_pk = '$pk';
SQL
    )"
    if [ "${count:-0}" -gt 0 ]; then
      return 0
    fi
    sleep "$delay"
  done
  echo "timed out waiting for doc_vectors row tenant=$tenant pk=$pk" >&2
  return 1
}

echo "== healthz =="
curl -fsS "$RAG_URL/healthz"
echo

echo "== auth rejects missing key =="
code="$(curl -s -o /dev/null -w '%{http_code}' -X POST "$RAG_URL/search" \
  -H "Content-Type: application/json" \
  -d '{"query":"auth should fail"}')"
test "$code" = "401"

echo "== connector status =="
curl -fsS "$CONNECT_URL/connectors/vec-stream-pg/status"
echo

echo "== insert smoke article =="
SMOKE_PK="$(
docker exec -i "$PG_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA <<'SQL'
INSERT INTO article (tenant_id, title, body, status)
VALUES (
  'default',
  'vec_stream smoke test',
  'smoke test marker: realtime vector sync through Debezium Kafka worker and RAG search',
  'published'
)
RETURNING id;
SQL
)"
echo "smoke article id=$SMOKE_PK"

echo "== wait for insert to become searchable =="
wait_for_pk "smoke test marker realtime vector sync" "published" "$SMOKE_PK" present

echo "== tenant isolation: other tenant data is invisible with default key =="
OTHER_PK="$(
docker exec -i "$PG_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA <<'SQL'
INSERT INTO article (tenant_id, title, body, status)
VALUES (
  'other-smoke',
  'vec_stream tenant isolation smoke',
  'tenant isolation marker should not be visible to default tenant',
  'published'
)
RETURNING id;
SQL
)"
wait_indexed_pk "$OTHER_PK" "other-smoke"
wait_for_pk "tenant isolation marker visible default tenant" "published" "$OTHER_PK" absent

echo "== metadata update: status filter follows source row without re-embedding =="
docker exec -i "$PG_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<SQL
UPDATE article SET status = 'archived', updated_at = now() WHERE id = $SMOKE_PK;
SQL
wait_for_pk "smoke test marker realtime vector sync" "published" "$SMOKE_PK" absent
wait_for_pk "smoke test marker realtime vector sync" "archived" "$SMOKE_PK" present

echo "== delete removes vectors =="
docker exec -i "$PG_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<SQL
DELETE FROM article WHERE id IN ($SMOKE_PK, $OTHER_PK);
SQL
wait_for_pk "smoke test marker realtime vector sync" "archived" "$SMOKE_PK" absent

echo "smoke OK"
