#!/usr/bin/env bash
# Fast structural checks for CI and pre-commit.
# This does not start containers; it only validates compose merges and script syntax.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

load_defaults() {
  if [ -f .env.example ]; then
    set -a
    # shellcheck disable=SC1091
    . ./.env.example
    set +a
    # compose 文件声明 env_file: .env,需要磁盘上真实文件;CI 里 .env 被 gitignore 不存在,
    # 用 .env.example 兜底生成,避免 `compose config` 因缺 .env 报错(本地已有 .env 时不覆盖)。
    [ -f .env ] || cp .env.example .env
  fi
  export BATCH_NETWORK="${BATCH_NETWORK:-batch-platform_batch-network}"
}

compose_config() {
  local name="$1"
  shift
  echo "== compose config: $name =="
  "$@" config --quiet
}

need docker
need python3
load_defaults

compose_config base docker compose
compose_config apps docker compose -f docker-compose.yml -f docker-compose.apps.yml
compose_config lake docker compose -f docker-compose.yml -f docker-compose.lake.yml
compose_config reuse-batch docker compose -f docker-compose.yml -f docker-compose.lake.yml -f docker-compose.reuse-batch.yml
compose_config paimon docker compose -f docker-compose.yml -f docker-compose.lake.yml -f docker-compose.paimon.yml
compose_config monitoring docker compose -f docker-compose.monitoring.yml
compose_config otel docker compose -f docker-compose.otel.yml

echo "== bash syntax =="
bash -n \
  scripts/smoke.sh \
  scripts/hudi-smoke.sh \
  scripts/iceberg-smoke.sh \
  scripts/stream-smoke.sh \
  scripts/validate.sh \
  debezium/register.sh \
  db/init/02-security.sh \
  spark-lake/run.sh

echo "== python syntax: spark-lake =="
python3 -m py_compile spark-lake/*.py
python3 -m unittest discover -s spark-lake/tests

echo "== compose host ports are loopback-bound =="
python3 - <<'PY'
from pathlib import Path
import re
import sys

problems = []
for path in sorted(Path(".").glob("docker-compose*.yml")):
    for lineno, line in enumerate(path.read_text().splitlines(), start=1):
        # Reject short syntax like "8083:8083"; require "127.0.0.1:8083:8083".
        if re.match(r"\s*-\s*['\"]?\d{2,5}:", line):
            problems.append(f"{path}:{lineno}: host port must bind 127.0.0.1: {line.strip()}")
if problems:
    print("\n".join(problems), file=sys.stderr)
    sys.exit(1)
PY

if command -v shellcheck >/dev/null 2>&1; then
  echo "== shellcheck =="
  shellcheck \
    scripts/smoke.sh \
    scripts/hudi-smoke.sh \
    scripts/iceberg-smoke.sh \
    scripts/stream-smoke.sh \
    scripts/validate.sh \
    debezium/register.sh \
    db/init/02-security.sh \
    spark-lake/run.sh
elif [ "${REQUIRE_SHELLCHECK:-false}" = "true" ]; then
  echo "shellcheck is required but not installed" >&2
  exit 1
else
  echo "== shellcheck skipped: command not found =="
fi

echo "validate OK"
