#!/usr/bin/env bash
# 注册 Debezium Postgres connector,把密码从 .env 注入到 register-postgres.json 的占位符,
# 避免凭据落盘明文(register-postgres.json 里 database.password 是占位符 __DEBEZIUM_PASSWORD__)。
#
# 用法:  ./debezium/register.sh           # 默认读项目根 .env
#        CONNECT_URL=http://host:8083 ./debezium/register.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"

# 读 .env(只为拿 DEBEZIUM_PASSWORD;不污染当前 shell 其它变量)
if [ -f "$ROOT/.env" ]; then
  DEBEZIUM_PASSWORD="$(grep -E '^DEBEZIUM_PASSWORD=' "$ROOT/.env" | head -1 | cut -d= -f2-)"
fi
: "${DEBEZIUM_PASSWORD:?在 .env 设置 DEBEZIUM_PASSWORD,或 export 后再运行}"

# 用 python 安全注入密码(避免 sed 对特殊字符的转义问题),再 POST
payload="$(
  DEBEZIUM_PASSWORD="$DEBEZIUM_PASSWORD" python3 - "$HERE/register-postgres.json" <<'PY'
import json, os, sys
cfg = json.load(open(sys.argv[1]))
cfg["config"]["database.password"] = os.environ["DEBEZIUM_PASSWORD"]
print(json.dumps(cfg))
PY
)"

echo "注册 connector 到 $CONNECT_URL ..."
curl -fsS -X POST "$CONNECT_URL/connectors" \
  -H 'Content-Type: application/json' \
  -d "$payload" | python3 -m json.tool || {
    echo "若已存在,用 PUT 更新配置:"
    echo "  curl -X PUT $CONNECT_URL/connectors/vecstream-pg/config -H 'Content-Type: application/json' -d '<config 部分>'"
    exit 1
  }
echo
echo "状态:curl $CONNECT_URL/connectors/vecstream-pg/status"
