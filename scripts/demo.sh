#!/usr/bin/env bash
# 一键本地演示：启动服务 -> 建租户/凭据/订阅 -> 发事件 -> 观察重试/死信/凭据轮换。
# 不依赖任何外部服务或真实凭据。
set -euo pipefail
cd "$(dirname "$0")/.."

export GATEWAY_DB="${GATEWAY_DB:-.data/demo.db}"
export GATEWAY_RUN_WORKER=true
export GATEWAY_BACKOFF_BASE="${GATEWAY_BACKOFF_BASE:-1}"
export GATEWAY_MAX_ATTEMPTS=4
rm -f "$GATEWAY_DB" "$GATEWAY_DB-wal" "$GATEWAY_DB-shm" 2>/dev/null || true

BASE="http://127.0.0.1:8000"
LOG_FILE=".data/demo-server.log"
mkdir -p .data
ADMIN=(-H "X-Admin-Key: local-admin-key" -H "Content-Type: application/json")
AUTH=(-H "X-API-Key: key-acme-demo-123" -H "Content-Type: application/json")

echo "== 启动服务（后台） =="
uv run uvicorn main:app --host 127.0.0.1 --port 8000 >"$LOG_FILE" 2>&1 &
SRV=$!
trap 'kill $SRV 2>/dev/null || true' EXIT
for _ in $(seq 1 30); do
  curl -sf "$BASE/" >/dev/null 2>&1 && break
  sleep 0.5
done

jqr() { python3 -c "import sys,json;d=json.load(sys.stdin);print(eval(sys.argv[1]))" "$1"; }

echo "== 1) 建租户 / 凭据（新旧并存）/ 订阅 =="
curl -sS "${ADMIN[@]}" -X POST "$BASE/admin/tenants" -d '{"tenant_id":"acme","api_key":"key-acme-demo-123","rate_limit_per_minute":600}' >/dev/null
NOW=$(python3 -c "from datetime import datetime,timezone;print(datetime.now(timezone.utc).isoformat())")
FROM_OLD=$(python3 -c "from datetime import datetime,timezone,timedelta;print((datetime.now(timezone.utc)-timedelta(days=30)).isoformat())")
TO_OLD=$(python3 -c "from datetime import datetime,timezone,timedelta;print((datetime.now(timezone.utc)+timedelta(days=1)).isoformat())")
curl -sS "${AUTH[@]}" -X POST "$BASE/admin/credentials" -d "{\"key_id\":\"k-old\",\"secret\":\"secret-old-demo-123\",\"valid_from\":\"$FROM_OLD\",\"valid_to\":\"$TO_OLD\"}" >/dev/null
curl -sS "${AUTH[@]}" -X POST "$BASE/admin/credentials" -d "{\"key_id\":\"k-new\",\"secret\":\"secret-new-demo-123\",\"valid_from\":\"$NOW\"}" >/dev/null
curl -sS "${AUTH[@]}" -X POST "$BASE/admin/subscriptions" -d '{"event_type":"order.created","target_url":"http://mock/orders","min_version":1,"max_version":2,"newer_version_policy":"DELIVER_ANYWAY","known_fields":["order_id"]}' | jqr 'd["subscription_id"]' | xargs echo "subscription_id="
curl -sS "${AUTH[@]}" -X POST "$BASE/admin/subscriptions" -d '{"event_type":"order.paid","target_url":"http://mock/retries","min_version":1}' >/dev/null

echo "== 2) 幂等：重复事件第二次返回 DUPLICATE =="
curl -sS "${AUTH[@]}" -X POST "$BASE/v1/events" -d '{"event_id":"demo-evt-1","event_type":"order.created","event_version":1,"data":{"order_id":"o-1"}}' | jqr '"first:  "+d["status"]'
curl -sS "${AUTH[@]}" -X POST "$BASE/v1/events" -d '{"event_id":"demo-evt-1","event_type":"order.created","event_version":1,"data":{"order_id":"o-1"}}' | jqr '"repeat: "+d["status"]'

echo "== 3) 接收端前 2 次失败 -> 观察指数退避重试（1s,2s）后成功 =="
curl -sS "${ADMIN[@]}" -X POST "$BASE/admin/mock/endpoints/mock/retries" -d '{"fail_times":2,"fail_status":503}' >/dev/null
curl -sS "${AUTH[@]}" -X POST "$BASE/v1/events" -d '{"event_id":"demo-evt-2","event_type":"order.paid","event_version":1,"data":{"order_id":"o-2"}}' >/dev/null
sleep 6
curl -sS "${AUTH[@]}" "$BASE/admin/events/demo-evt-2/deliveries" | jqr '"state="+d[0]["state"]+" attempts="+str(d[0]["attempts"])'

echo "== 4) 持续失败 -> 重试耗尽进入死信，可查询 =="
curl -sS "${AUTH[@]}" -X POST "$BASE/admin/subscriptions" -d '{"event_type":"order.cancelled","target_url":"http://mock/boom","min_version":1}' >/dev/null
curl -sS "${ADMIN[@]}" -X POST "$BASE/admin/mock/endpoints/mock/boom" -d '{"fail_times":99,"fail_status":500}' >/dev/null
curl -sS "${AUTH[@]}" -X POST "$BASE/v1/events" -d '{"event_id":"demo-evt-3","event_type":"order.cancelled","event_version":1,"data":{}}' >/dev/null
sleep 10
curl -sS "${AUTH[@]}" "$BASE/admin/dead-letters" | jqr '"dead_letters="+str(len(d))+" reason="+d[0]["fail_reason"]'

echo
echo "完成。投递日志见 $LOG_FILE；交互式文档：$BASE/docs"
