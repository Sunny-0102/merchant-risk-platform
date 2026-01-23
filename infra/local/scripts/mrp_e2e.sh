#!/usr/bin/env bash
set -euo pipefail

SRC="${SRC:-SYNTH_LIVE}"
ENDPOINT_URL="${ENDPOINT_URL:-http://localhost:8080}"

uniq8() {
  python3 - <<'PY'
import uuid
print(uuid.uuid4().hex[:8])
PY
}

db_logical_date() {
  docker exec -i -e PGPASSWORD=app postgres psql -U app -d appdb -tA -v ON_ERROR_STOP=1 -c \
    "SELECT to_char((now() - interval '30 seconds') at time zone 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"');"
}

RUN_ID="manual__mrp__$(date -u +%Y%m%dT%H%M%SZ)__$(uniq8)"
LOGICAL_DATE="$(db_logical_date)"

echo "== 1) Insert one realistic raw event for $SRC =="

ins_out="$(
docker exec -i -e PGPASSWORD=app postgres psql -U app -d appdb -tA -v ON_ERROR_STOP=1 <<'SQL'
WITH tpl AS (
  SELECT payload
  FROM mrp.raw_realtime_events
  WHERE source = 'SYNTH_LIVE'
    AND (payload::jsonb) ? 'event_id'
    AND (payload::jsonb) ? 'event_type'
    AND (payload::jsonb) ? 'merchant_id'
  ORDER BY id DESC
  LIMIT 1
),
gen AS (
  SELECT
    payload,
    'evt_'||substr(md5(random()::text),1,32) AS new_event_id,
    'ord_'||substr(md5(random()::text),1,32) AS new_order_id,
    'tr_'||substr(md5(random()::text),1,32)  AS new_trace_id,
    'cor_'||substr(md5(random()::text),1,32) AS new_correlation_id,
    'cap_'||substr(md5(random()::text),1,32) AS new_capture_id,
    to_char(now() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS ts
  FROM tpl
),
ins AS (
  INSERT INTO mrp.raw_realtime_events (source, payload)
  SELECT
    'SYNTH_LIVE',
    payload || jsonb_build_object(
      'event_id', new_event_id,
      'order_id', new_order_id,
      'trace_id', new_trace_id,
      'correlation_id', new_correlation_id,
      'capture_id', new_capture_id,
      'event_time_utc', ts,
      'ingested_at_utc', ts
    )
  FROM gen
  RETURNING (payload::jsonb)->>'event_id' AS event_id,
            (payload::jsonb)->>'merchant_id' AS merchant_id
)
SELECT concat_ws(' ', event_id, merchant_id) FROM ins;
SQL
)"

EVT_ID="$(awk '{print $1}' <<<"$ins_out")"
MID="$(awk '{print $2}' <<<"$ins_out")"

echo "Inserted EVT_ID=$EVT_ID MID=$MID"
echo

echo "== 2) Trigger manual DagRun =="
TOKEN="$(curl -s -X POST "${ENDPOINT_URL}/auth/token" \
  -H "Content-Type: application/json" \
  -d '{"username":"airflow","password":"airflow"}' \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])')"

# MRP_PRECHECK_DAG_READY
echo "== Precheck: wait for DAG to be available in Airflow API =="
DAG_URL="${ENDPOINT_URL}/api/v2/dags/mrp_pipeline_dag"

code=""
body=""
for i in $(seq 1 90); do
  resp="$(curl -sS -H "Authorization: Bearer ${TOKEN}" -w "\n%{http_code}" "${DAG_URL}" || true)"
  body="${resp%$'\n'*}"
  code="${resp##*$'\n'}"
  if [ "$code" = "200" ]; then
    break
  fi
  echo "dag not ready yet (http=$code) retry $i/90"
  sleep 2
done

if [ "${code:-}" != "200" ]; then
  echo "ERROR: DAG not available in API after waiting. last_http=${code:-} body=${body:-}"
  exit 1
fi

paused="$(printf '%s' "$body" | python3 - <<'PYIN'
import sys, json
try:
    d=json.load(sys.stdin)
    print(d.get("is_paused"))
except Exception:
    print("")
PYIN
 2>/dev/null || true)"

if [ "$paused" = "True" ] || [ "$paused" = "true" ]; then
  echo "== Unpausing DAG =="
  curl -sS -X PATCH "${DAG_URL}" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    -d '{"is_paused": false}' >/dev/null || true
fi

echo "RUN_ID=$RUN_ID"
echo "LOGICAL_DATE=$LOGICAL_DATE"

curl -s -X POST "${ENDPOINT_URL}/api/v2/dags/mrp_pipeline_dag/dagRuns" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"dag_run_id\":\"${RUN_ID}\",\"logical_date\":\"${LOGICAL_DATE}\",\"conf\":{}}" >/dev/null

echo
st=""
echo "== 3) Wait for DAG success (poll every 2s, up to 60 polls) =="
for i in $(seq 1 60); do
  st="$(curl -s "${ENDPOINT_URL}/api/v2/dags/mrp_pipeline_dag/dagRuns/${RUN_ID}" \
    -H "Authorization: Bearer ${TOKEN}" \
    | python3 -c 'import sys,json; print(json.load(sys.stdin).get("state"))')"
  echo "poll $i state=$st"
  if [[ "$st" == "success" ]]; then
    break
  fi
  if [[ "$st" == "failed" ]]; then
    echo "ERROR: DagRun failed. RUN_ID=$RUN_ID"
    exit 2
  fi
  sleep 2
done

echo
if [ "${st:-}" != "success" ]; then
  echo "ERROR: Timed out waiting for success. RUN_ID=$RUN_ID last_state=${st:-}"
  exit 1
fi

echo "== 4) Validate EVT_ID landed in fact + latest snapshot exists for MID =="
docker exec -i -e PGPASSWORD=app postgres psql -U app -d appdb -v ON_ERROR_STOP=1 \
  -v evt_id="$EVT_ID" -v mid="$MID" <<SQL
\echo --- fact row for EVT_ID ---
SELECT event_id, event_type, ingested_at_utc, merchant_id, order_id, amount_usd, status
FROM mrp.fact_payment_events
WHERE event_id = :'evt_id';

\echo
\echo --- latest snapshot for MID ---
SELECT merchant_id, snapshot_time_utc, computed_at_utc, txn_count_15m, gmv_usd_15m, txn_count_1h, gmv_usd_1h
FROM mrp.merchant_feature_snapshots
WHERE merchant_id = :'mid'
ORDER BY computed_at_utc DESC
LIMIT 1;
SQL

echo

FACT_CNT="$(docker exec -i -e PGPASSWORD=app postgres \
  psql -U app -d appdb -tAc "SELECT COUNT(*) FROM mrp.fact_payment_events WHERE event_id='${EVT_ID}';" | tr -d '[:space:]')"
SNAP_CNT="$(docker exec -i -e PGPASSWORD=app postgres \
  psql -U app -d appdb -tAc "SELECT COUNT(*) FROM mrp.merchant_feature_snapshots WHERE merchant_id='${MID}';" | tr -d '[:space:]')"

if [ "${FACT_CNT:-0}" -lt 1 ] || [ "${SNAP_CNT:-0}" -lt 1 ]; then
  echo "ERROR: E2E validation failed. fact_cnt=${FACT_CNT:-} snap_cnt=${SNAP_CNT:-} EVT_ID=$EVT_ID MID=$MID"
  exit 1
fi

echo "✅ E2E PASS: raw -> fact -> snapshot verified"
echo "RUN_ID=$RUN_ID EVT_ID=$EVT_ID MID=$MID"
