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

mk_run_id() {
  echo "manual__mrp__$(date -u +%Y%m%dT%H%M%SZ)__$(uniq8)"
}

echo "== A) Insert ONE realistic raw event for $SRC =="

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

trigger_run () {
  local run_id logical_date
  run_id="$(mk_run_id)"
  logical_date="$(db_logical_date)"
  curl -s -X POST "${ENDPOINT_URL}/api/v2/dags/mrp_pipeline_dag/dagRuns" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"dag_run_id\":\"${run_id}\",\"logical_date\":\"${logical_date}\",\"conf\":{}}" >/dev/null
  echo "$run_id"
}

wait_success () {
  local run_id="$1"
  for i in $(seq 1 60); do
    st="$(curl -s "${ENDPOINT_URL}/api/v2/dags/mrp_pipeline_dag/dagRuns/${run_id}" \
      -H "Authorization: Bearer ${TOKEN}" \
      | python3 -c 'import sys,json; print(json.load(sys.stdin).get("state"))')"
    echo "poll $i state=$st"
    if [[ "$st" == "success" ]]; then return 0; fi
    if [[ "$st" == "failed" ]]; then
      echo "ERROR: DagRun failed. RUN_ID=$run_id"
      return 2
    fi
    sleep 2
  done
  echo "ERROR: Timed out waiting for success. RUN_ID=$run_id"
  return 3
}

echo "== B) Run #1 (should process the new event) =="
RUN1="$(trigger_run)"
echo "RUN1=$RUN1"
wait_success "$RUN1"
echo

echo "== Capture snapshot state after Run #1 =="
snap1="$(
docker exec -i -e PGPASSWORD=app postgres psql -U app -d appdb -tA -v ON_ERROR_STOP=1 <<SQL
SELECT concat_ws('|',
  merchant_id,
  snapshot_time_utc::text,
  computed_at_utc::text,
  txn_count_15m::text,
  gmv_usd_15m::text,
  txn_count_1h::text,
  gmv_usd_1h::text
)
FROM mrp.merchant_feature_snapshots
WHERE merchant_id = '$MID'
ORDER BY computed_at_utc DESC
LIMIT 1;
SQL
)"
echo "snap1=$snap1"
echo

echo "== C) Run #2 (NO new raw insert; should be a no-op) =="
RUN2="$(trigger_run)"
echo "RUN2=$RUN2"
wait_success "$RUN2"
echo

echo "== Capture snapshot state after Run #2 =="
snap2="$(
docker exec -i -e PGPASSWORD=app postgres psql -U app -d appdb -tA -v ON_ERROR_STOP=1 <<SQL
SELECT concat_ws('|',
  merchant_id,
  snapshot_time_utc::text,
  computed_at_utc::text,
  txn_count_15m::text,
  gmv_usd_15m::text,
  txn_count_1h::text,
  gmv_usd_1h::text
)
FROM mrp.merchant_feature_snapshots
WHERE merchant_id = '$MID'
ORDER BY computed_at_utc DESC
LIMIT 1;
SQL
)"
echo "snap2=$snap2"
echo

echo "== D) Verify fact row count for EVT_ID is exactly 1 =="
cnt="$(
docker exec -i -e PGPASSWORD=app postgres psql -U app -d appdb -tA -v ON_ERROR_STOP=1 \
  -c "SELECT count(*) FROM mrp.fact_payment_events WHERE event_id = '$EVT_ID';"
)"
echo "fact_count=$cnt"
echo

if [[ "$cnt" != "1" ]]; then
  echo "❌ FAIL: expected fact row count 1 for EVT_ID=$EVT_ID"
  exit 10
fi

if [[ "$snap2" != "$snap1" ]]; then
  echo "❌ FAIL: snapshot changed on Run #2 (expected no change)"
  echo "snap1=$snap1"
  echo "snap2=$snap2"
  exit 11
fi

echo "✅ IDEMPOTENCY PASS: second run made no changes"
echo "RUN1=$RUN1 RUN2=$RUN2 EVT_ID=$EVT_ID MID=$MID"
