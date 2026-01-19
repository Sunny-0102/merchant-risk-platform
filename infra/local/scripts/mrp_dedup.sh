#!/usr/bin/env bash
set -euo pipefail

SRC="${SRC:-SYNTH_LIVE}"
ENDPOINT_URL="${ENDPOINT_URL:-http://localhost:8080}"

TOKEN="$(curl -s -X POST "${ENDPOINT_URL}/auth/token" \
  -H "Content-Type: application/json" \
  -d '{"username":"airflow","password":"airflow"}' \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])')"

trigger_run () {
  local run_id logical_date
  run_id="manual__mrp__$(date -u +%Y%m%dT%H%M%SZ)"
  logical_date="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
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

echo "== 1) Insert ONE realistic raw event =="
ins_out="$(
docker exec -i -e PGPASSWORD=app postgres psql -U app -d appdb -tA -v ON_ERROR_STOP=1 <<SQL
WITH tpl AS (
  SELECT payload
  FROM mrp.raw_realtime_events
  WHERE source = '${SRC}'
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
    '${SRC}',
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
  RETURNING (payload::jsonb)->>'event_id'     AS event_id,
            (payload::jsonb)->>'merchant_id'  AS merchant_id,
            payload::jsonb                    AS payload_json
)
SELECT
  (event_id || '|' || merchant_id || '|' ||
   regexp_replace(
     encode(convert_to(payload_json::text,'UTF8'),'base64'),
     E'[\\n\\r]+', '', 'g'
   )
  )
FROM ins;
SQL
)"

IFS='|' read -r EVT_ID MID PAYLOAD_B64 <<<"$ins_out"

if [[ -z "${EVT_ID:-}" || -z "${MID:-}" || -z "${PAYLOAD_B64:-}" ]]; then
  echo "ERROR: Could not parse insert output."
  echo "Raw output:"
  echo "$ins_out"
  exit 10
fi

echo "Inserted EVT_ID=$EVT_ID MID=$MID"
echo

echo "== 2) Run #1 (should process it) =="
RUN1="$(trigger_run)"
echo "RUN1=$RUN1"
wait_success "$RUN1"
echo

# Capture the snapshot_time_utc we want to compare, plus metrics (ignore computed_at to avoid false diffs)
snap_time="$(
docker exec -i -e PGPASSWORD=app postgres psql -U app -d appdb -tA -v ON_ERROR_STOP=1 \
  -c "SELECT snapshot_time_utc::text
      FROM mrp.merchant_feature_snapshots
      WHERE merchant_id='${MID}'
      ORDER BY computed_at_utc DESC
      LIMIT 1;"
)"

snap1="$(
docker exec -i -e PGPASSWORD=app postgres psql -U app -d appdb -tA -v ON_ERROR_STOP=1 \
  -c "SELECT concat_ws('|',
        merchant_id,
        snapshot_time_utc::text,
        txn_count_15m::text, gmv_usd_15m::text,
        txn_count_1h::text,  gmv_usd_1h::text,
        txn_count_24h::text, gmv_usd_24h::text
      )
      FROM mrp.merchant_feature_snapshots
      WHERE merchant_id='${MID}'
        AND snapshot_time_utc::text='${snap_time}'
      ORDER BY computed_at_utc DESC
      LIMIT 1;"
)"
echo "snap_time=$snap_time"
echo "snap1=$snap1"
echo

echo "== 3) Insert DUPLICATE raw event (same event_id) =="
docker exec -i -e PGPASSWORD=app postgres psql -U app -d appdb -v ON_ERROR_STOP=1 <<SQL
WITH p AS (
  SELECT convert_from(decode(\$\$${PAYLOAD_B64}\$\$, 'base64'), 'UTF8')::jsonb AS payload_json
)
INSERT INTO mrp.raw_realtime_events (source, payload)
SELECT '${SRC}', payload_json
FROM p;
SQL
echo "Inserted duplicate raw row for EVT_ID=$EVT_ID"
echo

echo "== 4) Run #2 (should NOT duplicate facts or change metrics) =="
RUN2="$(trigger_run)"
echo "RUN2=$RUN2"
wait_success "$RUN2"
echo

cnt="$(
docker exec -i -e PGPASSWORD=app postgres psql -U app -d appdb -tA -v ON_ERROR_STOP=1 \
  -c "SELECT count(*) FROM mrp.fact_payment_events WHERE event_id='${EVT_ID}';"
)"
echo "fact_count=$cnt"

snap2="$(
docker exec -i -e PGPASSWORD=app postgres psql -U app -d appdb -tA -v ON_ERROR_STOP=1 \
  -c "SELECT concat_ws('|',
        merchant_id,
        snapshot_time_utc::text,
        txn_count_15m::text, gmv_usd_15m::text,
        txn_count_1h::text,  gmv_usd_1h::text,
        txn_count_24h::text, gmv_usd_24h::text
      )
      FROM mrp.merchant_feature_snapshots
      WHERE merchant_id='${MID}'
        AND snapshot_time_utc::text='${snap_time}'
      ORDER BY computed_at_utc DESC
      LIMIT 1;"
)"
echo "snap2=$snap2"

if [[ "$cnt" != "1" ]]; then
  echo "❌ FAIL: duplicate raw caused fact duplication for EVT_ID=$EVT_ID"
  exit 20
fi

if [[ "$snap2" != "$snap1" ]]; then
  echo "❌ FAIL: duplicate raw changed snapshot metrics for the same snapshot_time_utc (should not)"
  exit 21
fi

echo "✅ DEDUP PASS: duplicate raw did not create duplicate fact or change snapshot metrics"
echo "RUN1=$RUN1 RUN2=$RUN2 EVT_ID=$EVT_ID MID=$MID snapshot_time_utc=$snap_time"
