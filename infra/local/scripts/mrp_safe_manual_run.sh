#!/usr/bin/env bash
set -euo pipefail

COMPOSE="docker compose"

uniq8() {
  python3 - <<'PY'
import uuid
print(uuid.uuid4().hex[:8])
PY
}

echo "== Context =="
pwd
$COMPOSE ps

echo
echo "== Detect DAG_ID (prefer mrp pipeline) =="
DAG_ID="$($COMPOSE exec -T airflow-scheduler airflow dags list \
  | awk 'NR>2 {print $1}' \
  | grep -E '^mrp' \
  | (grep -E 'pipeline' || true) \
  | head -n1 || true)"

if [ -z "${DAG_ID:-}" ]; then
  DAG_ID="$($COMPOSE exec -T airflow-scheduler airflow dags list \
    | awk 'NR>2 {print $1}' \
    | grep -E '^mrp' \
    | head -n1 || true)"
fi

if [ -z "${DAG_ID:-}" ]; then
  echo "ERROR: Could not find an Airflow DAG starting with 'mrp'."
  echo "Run: $COMPOSE exec -T airflow-scheduler airflow dags list"
  exit 2
fi

echo "DAG_ID=$DAG_ID"

echo
echo "== Choose safe logical_date (UTC now - 1 minute) and unique run_id =="
LOGICAL_DATE="$(python3 - <<'PY'
from datetime import datetime, timezone, timedelta
print((datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat())
PY
)"
RUN_ID="manual__mrp__immediate__$(python3 - <<'PY'
from datetime import datetime, timezone
print(datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
PY
)__$(uniq8)"

echo "LOGICAL_DATE=$LOGICAL_DATE"
echo "RUN_ID=$RUN_ID"

echo
echo "== Pre-run: watermark snapshot =="
$COMPOSE exec -T postgres psql -U app -d appdb -v ON_ERROR_STOP=1 -c "
WITH w AS (
  SELECT last_raw_id
  FROM mrp.raw_ingest_watermark
  WHERE source='SYNTH_LIVE'
),
m AS (
  SELECT COALESCE(MAX(id),0)::bigint AS max_raw_id
  FROM mrp.raw_realtime_events
  WHERE source='SYNTH_LIVE'
)
SELECT
  now() AT TIME ZONE 'UTC' AS now_utc,
  'SYNTH_LIVE' AS source,
  w.last_raw_id,
  m.max_raw_id,
  (m.max_raw_id - w.last_raw_id) AS lag
FROM w CROSS JOIN m;
"

echo
echo "== Trigger DAG =="
$COMPOSE exec -T airflow-scheduler airflow dags trigger "$DAG_ID" -r "$RUN_ID" -l "$LOGICAL_DATE"

echo
echo "== Wait for DAG state (poll) =="
STATE="unknown"
for i in $(seq 1 60); do
  STATE="$($COMPOSE exec -T airflow-scheduler airflow dags state "$DAG_ID" "$LOGICAL_DATE" 2>/dev/null | tail -n1 | tr -d '\r' || true)"
  echo "  [$i] state=$STATE"
  if [ "$STATE" = "success" ] || [ "$STATE" = "failed" ]; then
    break
  fi
  sleep 10
done

echo
echo "== Post-run: watermark + bucket verification =="
$COMPOSE exec -T postgres psql -U app -d appdb -v ON_ERROR_STOP=1 -c "
WITH w AS (
  SELECT last_raw_id
  FROM mrp.raw_ingest_watermark
  WHERE source='SYNTH_LIVE'
),
m AS (
  SELECT COALESCE(MAX(id),0)::bigint AS max_raw_id
  FROM mrp.raw_realtime_events
  WHERE source='SYNTH_LIVE'
)
SELECT
  now() AT TIME ZONE 'UTC' AS now_utc,
  'SYNTH_LIVE' AS source,
  w.last_raw_id,
  m.max_raw_id,
  (m.max_raw_id - w.last_raw_id) AS lag
FROM w CROSS JOIN m;

WITH b AS (
  SELECT date_bin('15 minutes', '$LOGICAL_DATE'::timestamptz, '1970-01-01'::timestamptz) AS bucket
)
SELECT
  b.bucket AS bucket_time_utc,
  COUNT(s.merchant_id) AS snapshot_rows_for_bucket
FROM b
LEFT JOIN mrp.merchant_feature_snapshots s
  ON s.snapshot_time_utc = b.bucket
GROUP BY 1;

SELECT snapshot_time_utc, COUNT(*) AS rows
FROM mrp.merchant_feature_snapshots
GROUP BY 1
ORDER BY 1 DESC
LIMIT 5;
"

echo
echo "== DONE =="
echo "DAG_ID=$DAG_ID"
echo "RUN_ID=$RUN_ID"
echo "LOGICAL_DATE=$LOGICAL_DATE"
echo "FINAL_STATE=$STATE"
