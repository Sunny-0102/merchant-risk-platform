#!/usr/bin/env bash
set -euo pipefail

mrpdc() {
  docker compose -p local \
    -f docker-compose.yml \
    -f docker-compose.airflow.yml \
    -f docker-compose.airflow.unpause.yml \
    "$@"
}

# Use NOW (not future) as logical_date, but compute target bucket explicitly in SQL.
DB_NOW_UTC=$(mrpdc exec -T postgres psql -U app -d appdb -t -A -c "SELECT now()::text;")
CUR_BUCKET=$(mrpdc exec -T postgres psql -U app -d appdb -t -A -c "SELECT date_bin('15 minutes', now(), '1970-01-01'::timestamptz)::text;")
NEXT_BUCKET=$(mrpdc exec -T postgres psql -U app -d appdb -t -A -c "SELECT (date_bin('15 minutes', now(), '1970-01-01'::timestamptz)+interval '15 minutes')::text;")

echo "DB_NOW_UTC=$DB_NOW_UTC"
echo "CUR_BUCKET=$CUR_BUCKET"
echo "NEXT_BUCKET=$NEXT_BUCKET"

# Pick a logical date that is safely in the past: now - 30 seconds
LOGICAL_DATE=$(mrpdc exec -T postgres psql -U app -d appdb -t -A -c "SELECT (now() - interval '30 seconds')::text;")
RUN_ID="manual__mrp__safe__$(date -u +%Y%m%dT%H%M%SZ)"

echo "LOGICAL_DATE=$LOGICAL_DATE"
echo "RUN_ID=$RUN_ID"

mrpdc exec -T airflow-worker bash -lc "airflow dags trigger -r ${RUN_ID} -l '${LOGICAL_DATE}' mrp_pipeline_dag"
echo "Triggered: $RUN_ID"
