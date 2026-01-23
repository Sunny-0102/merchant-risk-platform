#!/usr/bin/env bash
set -euo pipefail

mrpdc() {
  docker compose -p local \
    -f docker-compose.yml \
    -f docker-compose.airflow.yml \
    -f docker-compose.airflow.unpause.yml \
    "$@"
}

# Small helper: unique run_id even when the bucket tag repeats
make_run_id() {
  local bucket_tag="$1"
  # RANDOM exists in bash; add timestamp too to avoid same-second collisions in CI
  echo "manual__mrp__e2e__${bucket_tag}__$(date -u +%Y%m%dT%H%M%SZ)__${RANDOM}"
}

# Use DB time to avoid host/DB drift. Trim to whole seconds (no microseconds).
DB_NOW_UTC="$(mrpdc exec -T postgres psql -U app -d appdb -t -A -c "SELECT now()::text;")"

# A safe logical_date in the past (whole seconds)
LOGICAL_DATE="$(mrpdc exec -T postgres psql -U app -d appdb -t -A -c \
  "SELECT (date_trunc('second', now() - interval '30 seconds'))::timestamptz::text;")"

# Bucket derived from logical_date (this is what your DAG recompute uses via date_bin)
BUCKET="$(mrpdc exec -T postgres psql -U app -d appdb -t -A -c \
  "SELECT date_bin('15 minutes', '${LOGICAL_DATE}'::timestamptz, '1970-01-01'::timestamptz)::text;")"

# Human-friendly tag for the bucket (YYYYMMDDTHHMM00Z)
BUCKET_TAG="$(mrpdc exec -T postgres psql -U app -d appdb -t -A -c \
  "SELECT to_char('${BUCKET}'::timestamptz, 'YYYYMMDD\"T\"HH24MI\"00Z\"');")"

RUN_ID="$(make_run_id "${BUCKET_TAG}")"

echo "DB_NOW_UTC=$DB_NOW_UTC"
echo "LOGICAL_DATE=$LOGICAL_DATE"
echo "BUCKET=$BUCKET"
echo "BUCKET_TAG=$BUCKET_TAG"
echo "RUN_ID=$RUN_ID"

# Trigger (use scheduler container CLI; worker is also fine, scheduler is conventional)
mrpdc exec -T airflow-scheduler bash -lc \
  "airflow dags trigger -r '${RUN_ID}' -l '${LOGICAL_DATE}' mrp_pipeline_dag"

echo "Triggered: $RUN_ID"
