#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../../.." && pwd)"
LOCAL_DIR="${REPO_ROOT}/infra/local"

echo "Running local E2E..."
echo "REPO_ROOT=${REPO_ROOT}"
echo "LOCAL_DIR=${LOCAL_DIR}"

cd "${LOCAL_DIR}"

compose() {
  docker compose -p local \
    -f docker-compose.yml \
    -f docker-compose.airflow.yml \
    -f docker-compose.airflow.unpause.yml \
    "$@"
}

echo
echo "== 0) Ensure stack is up =="
compose up -d --build
echo
echo "== 0.5) Wait for Airflow to register DAGs in the metadb =="

dc() {
  docker compose -p local \
    -f "${LOCAL_DIR}/docker-compose.yml" \
    -f "${LOCAL_DIR}/docker-compose.airflow.yml" \
    -f "${LOCAL_DIR}/docker-compose.airflow.unpause.yml" \
    "$@"
}

wait_for_dag() {
  local dag_id="$1"
  for i in $(seq 1 90); do
    if dc exec -T airflow-scheduler airflow dags list 2>/dev/null \
      | awk 'NR>2 {print $1}' \
      | grep -qx "${dag_id}"; then
      echo "DAG ready: ${dag_id}"
      return 0
    fi
    echo "waiting for DAG to be registered (${dag_id}) retry ${i}/90"
    sleep 2
  done

  echo "ERROR: DAG not registered in metadb after waiting: ${dag_id}"
  echo "Current dags list:"
  dc exec -T airflow-scheduler airflow dags list || true
  return 1
}

wait_for_dag "mrp_smoke_postgres"
wait_for_dag "mrp_smoke_ingestion_api"
wait_for_dag "mrp_smoke_ingest_raw"
wait_for_dag "mrp_pipeline_dag"

echo
echo "== 1) Smoke checks (trigger + wait) =="
LOGICAL_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Unique run ids prevent DagRunAlreadyExists if you rerun quickly
RID_PG="manual__smoke__pg__$(date -u +%Y%m%dT%H%M%SZ)__$RANDOM"
RID_API="manual__smoke__api__$(date -u +%Y%m%dT%H%M%SZ)__$RANDOM"
RID_ING="manual__smoke__ingest__$(date -u +%Y%m%dT%H%M%SZ)__$RANDOM"

compose exec -T airflow-worker bash -lc "
  set -e
  airflow dags trigger mrp_smoke_postgres       -r '${RID_PG}'  -l '${LOGICAL_DATE}'
  airflow dags trigger mrp_smoke_ingestion_api  -r '${RID_API}' -l '${LOGICAL_DATE}'
  airflow dags trigger mrp_smoke_ingest_raw     -r '${RID_ING}' -l '${LOGICAL_DATE}'
"

# Wait for smokes to finish (poll via CLI)
compose exec -T airflow-worker bash -lc "
  set -e
  for dag in mrp_smoke_postgres mrp_smoke_ingestion_api mrp_smoke_ingest_raw; do
    echo \"-- waiting for \$dag (logical_date=${LOGICAL_DATE})\"
    for i in \$(seq 1 60); do
      st=\$(airflow dags state \"\$dag\" '${LOGICAL_DATE}' 2>/dev/null | tail -n1 | tr -d '\r' || true)
      echo \"   poll \$i state=\$st\"
      if [[ \"\$st\" == \"success\" ]]; then break; fi
      if [[ \"\$st\" == \"failed\" ]]; then exit 2; fi
      sleep 2
    done
  done
"

echo
echo "== 2) Run E2E harness =="
bash scripts/mrp_e2e.sh

echo
echo "== 3) Run dedup + idempotency checks =="
bash scripts/mrp_dedup.sh
bash scripts/mrp_idempotency.sh

echo
echo "== 4) Bucket trigger sanity =="
bash scripts/mrp_trigger_bucket.sh

echo
echo "✅ DONE: local CI checks completed"
