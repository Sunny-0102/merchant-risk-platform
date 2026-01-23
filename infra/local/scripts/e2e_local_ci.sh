#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../../.." && pwd)"
LOCAL_DIR="${REPO_ROOT}/infra/local"

echo "Running local E2E..."
echo "REPO_ROOT=${REPO_ROOT}"
echo "LOCAL_DIR=${LOCAL_DIR}"

cd "${LOCAL_DIR}"

dc() {
  docker compose -p local \
    -f "${LOCAL_DIR}/docker-compose.yml" \
    -f "${LOCAL_DIR}/docker-compose.airflow.yml" \
    -f "${LOCAL_DIR}/docker-compose.airflow.unpause.yml" \
    "$@"
}

on_err() {
  echo
  echo "❌ ERROR: local CI failed. Dumping quick diagnostics..."
  echo
  echo "== docker compose ps =="
  dc ps || true

  echo
  echo "== scheduler logs (tail) =="
  dc logs --no-color --tail 200 airflow-scheduler || true

  echo
  echo "== dag-processor logs (tail) =="
  dc logs --no-color --tail 200 airflow-dag-processor || true

  echo
  echo "== worker logs (tail) =="
  dc logs --no-color --tail 200 airflow-worker || true
}
trap on_err ERR

wait_service() {
  local svc="$1"
  local tries="${2:-90}"
  for i in $(seq 1 "${tries}"); do
    if dc ps --status running --services 2>/dev/null | grep -qx "${svc}"; then
      echo "Service running: ${svc}"
      return 0
    fi
    echo "waiting for service to be running (${svc}) retry ${i}/${tries}"
    sleep 2
  done
  echo "ERROR: service not running: ${svc}"
  dc ps || true
  return 1
}

wait_for_dag_in_metadb() {
  local dag_id="$1"
  local tries="${2:-90}"

  for i in $(seq 1 "${tries}"); do
    if dc exec -T airflow-scheduler airflow dags list -o plain 2>/dev/null \
      | awk '{print $1}' \
      | grep -qx "${dag_id}"; then
      echo "DAG ready in metadb: ${dag_id}"
      return 0
    fi
    echo "waiting for DAG to be registered in metadb (${dag_id}) retry ${i}/${tries}"
    sleep 2
  done

  echo "ERROR: DAG not registered in metadb after waiting: ${dag_id}"

  echo
  echo "== Debug: airflow dags list (DB-backed) =="
  dc exec -T airflow-scheduler airflow dags list -o table || true

  echo
  echo "== Debug: airflow dags list --local (file parse) =="
  dc exec -T airflow-scheduler airflow dags list --local -o table || true

  echo
  echo "== Debug: import errors (local + DB) =="
  dc exec -T airflow-scheduler airflow dags list-import-errors --local -o table || true
  dc exec -T airflow-scheduler airflow dags list-import-errors -o table || true

  echo
  echo "== Debug: dags folder listing (scheduler) =="
  dc exec -T airflow-scheduler bash -lc "ls -la /opt/airflow/dags || true" || true
  dc exec -T airflow-scheduler bash -lc "find /opt/airflow/dags -maxdepth 3 -type f -name '*.py' -print || true" || true

  echo
  echo "== Debug: scheduler logs (tail) =="
  dc logs --no-color --tail 200 airflow-scheduler || true

  echo
  echo "== Debug: dag-processor logs (tail) =="
  dc logs --no-color --tail 200 airflow-dag-processor || true

  return 1
}

echo
echo "== 0) Ensure stack is up =="
dc up -d --build

# Ensure core services are actually running before we do any Airflow CLI work
wait_service postgres
wait_service airflow-dag-processor
wait_service airflow-worker
wait_service airflow-apiserver
wait_service airflow-triggerer
wait_service airflow-scheduler

echo
echo "== 0.5a) Wait for Airflow DB connectivity + migrations =="
dc exec -T airflow-scheduler airflow db check --retry 30 --retry-delay 2
dc exec -T airflow-scheduler airflow db check-migrations -t 180

echo
echo "== 0.5b) Force DAG serialization into the metadb =="
dc exec -T airflow-scheduler airflow dags reserialize

echo
echo "== 0.6) Wait for required DAGs to be present in the metadb =="
wait_for_dag_in_metadb "mrp_smoke_postgres"
wait_for_dag_in_metadb "mrp_smoke_ingestion_api"
wait_for_dag_in_metadb "mrp_smoke_ingest_raw"
wait_for_dag_in_metadb "mrp_pipeline_dag"

echo
echo "== 1) Smoke checks (trigger + wait) =="
LOGICAL_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

RID_PG="manual__smoke__pg__$(date -u +%Y%m%dT%H%M%SZ)__$RANDOM"
RID_API="manual__smoke__api__$(date -u +%Y%m%dT%H%M%SZ)__$RANDOM"
RID_ING="manual__smoke__ingest__$(date -u +%Y%m%dT%H%M%SZ)__$RANDOM"

dc exec -T airflow-worker bash -lc "
  set -e
  airflow dags trigger mrp_smoke_postgres       -r '${RID_PG}'  -l '${LOGICAL_DATE}'
  airflow dags trigger mrp_smoke_ingestion_api  -r '${RID_API}' -l '${LOGICAL_DATE}'
  airflow dags trigger mrp_smoke_ingest_raw     -r '${RID_ING}' -l '${LOGICAL_DATE}'
"

dc exec -T airflow-worker bash -lc "
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
