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

# -------- wait helpers --------

wait_for_service_running() {
  local svc="$1"
  for i in $(seq 1 90); do
    local cid running
    cid="$(compose ps -q "${svc}" 2>/dev/null || true)"
    if [[ -n "${cid}" ]]; then
      running="$(docker inspect -f '{{.State.Running}}' "${cid}" 2>/dev/null || echo "false")"
      if [[ "${running}" == "true" ]]; then
        echo "Service running: ${svc}"
        return 0
      fi
    fi
    echo "waiting for service to be running (${svc}) retry ${i}/90"
    sleep 2
  done
  echo "ERROR: service not running: ${svc}"
  compose ps || true
  return 1
}

wait_for_init_container_success() {
  local svc="$1"
  for i in $(seq 1 90); do
    local cid status exitcode
    cid="$(compose ps -q "${svc}" 2>/dev/null || true)"
    if [[ -n "${cid}" ]]; then
      status="$(docker inspect -f '{{.State.Status}}' "${cid}" 2>/dev/null || echo "")"
      exitcode="$(docker inspect -f '{{.State.ExitCode}}' "${cid}" 2>/dev/null || echo "")"

      if [[ "${status}" == "exited" && "${exitcode}" == "0" ]]; then
        echo "Init container completed OK: ${svc}"
        return 0
      fi

      if [[ "${status}" == "exited" && "${exitcode}" != "0" ]]; then
        echo "ERROR: init container exited non-zero: ${svc} (exitcode=${exitcode})"
        compose logs --no-color --tail 200 "${svc}" || true
        return 1
      fi
    fi

    echo "waiting for init container to complete (${svc}) retry ${i}/90"
    sleep 2
  done

  echo "ERROR: init container did not complete: ${svc}"
  compose ps || true
  compose logs --no-color --tail 200 "${svc}" || true
  return 1
}

wait_for_airflow_db() {
  for i in $(seq 1 90); do
    if compose exec -T airflow-scheduler airflow db check >/dev/null 2>&1; then
      # If db check passes, migrations are at least reachable/valid for the metadb.
      echo "Airflow metadb reachable (db check OK)"
      return 0
    fi
    echo "waiting for Airflow metadb connectivity/migrations retry ${i}/90"
    sleep 2
  done
  echo "ERROR: Airflow metadb not ready after waiting"
  compose logs --no-color --tail 200 airflow-scheduler || true
  return 1
}

wait_for_app_postgres() {
  for i in $(seq 1 90); do
    if compose exec -T postgres pg_isready -U app -d appdb >/dev/null 2>&1; then
      echo "App Postgres ready (pg_isready OK)"
      return 0
    fi
    echo "waiting for app postgres readiness retry ${i}/90"
    sleep 2
  done
  echo "ERROR: app postgres not ready after waiting"
  compose logs --no-color --tail 200 postgres || true
  return 1
}

wait_for_dag_in_metadb() {
  local dag_id="$1"
  for i in $(seq 1 90); do
    if compose exec -T airflow-scheduler airflow dags list 2>/dev/null \
      | awk 'NR>2 {print $1}' \
      | grep -qx "${dag_id}"; then
      echo "DAG ready in metadb: ${dag_id}"
      return 0
    fi
    echo "waiting for DAG to be registered (${dag_id}) retry ${i}/90"
    sleep 2
  done

  echo "ERROR: DAG not registered in metadb after waiting: ${dag_id}"

  echo
  echo "== Debug: airflow dags list (scheduler) =="
  compose exec -T airflow-scheduler airflow dags list || true

  echo
  echo "== Debug: import errors (scheduler) =="
  compose exec -T airflow-scheduler airflow dags list-import-errors || true

  echo
  echo "== Debug: dag-processor logs (tail) =="
  compose logs --no-color --tail 200 airflow-dag-processor || true

  echo
  echo "== Debug: scheduler logs (tail) =="
  compose logs --no-color --tail 200 airflow-scheduler || true

  return 1
}

dump_smoke_failure() {
  local dag_id="$1"
  local run_id="$2"

  echo
  echo "===================="
  echo "SMOKE FAILED: dag=${dag_id} run_id=${run_id}"
  echo "===================="
  echo

  echo "== airflow dags list-runs (worker) =="
  compose exec -T airflow-worker bash -lc \
    "airflow dags list-runs -d '${dag_id}' -o table | tail -n 60" || true

  echo
  echo "== airflow tasks list (worker) =="
  compose exec -T airflow-worker bash -lc \
    "airflow tasks list '${dag_id}' || true" || true

  echo
  echo "== airflow tasks states-for-dag-run (worker) =="
  compose exec -T airflow-worker bash -lc \
    "airflow tasks states-for-dag-run '${dag_id}' '${run_id}' -o table || true" || true

  echo
  echo "== Search task logs (worker) =="
  compose exec -T airflow-worker bash -lc "
    set -e
    LOGROOT=\${AIRFLOW_HOME:-/opt/airflow}/logs
    echo \"LOGROOT=\$LOGROOT\"
    echo \"(showing up to 50 matching log files)\"
    find \"\$LOGROOT\" -type f -path \"*${dag_id}*\" -path \"*${run_id}*\" -print | head -n 50 || true
    echo
    echo \"(tailing up to 5 matching log files)\"
    files=\$(find \"\$LOGROOT\" -type f -path \"*${dag_id}*\" -path \"*${run_id}*\" -print | head -n 5 || true)
    for f in \$files; do
      echo
      echo \"--- tail: \$f ---\"
      tail -n 200 \"\$f\" || true
    done
  " || true

  echo
  echo "== scheduler logs (tail) =="
  compose logs --no-color --tail 200 airflow-scheduler || true

  echo
  echo "== worker logs (tail) =="
  compose logs --no-color --tail 200 airflow-worker || true
}

wait_for_smoke() {
  local dag_id="$1"
  local run_id="$2"

  echo "-- waiting for ${dag_id} (run_id=${run_id})"
  for i in $(seq 1 60); do
    local st
    st="$(compose exec -T airflow-worker bash -lc \
      "airflow dags state '${dag_id}' '${run_id}' 2>/dev/null | tail -n1 | tr -d '\r' || true")"
    echo "   poll ${i} state=${st}"

    if [[ "${st}" == "success" ]]; then
      return 0
    fi

    if [[ "${st}" == "failed" ]]; then
      dump_smoke_failure "${dag_id}" "${run_id}"
      return 2
    fi

    sleep 2
  done

  echo "ERROR: timeout waiting for ${dag_id} (run_id=${run_id})"
  dump_smoke_failure "${dag_id}" "${run_id}" || true
  return 3
}

# -------- main flow --------

echo
echo "== 0) Ensure stack is up =="
compose up -d --build

# Wait for core containers
wait_for_service_running postgres
wait_for_service_running airflow-dag-processor
wait_for_service_running airflow-worker
wait_for_service_running airflow-apiserver
wait_for_service_running airflow-triggerer
wait_for_service_running airflow-scheduler
wait_for_service_running ingestion-api

# Wait for airflow init container (this is a common CI race if you trigger DAGs too early)
wait_for_init_container_success airflow-init

echo
echo "== 0.5a) Wait for Airflow DB connectivity + migrations =="
wait_for_airflow_db

echo
echo "== 0.5b) Wait for app Postgres readiness =="
wait_for_app_postgres

echo
echo "== 0.5c) Force DAG serialization into the metadb =="
compose exec -T airflow-scheduler airflow dags reserialize

echo
echo "== 0.6) Wait for required DAGs to be present in the metadb =="
wait_for_dag_in_metadb "mrp_smoke_postgres"
wait_for_dag_in_metadb "mrp_smoke_ingestion_api"
wait_for_dag_in_metadb "mrp_smoke_ingest_raw"
wait_for_dag_in_metadb "mrp_pipeline_dag"

echo
echo "== 1) Smoke checks (trigger + wait) =="

# Keep a logical_date, but poll by run_id (more deterministic) :contentReference[oaicite:1]{index=1}
LOGICAL_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

RID_PG="manual__smoke__pg__$(date -u +%Y%m%dT%H%M%SZ)__$RANDOM"
RID_API="manual__smoke__api__$(date -u +%Y%m%dT%H%M%SZ)__$RANDOM"
RID_ING="manual__smoke__ingest__$(date -u +%Y%m%dT%H%M%SZ)__$RANDOM"

echo "Smoke run_ids:"
echo "  mrp_smoke_postgres      run_id=${RID_PG}"
echo "  mrp_smoke_ingestion_api run_id=${RID_API}"
echo "  mrp_smoke_ingest_raw    run_id=${RID_ING}"
echo "  logical_date=${LOGICAL_DATE}"

compose exec -T airflow-worker bash -lc "
  set -e
  airflow dags trigger mrp_smoke_postgres      -r '${RID_PG}'  -l '${LOGICAL_DATE}'
  airflow dags trigger mrp_smoke_ingestion_api -r '${RID_API}' -l '${LOGICAL_DATE}'
  airflow dags trigger mrp_smoke_ingest_raw    -r '${RID_ING}' -l '${LOGICAL_DATE}'
"

wait_for_smoke "mrp_smoke_postgres"      "${RID_PG}"
wait_for_smoke "mrp_smoke_ingestion_api" "${RID_API}"
wait_for_smoke "mrp_smoke_ingest_raw"    "${RID_ING}"

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
