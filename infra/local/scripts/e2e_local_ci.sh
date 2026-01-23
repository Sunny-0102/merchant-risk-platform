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

# --- generic waits -----------------------------------------------------------

wait_for_service_running() {
  local svc="$1"
  for i in $(seq 1 90); do
    if compose ps -q "${svc}" >/dev/null 2>&1 && [[ -n "$(compose ps -q "${svc}" 2>/dev/null | head -n1 || true)" ]]; then
      echo "Service running: ${svc}"
      return 0
    fi
    echo "waiting for service to be running (${svc}) retry ${i}/90"
    sleep 2
  done
  echo "ERROR: service not running: ${svc}"
  compose ps || true
  compose logs --no-color --tail 200 "${svc}" || true
  return 1
}

# IMPORTANT: docker compose ps shows only RUNNING containers by default.
# Use --all so we can detect init containers that EXITED successfully.
wait_for_init_container_success() {
  local svc="$1"
  for i in $(seq 1 90); do
    local cid
    cid="$(compose ps --all -q "${svc}" 2>/dev/null | head -n1 || true)"
    if [[ -n "${cid}" ]]; then
      local status exit_code
      status="$(docker inspect -f '{{.State.Status}}' "${cid}" 2>/dev/null || true)"
      exit_code="$(docker inspect -f '{{.State.ExitCode}}' "${cid}" 2>/dev/null || true)"

      if [[ "${status}" == "exited" && "${exit_code}" == "0" ]]; then
        echo "Init container completed: ${svc}"
        return 0
      fi

      if [[ "${status}" == "exited" && "${exit_code}" != "0" ]]; then
        echo "ERROR: init container exited non-zero: ${svc} (exit_code=${exit_code})"
        compose logs --no-color --tail 300 "${svc}" || true
        return 1
      fi
    fi

    echo "waiting for init container to complete (${svc}) retry ${i}/90"
    sleep 2
  done

  echo "ERROR: init container did not complete: ${svc}"
  compose ps --all || true
  compose logs --no-color --tail 300 "${svc}" || true
  return 1
}

wait_for_airflow_metadb() {
  # Airflow 3.x: "airflow db check" validates connectivity and migration state.
  for i in $(seq 1 90); do
    if compose exec -T airflow-scheduler airflow db check >/dev/null 2>&1; then
      echo "Airflow metadb ready"
      return 0
    fi
    echo "waiting for Airflow metadb readiness retry ${i}/90"
    sleep 2
  done
  echo "ERROR: Airflow metadb not ready"
  compose logs --no-color --tail 300 airflow-scheduler || true
  return 1
}

wait_for_appdb() {
  # Optional but helps prevent flaky mrp_smoke_postgres failures if it uses appdb.
  # Assumes appdb/app user/password are present (as used by the rest of the repo scripts).
  for i in $(seq 1 90); do
    if compose exec -T postgres bash -lc "PGPASSWORD=app psql -U app -d appdb -c 'select 1' >/dev/null 2>&1"; then
      echo "appdb ready"
      return 0
    fi
    echo "waiting for appdb readiness retry ${i}/90"
    sleep 2
  done
  echo "WARN: appdb readiness check did not pass (continuing anyway)"
  return 0
}

force_dag_reserialize() {
  echo
  echo "== Force DAG serialization into the metadb =="
  # Best-effort: if this fails, we want debuggability rather than a silent failure.
  if ! compose exec -T airflow-scheduler airflow dags reserialize; then
    echo "ERROR: airflow dags reserialize failed"
    compose exec -T airflow-scheduler airflow dags list-import-errors || true
    compose logs --no-color --tail 300 airflow-dag-processor || true
    compose logs --no-color --tail 300 airflow-scheduler || true
    return 1
  fi
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
  compose logs --no-color --tail 300 airflow-dag-processor || true

  echo
  echo "== Debug: scheduler logs (tail) =="
  compose logs --no-color --tail 300 airflow-scheduler || true

  return 1
}

# --- main -------------------------------------------------------------------

echo
echo "== 0) Ensure stack is up =="
compose up -d --build

# Wait for core services (reduce flakiness in CI runners)
wait_for_service_running postgres
wait_for_service_running airflow-dag-processor
wait_for_service_running airflow-scheduler
wait_for_service_running airflow-worker
wait_for_service_running airflow-apiserver
wait_for_service_running airflow-triggerer
wait_for_service_running ingestion-api

# airflow-init is a one-shot container; it often exits quickly.
# Use --all to detect it even after it has exited.
wait_for_init_container_success airflow-init

echo
echo "== 0.5a) Wait for Airflow DB connectivity + migrations =="
wait_for_airflow_metadb

# Optional but recommended (helps mrp_smoke_postgres be stable)
wait_for_appdb

echo
echo "== 0.5b) Force DAG serialization into the metadb =="
force_dag_reserialize

echo
echo "== 0.6) Wait for required DAGs to be present in the metadb =="
wait_for_dag_in_metadb "mrp_smoke_postgres"
wait_for_dag_in_metadb "mrp_smoke_ingestion_api"
wait_for_dag_in_metadb "mrp_smoke_ingest_raw"
wait_for_dag_in_metadb "mrp_pipeline_dag"

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
      if [[ \"\$st\" == \"failed\" ]]; then
        echo \"== Debug: list-runs (\$dag) ==\"
        airflow dags list-runs -d \"\$dag\" | tail -n 50 || true
        exit 2
      fi
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
