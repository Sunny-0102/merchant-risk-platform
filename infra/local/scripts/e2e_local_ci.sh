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

section() {
  echo
  echo "== $* =="
}

# Wait for airflow-init (one-shot) container to exit 0
wait_for_init_container() {
  local svc="airflow-init"
  local max=180

  for i in $(seq 1 "${max}"); do
    # -a/--all ensures we can see exited one-shot containers
    local cid
    cid="$(compose ps -a -q "${svc}" 2>/dev/null || true)"

    if [[ -n "${cid}" ]]; then
      local status exitcode
      status="$(docker inspect -f '{{.State.Status}}' "${cid}" 2>/dev/null || true)"
      exitcode="$(docker inspect -f '{{.State.ExitCode}}' "${cid}" 2>/dev/null || true)"

      if [[ "${status}" == "exited" ]]; then
        if [[ "${exitcode}" == "0" ]]; then
          echo "Init container completed: ${svc}"
          return 0
        fi

        echo "ERROR: init container exited non-zero: ${svc} (exit=${exitcode})"
        compose logs --no-color --tail 200 "${svc}" || true
        return 1
      fi
    fi

    echo "waiting for init container to complete (${svc}) retry ${i}/${max}"
    sleep 2
  done

  echo "ERROR: init container did not complete: ${svc}"
  compose ps || true
  compose logs --no-color --tail 200 "${svc}" || true
  return 1
}

wait_for_airflow_metadb() {
  local max=120
  for i in $(seq 1 "${max}"); do
    if compose exec -T airflow-scheduler bash -lc "airflow db check >/dev/null 2>&1"; then
      # Also ensure migrations are applied (Airflow 3.x has check-migrations)
      if compose exec -T airflow-scheduler bash -lc "airflow db check-migrations >/dev/null 2>&1"; then
        echo "Airflow metadb ready"
        return 0
      fi
    fi
    echo "waiting for Airflow metadb connectivity+migrations retry ${i}/${max}"
    sleep 2
  done

  echo "ERROR: Airflow metadb not ready"
  compose logs --no-color --tail 200 airflow-scheduler || true
  return 1
}

wait_for_appdb_ready() {
  local max=120
  for i in $(seq 1 "${max}"); do
    if compose exec -T postgres pg_isready -U app -d appdb >/dev/null 2>&1; then
      echo "appdb ready"
      return 0
    fi
    echo "waiting for appdb readiness retry ${i}/${max}"
    sleep 2
  done

  echo "ERROR: appdb not ready"
  compose logs --no-color --tail 200 postgres || true
  return 1
}

# This prevents a very common CI-only failure: the container is up, but schema/tables
# are not bootstrapped yet, so mrp_smoke_postgres fails instantly.
wait_for_appdb_bootstrap() {
  local max=120
  for i in $(seq 1 "${max}"); do
    if compose exec -T postgres bash -lc \
      "psql -U app -d appdb -v ON_ERROR_STOP=1 -tAc \
        \"select 1 from information_schema.tables where table_schema='mrp' and table_name='raw_events';\" \
        | tr -d '[:space:]' | grep -qx 1"; then
      echo "appdb bootstrap ready (mrp.raw_events present)"
      return 0
    fi
    echo "waiting for appdb bootstrap (mrp.raw_events) retry ${i}/${max}"
    sleep 2
  done

  echo "ERROR: appdb bootstrap not ready (mrp.raw_events missing)"
  compose exec -T postgres bash -lc "psql -U app -d appdb -c '\dn+' || true" || true
  compose exec -T postgres bash -lc "psql -U app -d appdb -c '\dt mrp.*' || true" || true
  return 1
}

force_dag_serialization() {
  # In Airflow 3.x, "reserialize" forces DagBag -> serialized_dag in metadb
  compose exec -T airflow-scheduler bash -lc "airflow dags reserialize"
}

wait_for_dag_in_metadb() {
  local dag_id="$1"
  local max=90

  for i in $(seq 1 "${max}"); do
    if compose exec -T airflow-scheduler bash -lc "airflow dags list 2>/dev/null" \
      | awk 'NR>2 {print $1}' \
      | grep -qx "${dag_id}"; then
      echo "DAG ready in metadb: ${dag_id}"
      return 0
    fi
    echo "waiting for DAG to be registered (${dag_id}) retry ${i}/${max}"
    sleep 2
  done

  echo "ERROR: DAG not registered in metadb after waiting: ${dag_id}"
  compose exec -T airflow-scheduler bash -lc "airflow dags list || true" || true
  compose exec -T airflow-scheduler bash -lc "airflow dags list-import-errors || true" || true
  return 1
}

debug_smoke_failure() {
  local dag_id="$1"
  local run_id="$2"
  local logical_date="$3"

  set +e

  section "Debug: dags list-runs (${dag_id})"
  # Airflow 3.x syntax: dag_id is positional; there is NO -d flag.  :contentReference[oaicite:4]{index=4}
  compose exec -T airflow-worker bash -lc "airflow dags list-runs -o table ${dag_id} || true" || true

  section "Debug: task states (states-for-dag-run) (${dag_id} / ${run_id})"
  # Airflow 3.x provides states-for-dag-run for task-level states. :contentReference[oaicite:5]{index=5}
  compose exec -T airflow-worker bash -lc "airflow tasks states-for-dag-run -o table ${dag_id} ${run_id} || true" || true

  section "Debug: connection(s) likely used by smoke"
  # If your smoke checks rely on a connection id like 'appdb', this will show whether it exists.
  # (If it doesn't exist, that is a direct explanation for immediate failure.)
  compose exec -T airflow-worker bash -lc "airflow connections list --conn-id appdb -o table || true" || true
  compose exec -T airflow-worker bash -lc "airflow connections get appdb -o table || true" || true

  section "Debug: recent log files (worker filesystem)"
  compose exec -T airflow-worker bash -lc \
    "if [[ -d /opt/airflow/logs ]]; then
       find /opt/airflow/logs -type f -printf '%T@ %p\n' | sort -n | tail -n 20;
     else
       echo 'No /opt/airflow/logs directory found';
     fi" || true

  section "Debug: tail last 5 log files"
  compose exec -T airflow-worker bash -lc \
    "if [[ -d /opt/airflow/logs ]]; then
       for f in \$(find /opt/airflow/logs -type f -printf '%T@ %p\n' | sort -n | tail -n 5 | awk '{print \$2}'); do
         echo \"--- \$f\";
         tail -n 200 \"\$f\" || true;
       done
     fi" || true

  section "Debug: scheduler logs (tail)"
  compose logs --no-color --tail 200 airflow-scheduler || true

  section "Debug: worker logs (tail)"
  compose logs --no-color --tail 200 airflow-worker || true

  set -e
}

wait_for_smoke_success() {
  local dag_id="$1"
  local run_id="$2"
  local logical_date="$3"

  echo "-- waiting for ${dag_id} (logical_date=${logical_date}, run_id=${run_id})"
  for i in $(seq 1 60); do
    local st
    st="$(compose exec -T airflow-worker bash -lc \
      "airflow dags state \"${dag_id}\" \"${logical_date}\" 2>/dev/null | tail -n1 | tr -d '\r' || true" \
      | tr -d '\r' || true)"
    echo "   poll ${i} state=${st}"

    if [[ "${st}" == "success" ]]; then
      return 0
    fi
    if [[ "${st}" == "failed" ]]; then
      debug_smoke_failure "${dag_id}" "${run_id}" "${logical_date}"
      return 2
    fi
    sleep 2
  done

  echo "ERROR: smoke did not finish in time: ${dag_id}"
  debug_smoke_failure "${dag_id}" "${run_id}" "${logical_date}"
  return 2
}

section "0) Ensure stack is up"
compose up -d --build

section "0.25) Wait for airflow-init to complete"
wait_for_init_container

section "0.5a) Wait for Airflow DB connectivity + migrations"
wait_for_airflow_metadb
wait_for_appdb_ready
wait_for_appdb_bootstrap

section "0.5b) Force DAG serialization into the metadb"
force_dag_serialization

section "0.6) Wait for required DAGs to be present in the metadb"
wait_for_dag_in_metadb "mrp_smoke_postgres"
wait_for_dag_in_metadb "mrp_smoke_ingestion_api"
wait_for_dag_in_metadb "mrp_smoke_ingest_raw"
wait_for_dag_in_metadb "mrp_pipeline_dag"

section "1) Smoke checks (trigger + wait)"
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

wait_for_smoke_success "mrp_smoke_postgres" "${RID_PG}" "${LOGICAL_DATE}"
wait_for_smoke_success "mrp_smoke_ingestion_api" "${RID_API}" "${LOGICAL_DATE}"
wait_for_smoke_success "mrp_smoke_ingest_raw" "${RID_ING}" "${LOGICAL_DATE}"

section "2) Run E2E harness"
bash scripts/mrp_e2e.sh

section "3) Run dedup + idempotency checks"
bash scripts/mrp_dedup.sh
bash scripts/mrp_idempotency.sh

section "4) Bucket trigger sanity"
bash scripts/mrp_trigger_bucket.sh

echo
echo "✅ DONE: local CI checks completed"
