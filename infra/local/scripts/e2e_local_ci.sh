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

# ---------- helpers ----------
log_step() {
  echo
  echo "== $* =="
}

pg_scalar() {
  # Usage: pg_scalar "select 1"
  compose exec -T -e PGPASSWORD=app postgres \
    psql -U app -d appdb -v ON_ERROR_STOP=1 -tAc "$1" | tr -d '[:space:]'
}

wait_for_airflow_init() {
  log_step "0.25) Wait for airflow-init to complete"
  for i in $(seq 1 180); do
    cid="$(compose ps -q airflow-init 2>/dev/null || true)"
    if [[ -z "${cid}" ]]; then
      # Some environments don't keep the init container around; don't hard fail here.
      echo "No airflow-init container found via compose; continuing"
      return 0
    fi

    status="$(docker inspect -f '{{.State.Status}}' "${cid}" 2>/dev/null || true)"
    exit_code="$(docker inspect -f '{{.State.ExitCode}}' "${cid}" 2>/dev/null || echo 1)"

    if [[ "${status}" == "exited" && "${exit_code}" == "0" ]]; then
      echo "Init container completed: airflow-init"
      return 0
    fi

    echo "waiting for init container to complete (airflow-init) retry ${i}/180"
    sleep 2
  done

  echo "ERROR: init container did not complete: airflow-init"
  compose ps || true
  compose logs --no-color --tail 200 airflow-init || true
  return 1
}

wait_for_airflow_metadb() {
  log_step "0.5a) Wait for Airflow DB connectivity + migrations"
  for i in $(seq 1 120); do
    if compose exec -T airflow-scheduler airflow db check >/dev/null 2>&1; then
      echo "Airflow metadb ready"
      return 0
    fi
    echo "waiting for Airflow metadb (airflow db check) retry ${i}/120"
    sleep 2
  done

  echo "ERROR: Airflow metadb not ready"
  compose logs --no-color --tail 200 airflow-scheduler || true
  return 1
}

wait_for_appdb() {
  log_step "0.5a) Wait for appdb connectivity"
  for i in $(seq 1 120); do
    # Fast reachability check
    if compose exec -T postgres pg_isready -U app -d appdb >/dev/null 2>&1; then
      # Actual query check
      if pg_scalar "select 1" >/dev/null 2>&1; then
        echo "appdb ready"
        return 0
      fi
    fi
    echo "waiting for appdb (psql) retry ${i}/120"
    sleep 2
  done

  echo "ERROR: appdb not reachable"
  compose logs --no-color --tail 200 postgres || true
  return 1
}

ensure_raw_events_compat_view() {
  # If mrp.raw_events is missing but raw_realtime_events exists, create a simple updatable view alias.
  compose exec -T -e PGPASSWORD=app postgres psql -U app -d appdb -v ON_ERROR_STOP=1 -c "
DO \$\$
BEGIN
  IF to_regclass('mrp.raw_events') IS NULL
     AND to_regclass('mrp.raw_realtime_events') IS NOT NULL
  THEN
    EXECUTE 'CREATE OR REPLACE VIEW mrp.raw_events AS SELECT * FROM mrp.raw_realtime_events';
  END IF;
END
\$\$;
" >/dev/null
}

wait_for_appdb_bootstrap() {
  log_step "0.5a) Wait for appdb bootstrap objects"
  for i in $(seq 1 120); do
    # Accept either table name; then ensure mrp.raw_events exists (view) for backward compatibility.
    raw_obj="$(pg_scalar "select coalesce(to_regclass('mrp.raw_events')::text, to_regclass('mrp.raw_realtime_events')::text, '')" || true)"
    if [[ -n "${raw_obj}" ]]; then
      ensure_raw_events_compat_view || true

      # Validate key objects exist (adjust if you add/remove core tables)
      fact_ok="$(pg_scalar "select coalesce(to_regclass('mrp.fact_payment_events')::text,'')" || true)"
      snap_ok="$(pg_scalar "select coalesce(to_regclass('mrp.merchant_feature_snapshots')::text,'')" || true)"
      dim_ok="$(pg_scalar "select coalesce(to_regclass('mrp.dim_merchant')::text,'')" || true)"

      if [[ -n "${fact_ok}" && -n "${snap_ok}" && -n "${dim_ok}" ]]; then
        echo "appdb bootstrap ready (raw=${raw_obj})"
        return 0
      fi
    fi

    echo "waiting for appdb bootstrap (mrp.raw_events or mrp.raw_realtime_events) retry ${i}/120"
    sleep 2
  done

  echo "ERROR: appdb bootstrap not ready"
  # Diagnostics
  compose exec -T -e PGPASSWORD=app postgres psql -U app -d appdb -c "\dn+" || true
  compose exec -T -e PGPASSWORD=app postgres psql -U app -d appdb -c "\dt mrp.*" || true
  compose exec -T -e PGPASSWORD=app postgres psql -U app -d appdb -c "\dv mrp.*" || true
  return 1
}

force_dag_serialize() {
  log_step "0.5b) Force DAG serialization into the metadb"
  compose exec -T airflow-scheduler airflow dags reserialize
}

wait_for_dag() {
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
  echo "== Debug: dags folder contents (dag-processor) =="
  compose exec -T airflow-dag-processor bash -lc "ls -la /opt/airflow/dags || true"
  compose exec -T airflow-dag-processor bash -lc "find /opt/airflow/dags -maxdepth 3 -type f -name '*.py' -print || true"

  echo
  echo "== Debug: import errors (dag-processor) =="
  compose exec -T airflow-dag-processor airflow dags list-import-errors || true

  echo
  echo "== Debug: dag-processor logs (tail) =="
  compose logs --no-color --tail 200 airflow-dag-processor || true

  echo
  echo "== Debug: scheduler logs (tail) =="
  compose logs --no-color --tail 200 airflow-scheduler || true

  return 1
}

debug_failed_smoke() {
  local dag_id="$1"
  local logical_date="$2"
  echo
  echo "== Debug: smoke failure for ${dag_id} logical_date=${logical_date} =="

  echo
  echo "-- dags list-runs (Airflow 3: dag_id is positional)"
  compose exec -T airflow-worker bash -lc "airflow dags list-runs '${dag_id}' -o table || true"

  echo
  echo "-- import errors"
  compose exec -T airflow-dag-processor airflow dags list-import-errors || true

  echo
  echo "-- scheduler logs (tail)"
  compose logs --no-color --tail 200 airflow-scheduler || true

  echo
  echo "-- worker logs (tail)"
  compose logs --no-color --tail 200 airflow-worker || true
}

# ---------- main ----------
log_step "0) Ensure stack is up"
compose up -d --build

wait_for_airflow_init
wait_for_airflow_metadb
wait_for_appdb
wait_for_appdb_bootstrap

force_dag_serialize

log_step "0.6) Wait for required DAGs to be present in the metadb"
wait_for_dag "mrp_smoke_postgres"
wait_for_dag "mrp_smoke_ingestion_api"
wait_for_dag "mrp_smoke_ingest_raw"
wait_for_dag "mrp_pipeline_dag"

log_step "1) Smoke checks (trigger + wait)"
LOGICAL_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

RID_PG="manual__smoke__pg__$(date -u +%Y%m%dT%H%M%SZ)__$RANDOM"
RID_API="manual__smoke__api__$(date -u +%Y%m%dT%H%M%SZ)__$RANDOM"
RID_ING="manual__smoke__ingest__$(date -u +%Y%m%dT%H%M%SZ)__$RANDOM"

compose exec -T airflow-worker bash -lc "
  set -e
  airflow dags trigger mrp_smoke_postgres       -r '${RID_PG}'  -l '${LOGICAL_DATE}'
  airflow dags trigger mrp_smoke_ingestion_api  -r '${RID_API}' -l '${LOGICAL_DATE}'
  airflow dags trigger mrp_smoke_ingest_raw     -r '${RID_ING}' -l '${LOGICAL_DATE}'
"

compose exec -T airflow-worker bash -lc "
  set -e
  for dag in mrp_smoke_postgres mrp_smoke_ingestion_api mrp_smoke_ingest_raw; do
    echo \"-- waiting for \$dag (logical_date=${LOGICAL_DATE})\"
    for i in \$(seq 1 60); do
      st=\$(airflow dags state \"\$dag\" '${LOGICAL_DATE}' 2>/dev/null | tail -n1 | tr -d '\r' || true)
      echo \"   poll \$i state=\$st\"
      if [[ \"\$st\" == \"success\" ]]; then break; fi
      if [[ \"\$st\" == \"failed\" ]]; then
        exit 2
      fi
      sleep 2
    done
  done
" || {
  # Identify which smoke failed quickly for debug.
  for dag in mrp_smoke_postgres mrp_smoke_ingestion_api mrp_smoke_ingest_raw; do
    st="$(compose exec -T airflow-worker bash -lc "airflow dags state '${dag}' '${LOGICAL_DATE}' 2>/dev/null | tail -n1 | tr -d '\r' || true")"
    if [[ "${st}" == "failed" ]]; then
      debug_failed_smoke "${dag}" "${LOGICAL_DATE}"
      break
    fi
  done
  exit 2
}

log_step "2) Run E2E harness"
bash scripts/mrp_e2e.sh

log_step "3) Run dedup + idempotency checks"
bash scripts/mrp_dedup.sh
bash scripts/mrp_idempotency.sh

log_step "4) Bucket trigger sanity"
bash scripts/mrp_trigger_bucket.sh

echo
echo "✅ DONE: local CI checks completed"
