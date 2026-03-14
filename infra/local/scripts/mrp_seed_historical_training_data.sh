#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${LOCAL_DIR}/../.." && pwd)"

PYTHON_BIN="${PYTHON_BIN:-${REPO_ROOT}/.venv311/bin/python}"
if [ ! -x "${PYTHON_BIN}" ]; then
  PYTHON_BIN="${PYTHON_BIN_FALLBACK:-python3}"
fi

MERCHANTS="${MERCHANTS:-50}"
EVENTS="${EVENTS:-12000}"
HISTORY_DAYS="${HISTORY_DAYS:-45}"
SEED="${SEED:-42}"

GENERATED_DIR="${REPO_ROOT}/data/generated/historical_training_seed"
FACT_CSV="${GENERATED_DIR}/fact_payment_events.csv"
TRAINING_EXPORT_DIR="${REPO_ROOT}/data/training_exports"
TRAINING_EXPORT_PATH="${TRAINING_EXPORT_DIR}/risk_training_dataset_latest.csv"
LOCAL_MODEL_PATH="${REPO_ROOT}/data/models/risk_model_latest.pkl"

compose() {
  (
    cd "${LOCAL_DIR}" && \
    docker compose -p local \
      -f docker-compose.yml \
      -f docker-compose.airflow.yml \
      -f docker-compose.airflow.unpause.yml \
      "$@"
  )
}

pg_exec() {
  compose exec -T -e PGPASSWORD=app postgres \
    psql -U app -d appdb -v ON_ERROR_STOP=1 "$@"
}

echo "== 1) Ensure local postgres is up =="
compose up -d postgres >/dev/null

for i in $(seq 1 60); do
  if pg_exec -tAc "SELECT 1" >/dev/null 2>&1; then
    echo "postgres_ready=yes"
    break
  fi
  echo "waiting for postgres ${i}/60"
  sleep 2
done

echo
echo "== 2) Generate deterministic historical synthetic fact data =="
mkdir -p "${GENERATED_DIR}" "${TRAINING_EXPORT_DIR}"

"${PYTHON_BIN}" "${REPO_ROOT}/scripts/generate_synthetic_data.py" \
  --out "${GENERATED_DIR}" \
  --merchants "${MERCHANTS}" \
  --events "${EVENTS}" \
  --history-days "${HISTORY_DAYS}" \
  --seed "${SEED}"

if [ ! -f "${FACT_CSV}" ]; then
  echo "ERROR: expected fact CSV not found: ${FACT_CSV}"
  exit 1
fi

echo "fact_csv=${FACT_CSV}"
wc -l "${FACT_CSV}"

echo
echo "== 3) Reset local fact/snapshot tables (destructive to local derived history only) =="
pg_exec <<'SQL'
TRUNCATE TABLE mrp.merchant_feature_snapshots;
TRUNCATE TABLE mrp.fact_payment_events;
SQL

echo
echo "== 4) Load fact_payment_events.csv into local postgres =="
pg_exec <<'SQL'
DROP TABLE IF EXISTS mrp.historical_fact_seed_stage;
CREATE TABLE mrp.historical_fact_seed_stage (
  event_id TEXT,
  event_type TEXT,
  event_time_utc TIMESTAMPTZ,
  merchant_id TEXT,
  order_id TEXT,
  status TEXT,
  amount_usd NUMERIC,
  payer_id_hash TEXT,
  merchant_country TEXT,
  merchant_mcc TEXT,
  source_system TEXT,
  ingested_at_utc TIMESTAMPTZ,
  label_bad_outcome INTEGER,
  drift_regime_id INTEGER
);
SQL

cat "${FACT_CSV}" | pg_exec -c "
COPY mrp.historical_fact_seed_stage (
  event_id,
  event_type,
  event_time_utc,
  merchant_id,
  order_id,
  status,
  amount_usd,
  payer_id_hash,
  merchant_country,
  merchant_mcc,
  source_system,
  ingested_at_utc,
  label_bad_outcome,
  drift_regime_id
)
FROM STDIN WITH (FORMAT csv, HEADER true);
"

pg_exec <<'SQL'
INSERT INTO mrp.fact_payment_events (
  event_id,
  event_type,
  event_time_utc,
  ingested_at_utc,
  merchant_id,
  order_id,
  amount_usd,
  status,
  label_bad_outcome,
  label_anomaly,
  label_dispute_lost,
  raw_id,
  source,
  received_at,
  payload
)
SELECT
  s.event_id,
  s.event_type,
  s.event_time_utc,
  s.event_time_utc,
  s.merchant_id,
  s.order_id,
  s.amount_usd,
  s.status,
  COALESCE(s.label_bad_outcome, 0),
  0 AS label_anomaly,
  0 AS label_dispute_lost,
  ROW_NUMBER() OVER (ORDER BY s.event_time_utc, s.event_id)::bigint AS raw_id,
  'SYNTHETIC_HISTORICAL' AS source,
  s.event_time_utc AS received_at,
  jsonb_build_object(
    'event_id', s.event_id,
    'event_type', s.event_type,
    'event_time_utc', s.event_time_utc,
    'merchant_id', s.merchant_id,
    'order_id', s.order_id,
    'status', s.status,
    'amount_usd', s.amount_usd,
    'payer_id_hash', s.payer_id_hash,
    'merchant_country', s.merchant_country,
    'merchant_mcc', s.merchant_mcc,
    'source_system', s.source_system,
    'ingested_at_utc', s.event_time_utc,
    'label_bad_outcome', COALESCE(s.label_bad_outcome, 0),
    'drift_regime_id', s.drift_regime_id
  ) AS payload
FROM mrp.historical_fact_seed_stage s;
SQL

echo
echo "== 5) Recompute historical merchant_feature_snapshots =="
pg_exec -P pager=off <<'SQL'
WITH bounds AS (
  SELECT
    date_bin('15 minutes', MIN(event_time_utc), '1970-01-01'::timestamptz) + interval '15 minutes' AS bucket_start,
    date_bin('15 minutes', MAX(event_time_utc), '1970-01-01'::timestamptz) + interval '15 minutes' AS bucket_end
  FROM mrp.fact_payment_events
),
buckets AS (
  SELECT generate_series(bucket_start, bucket_end, interval '15 minutes') AS snapshot_time_utc
  FROM bounds
)
SELECT
  COUNT(*) AS bucket_count,
  COALESCE(SUM(mrp.recompute_feature_snapshot(snapshot_time_utc))::bigint, 0) AS snapshot_rows_upserted
FROM buckets;
SQL

echo
echo "== 6) Export labeled training dataset =="
pg_exec --csv -c "
COPY (
  WITH bounds AS (
    SELECT
      GREATEST(
        date_bin('15 minutes', MIN(event_time_utc), '1970-01-01'::timestamptz) + interval '15 minutes',
        now() - interval '30 days'
      ) AS snapshot_start_utc,
      LEAST(
        date_bin('15 minutes', MAX(event_time_utc), '1970-01-01'::timestamptz) + interval '15 minutes',
        now() - interval '24 hours'
      ) AS snapshot_end_utc
    FROM mrp.fact_payment_events
  )
  SELECT exported.*
  FROM bounds b
  CROSS JOIN LATERAL mrp.export_risk_training_dataset(
    b.snapshot_start_utc::timestamptz,
    b.snapshot_end_utc::timestamptz
  ) AS exported
  ORDER BY exported.snapshot_time_utc, exported.merchant_id
) TO STDOUT WITH CSV HEADER;
" > "${TRAINING_EXPORT_PATH}"

echo "training_export_path=${TRAINING_EXPORT_PATH}"
wc -l "${TRAINING_EXPORT_PATH}"

echo
echo "== 7) Coverage summary =="
pg_exec -P pager=off <<'SQL'
SELECT COUNT(*) AS fact_rows FROM mrp.fact_payment_events;
SELECT COUNT(*) AS snapshot_rows FROM mrp.merchant_feature_snapshots;
SELECT COUNT(*) AS exported_rows
FROM mrp.export_risk_training_dataset(
  now() - interval '30 days',
  now() - interval '24 hours'
);
SQL

if [ "$(wc -l < "${TRAINING_EXPORT_PATH}")" -le 1 ]; then
  echo "ERROR: training export is still header-only: ${TRAINING_EXPORT_PATH}"
  exit 1
fi

echo
echo "== 8) Train local model artifact from exported dataset =="
"${REPO_ROOT}/.venv311/bin/python" "${REPO_ROOT}/scripts/train_local_risk_model.py"

echo
echo
echo "✅ Historical training data seeded successfully"
echo "fact_csv=${FACT_CSV}"
echo "training_export_path=${TRAINING_EXPORT_PATH}"
