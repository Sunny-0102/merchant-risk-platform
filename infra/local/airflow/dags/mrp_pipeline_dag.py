import csv
import importlib.util
from pathlib import Path
import subprocess

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

DEFAULT_SOURCE = "SYNTH_LIVE"
ALLOWED_SOURCES = {"SYNTH_LIVE"}
BATCH_SIZE = 20000

def resolve_source(context) -> str:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    src = conf.get("source", DEFAULT_SOURCE)
    if src not in ALLOWED_SOURCES:
        raise ValueError(f"Unsupported source={src}. Allowed: {sorted(ALLOWED_SOURCES)}")
    return src


def has_new_raw(**context) -> bool:
    """
    Guard: only run expensive steps if there are new raw rows beyond the watermark.
    Also pushes debug numbers into XCom for quick inspection.
    """
    hook = PostgresHook(postgres_conn_id="mrp_postgres")

    source = resolve_source(context)

    row = hook.get_first(
        """
        WITH w AS (
          SELECT
            COALESCE((SELECT last_raw_id
                      FROM mrp.raw_ingest_watermark
                      WHERE source = %(source)s), 0) AS last_raw_id,
            COALESCE((SELECT MAX(id)
                      FROM mrp.raw_realtime_events
                      WHERE source = %(source)s), 0) AS max_raw_id
        )
        SELECT
          (max_raw_id > last_raw_id) AS has_new,
          (max_raw_id - last_raw_id) AS new_rows,
          max_raw_id,
          last_raw_id
        FROM w;
        """,
        parameters={"source": source},
    )

    has_new, new_rows, max_raw_id, last_raw_id = row
    ti = context["ti"]
    ti.xcom_push(key="new_rows", value=int(new_rows))
    ti.xcom_push(key="max_raw_id", value=int(max_raw_id))
    ti.xcom_push(key="last_raw_id", value=int(last_raw_id))
    ti.xcom_push(key="source", value=str(source))

    return bool(has_new)

EXPORT_ROOT = Path("/workspace/data/training_exports")
TRAINING_EXPORT_FILENAME = "risk_training_dataset_latest.csv"
LOCAL_MODEL_PATH = Path("/workspace/data/models/risk_model_latest.pkl")
LOCAL_SCORING_SCRIPT_PATH = Path("/workspace/scripts/score_local_risk_model.py")

def export_training_dataset_csv(**context) -> str:
    hook = PostgresHook(postgres_conn_id="mrp_postgres")

    interval_end_utc = (
        context.get("data_interval_end")
        or context.get("logical_date")
        or pendulum.now("UTC")
    )
    snapshot_start_utc = interval_end_utc - pendulum.duration(days=8)
    snapshot_end_utc = interval_end_utc - pendulum.duration(hours=24)

    rows = hook.get_records(
        """
        SELECT *
        FROM mrp.export_risk_training_dataset(
          %(snapshot_start_utc)s::timestamptz,
          %(snapshot_end_utc)s::timestamptz
        )
        ORDER BY snapshot_time_utc, merchant_id
        """,
        parameters={
            "snapshot_start_utc": snapshot_start_utc.to_iso8601_string(),
            "snapshot_end_utc": snapshot_end_utc.to_iso8601_string(),
        },
    )

    EXPORT_ROOT.mkdir(parents=True, exist_ok=True)
    out_path = EXPORT_ROOT / TRAINING_EXPORT_FILENAME

    fieldnames = [
        "merchant_id",
        "snapshot_time_utc",
        "computed_at_utc",
        "txn_count_15m",
        "gmv_usd_15m",
        "txn_count_1h",
        "gmv_usd_1h",
        "risk_score_v1",
        "risk_band_v1",
        "label_bad_outcome_24h",
        "label_anomaly_24h",
        "label_dispute_lost_24h",
    ]

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(fieldnames)
        writer.writerows(rows)

    context["ti"].xcom_push(key="exported_rows", value=len(rows))
    return str(out_path)

def has_exported_training_rows(**context) -> bool:
    exported_rows = context["ti"].xcom_pull(
        task_ids="export_risk_training_dataset",
        key="exported_rows",
    )
    return int(exported_rows or 0) > 0


def has_local_model_artifact_exists(**context) -> bool:
    return LOCAL_MODEL_PATH.exists()


def load_local_scoring_module():
    if not LOCAL_SCORING_SCRIPT_PATH.exists():
        raise FileNotFoundError(f"Local scoring script not found: {LOCAL_SCORING_SCRIPT_PATH}")

    spec = importlib.util.spec_from_file_location(
        "score_local_risk_model",
        LOCAL_SCORING_SCRIPT_PATH,
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load scoring module from: {LOCAL_SCORING_SCRIPT_PATH}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def persist_local_model_scores(**context) -> int:
    if not LOCAL_MODEL_PATH.exists():
        raise FileNotFoundError(f"Local model artifact not found: {LOCAL_MODEL_PATH}")

    scoring_module = load_local_scoring_module()
    model = scoring_module.load_model(LOCAL_MODEL_PATH)
    hook = PostgresHook(postgres_conn_id="mrp_postgres")

    interval_end_utc = (
        context.get("data_interval_end")
        or context.get("logical_date")
        or pendulum.now("UTC")
    )
    score_window_end_utc = (
        interval_end_utc.subtract(microseconds=1)
        .start_of("minute")
        .replace(minute=(interval_end_utc.subtract(microseconds=1).minute // 15) * 15, second=0, microsecond=0)
        .add(minutes=15)
    )
    score_window_start_utc = score_window_end_utc.subtract(minutes=75)

    rows = hook.get_records(
        """
        SELECT
          merchant_id,
          snapshot_time_utc,
          txn_count_15m,
          gmv_usd_15m,
          txn_count_1h,
          gmv_usd_1h,
          risk_score_v1
        FROM mrp.merchant_feature_snapshots
        WHERE snapshot_time_utc BETWEEN %(score_window_start_utc)s::timestamptz
                                    AND %(score_window_end_utc)s::timestamptz
        ORDER BY snapshot_time_utc, merchant_id
        """,
        parameters={
            "score_window_start_utc": score_window_start_utc.to_iso8601_string(),
            "score_window_end_utc": score_window_end_utc.to_iso8601_string(),
        },
    )

    if not rows:
        return 0

    scored_at_utc = pendulum.now("UTC").to_iso8601_string()
    updates = []
    for (
        merchant_id,
        snapshot_time_utc,
        txn_count_15m,
        gmv_usd_15m,
        txn_count_1h,
        gmv_usd_1h,
        risk_score_v1,
    ) in rows:
        raw_row = {
            "txn_count_15m": txn_count_15m,
            "gmv_usd_15m": gmv_usd_15m,
            "txn_count_1h": txn_count_1h,
            "gmv_usd_1h": gmv_usd_1h,
            "risk_score_v1": risk_score_v1,
        }
        score, band = scoring_module.score_row(model, raw_row)
        updates.append(
            (
                round(score, 5),
                band,
                LOCAL_MODEL_PATH.name,
                scored_at_utc,
                merchant_id,
                snapshot_time_utc,
            )
        )

    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.executemany(
                """
                UPDATE mrp.merchant_feature_snapshots
                SET
                  local_model_score = %s,
                  local_model_band = %s,
                  local_model_version = %s,
                  local_model_scored_at_utc = %s::timestamptz
                WHERE merchant_id = %s
                  AND snapshot_time_utc = %s
                """,
                updates,
            )
        conn.commit()
    finally:
        conn.close()

    return len(updates)


def train_local_risk_model(**context) -> str:
    result = subprocess.run(
        ["python", "/workspace/scripts/train_local_risk_model.py"],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


with DAG(
    dag_id="mrp_pipeline_dag",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="*/15 * * * *",  # every 15 minutes
    catchup=False,
    max_active_runs=1,        # prevent overlaps
    tags=["mrp", "pipeline"],
    default_args={"retries": 0},
) as dag:

    gate_new_data = ShortCircuitOperator(
        task_id="gate_new_raw_events",
        python_callable=has_new_raw,
    )

    process_raw = SQLExecuteQueryOperator(
        task_id="process_raw_events",
        conn_id="mrp_postgres",
        sql="""
        SELECT * FROM mrp.process_raw_events(
          '{{ ti.xcom_pull(task_ids="gate_new_raw_events", key="source") }}',
          {{ params.batch_size }}
        );
        """,
        params={"batch_size": BATCH_SIZE},
    )


    # IMPORTANT: compute the bucket for the scheduled interval, not "now()"
    recompute_snapshot = SQLExecuteQueryOperator(
        task_id="recompute_feature_snapshot",
        conn_id="mrp_postgres",
        sql="""
        WITH params AS (
            SELECT
                date_bin(
                    '15 minutes',
                    {% if data_interval_end is defined and data_interval_end %}
                    '{{ data_interval_end | ts }}'::timestamptz
                    {% elif logical_date is defined and logical_date %}
                    '{{ logical_date | ts }}'::timestamptz
                    {% else %}
                    now()
                    {% endif %} - interval '1 microsecond',
                    '1970-01-01'::timestamptz
                ) + interval '15 minutes' AS bucket_end
            ),
            buckets AS (
            SELECT bucket_end - (i * interval '15 minutes') AS snapshot_time_utc
            FROM params, generate_series(0, 5) AS i   -- last 6 buckets = 90 minutes
            )
            SELECT COALESCE(
            SUM(mrp.recompute_feature_snapshot(snapshot_time_utc))::bigint,
            0
            ) AS rows_upserted
            FROM buckets;

        """,
        show_return_value_in_logs=True
    )

    export_risk_training_dataset = PythonOperator(
        task_id="export_risk_training_dataset",
        python_callable=export_training_dataset_csv,
    )

    gate_training_dataset_rows = ShortCircuitOperator(
        task_id="gate_training_dataset_rows",
        python_callable=has_exported_training_rows,
    )

    train_local_risk_model = PythonOperator(
        task_id="train_local_risk_model",
        python_callable=train_local_risk_model,
    )

    gate_local_model_artifact_exists = ShortCircuitOperator(
        task_id="gate_local_model_artifact_exists",
        python_callable=has_local_model_artifact_exists,
    )

    persist_local_model_scores = PythonOperator(
        task_id="persist_local_model_scores",
        python_callable=persist_local_model_scores,
    )

    gate_new_data >> process_raw >> recompute_snapshot
    recompute_snapshot >> export_risk_training_dataset >> gate_training_dataset_rows >> train_local_risk_model
    recompute_snapshot >> gate_local_model_artifact_exists >> persist_local_model_scores
