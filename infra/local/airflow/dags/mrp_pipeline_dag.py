import pendulum

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

SOURCE = "SYNTH_LIVE"
BATCH_SIZE = 20000

def has_new_raw(**context) -> bool:
    """
    Guard: only run expensive steps if there are new raw rows beyond the watermark.
    Also pushes debug numbers into XCom for quick inspection.
    """
    hook = PostgresHook(postgres_conn_id="mrp_postgres")

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
        parameters={"source": SOURCE},
    )

    has_new, new_rows, max_raw_id, last_raw_id = row
    ti = context["ti"]
    ti.xcom_push(key="new_rows", value=int(new_rows))
    ti.xcom_push(key="max_raw_id", value=int(max_raw_id))
    ti.xcom_push(key="last_raw_id", value=int(last_raw_id))

    return bool(has_new)


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
        SELECT * FROM mrp.process_raw_events(%(source)s, %(batch_size)s);
        """,
        parameters={"source": SOURCE, "batch_size": BATCH_SIZE},
    )


    # IMPORTANT: compute the bucket for the scheduled interval, not "now()"
    recompute_snapshot = SQLExecuteQueryOperator(
        task_id="recompute_feature_snapshot",
        conn_id="mrp_postgres",
        sql="""
        SELECT mrp.recompute_feature_snapshot(
        date_bin(
            '15 minutes',
            '{{ dag_run.logical_date | default(dag_run.start_date, true) }}'::timestamptz,
            '1970-01-01'::timestamptz
        )
        ) AS rows_upserted;
        """,

    )


    gate_new_data >> process_raw >> recompute_snapshot
