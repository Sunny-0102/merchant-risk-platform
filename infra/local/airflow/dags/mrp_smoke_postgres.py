from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Uses the connection injected by env var AIRFLOW_CONN_MRP_POSTGRES
# (conn_id is "mrp_postgres" by Airflow convention).
with DAG(
    dag_id="mrp_smoke_postgres",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mrp", "smoke"],
) as dag:
    SQLExecuteQueryOperator(
        task_id="select_1",
        conn_id="mrp_postgres",
        sql="SELECT 1 AS ok;",
    )
