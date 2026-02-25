from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator

with DAG(
    dag_id="mrp_smoke_ingestion_api",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mrp", "smoke"],
) as dag:
    HttpOperator(
        task_id="get_healthz",
        http_conn_id="mrp_ingestion_api",
        endpoint="/healthz",
        method="GET",
        log_response=True,
        response_check=lambda r: r.status_code == 200,
        retries=10,
        retry_delay=timedelta(seconds=3),
        extra_options={"timeout": 10},
    )
