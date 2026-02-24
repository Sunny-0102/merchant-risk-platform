from __future__ import annotations

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator

with DAG(
    dag_id="mrp_smoke_ingest_raw",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["mrp", "smoke"],
) as dag:
    HttpOperator(
        task_id="post_ingest_raw",
        http_conn_id="mrp_ingestion_api",  # comes from AIRFLOW_CONN_MRP_INGESTION_API
        endpoint="/ingest/raw",
        method="POST",
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "source": "airflow_smoke",
                "payload": {
                    "event_type": "charge",
                    "merchant_id": "m_demo_001",
                    "amount": 12.34,
                    "currency": "USD",
                    "event_ts": "2026-01-01T00:00:00Z",
                },
            }
        ),
        log_response=True,
        response_check=lambda r: r.status_code == 200,
        response_filter=lambda r: r.json(),
        do_xcom_push=True,
        extra_options={"timeout": 10},
        retries=10,
        retry_delay=timedelta(seconds=3),
    )
