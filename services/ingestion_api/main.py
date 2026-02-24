from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from typing import Any, Dict
import boto3
from botocore.exceptions import ClientError

import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://app:app@postgres:5432/appdb")


def db_conn() -> psycopg.Connection[dict[str, Any]]:
    return psycopg.connect(
        DATABASE_URL,
        autocommit=True,
        row_factory=dict_row,  # type: ignore[arg-type]
    )


def ensure_schema() -> None:
    ddl = [
        "CREATE SCHEMA IF NOT EXISTS mrp;",
        """
        CREATE TABLE IF NOT EXISTS mrp.raw_realtime_events (
          id BIGSERIAL PRIMARY KEY,
          source TEXT NOT NULL,
          payload JSONB NOT NULL,
          received_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """,
        "ALTER TABLE mrp.raw_realtime_events ADD COLUMN IF NOT EXISTS received_at TIMESTAMPTZ NOT NULL DEFAULT now();",
        """
        CREATE TABLE IF NOT EXISTS mrp.raw_ingest_watermark (
          id SMALLINT PRIMARY KEY DEFAULT 1,
          last_raw_id BIGINT NOT NULL DEFAULT 0,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """,
        "INSERT INTO mrp.raw_ingest_watermark (id, last_raw_id) VALUES (1, 0) ON CONFLICT (id) DO NOTHING;",
        """
        CREATE TABLE IF NOT EXISTS mrp.fact_payment_events (
          event_id TEXT PRIMARY KEY,
          event_type TEXT NOT NULL,
          event_time_utc TIMESTAMPTZ,
          merchant_id TEXT,
          order_id TEXT,
          authorization_id TEXT,
          capture_id TEXT,
          refund_id TEXT,
          dispute_id TEXT,
          payout_id TEXT,
          settlement_id TEXT,
          status TEXT,
          amount NUMERIC,
          currency TEXT,
          fx_rate_to_usd NUMERIC,
          amount_usd NUMERIC,
          fee_amount NUMERIC,
          net_amount NUMERIC,
          refund_amount NUMERIC,
          dispute_amount NUMERIC,
          dispute_fee NUMERIC,
          payer_id_hash TEXT,
          payer_country TEXT,
          merchant_country TEXT,
          merchant_mcc TEXT,
          payment_method TEXT,
          card_brand TEXT,
          channel TEXT,
          card_present BOOLEAN,
          three_ds_result TEXT,
          device_id_hash TEXT,
          ip_hash TEXT,
          avs_result TEXT,
          cvv_result TEXT,
          settlement_time_utc TIMESTAMPTZ,
          payout_time_utc TIMESTAMPTZ,
          settlement_lag_hours DOUBLE PRECISION,
          payout_lag_hours DOUBLE PRECISION,
          is_refund BOOLEAN,
          is_dispute BOOLEAN,
          is_denied BOOLEAN,
          is_cross_border BOOLEAN,
          source_system TEXT,
          ingested_at_utc TIMESTAMPTZ,
          trace_id TEXT,
          correlation_id TEXT,
          raw_event_version INT,
          label_bad_outcome INT,
          label_anomaly INT,
          label_dispute_lost INT,
          drift_regime_id INT
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_fact_merchant_time ON mrp.fact_payment_events (merchant_id, event_time_utc DESC);",
        """
        CREATE TABLE IF NOT EXISTS mrp.dim_merchant (
          merchant_id TEXT PRIMARY KEY,
          merchant_name TEXT,
          onboarding_date DATE,
          merchant_segment TEXT,
          mcc TEXT,
          industry_name TEXT,
          risk_tier TEXT,
          kyc_status TEXT,
          account_status TEXT,
          country TEXT,
          state TEXT,
          timezone TEXT,
          fee_plan_id TEXT,
          processor_fee_rate NUMERIC,
          fixed_fee NUMERIC,
          settlement_schedule TEXT,
          payout_frequency TEXT,
          payout_method TEXT,
          reserve_rate NUMERIC,
          avg_order_value_usd_est NUMERIC,
          avg_daily_txn_est INT,
          created_at_utc TIMESTAMPTZ DEFAULT now()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS mrp.merchant_feature_snapshots (
          id BIGSERIAL PRIMARY KEY,
          merchant_id TEXT NOT NULL,
          feature_time_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
          snapshot_time_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
          features JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """,
        "ALTER TABLE mrp.merchant_feature_snapshots ADD COLUMN IF NOT EXISTS snapshot_time_utc TIMESTAMPTZ NOT NULL DEFAULT now();",
        "CREATE INDEX IF NOT EXISTS idx_mfs_mid_snapshot_time ON mrp.merchant_feature_snapshots (merchant_id, snapshot_time_utc DESC);",
    ]

    with db_conn() as conn:
        with conn.cursor() as cur:
            for stmt in ddl:
                cur.execute(stmt)


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_schema()
    yield


app = FastAPI(title="Merchant Risk Platform - Ingestion API", version="0.1.0", lifespan=lifespan)


@app.get("/healthz")
def healthz() -> Dict[str, str]:
    return {"status": "ok"}


class RawEventIn(BaseModel):
    source: str = Field(default="HTTP", description="Origin of the event (SYNTHETIC, WEBHOOK, etc.)")
    payload: Dict[str, Any] = Field(..., description="Raw event JSON object to store as jsonb")

class S3CsvIn(BaseModel):
    bucket: str = Field(..., description="S3 bucket name")
    key: str = Field(..., description="S3 object key (path)")

@app.post("/ingest/raw")
def ingest_raw(body: RawEventIn) -> Dict[str, Any]:
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO mrp.raw_realtime_events (source, payload)
                    VALUES (%s, %s::jsonb)
                    RETURNING id, received_at
                    """,
                    (body.source, json.dumps(body.payload)),
                )
                row = cur.fetchone()
                if row is None:
                    raise HTTPException(status_code=500, detail="db_insert_failed: no row returned")
                return {"status": "ok", "id": row["id"], "received_at": row["received_at"].isoformat()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"db_insert_failed: {e}")


@app.post("/ingest/dim_merchant/csv")
async def ingest_dim_merchant_csv(request: Request) -> Dict[str, Any]:
    """
    Ingest dim_merchant as CSV (with header) via raw request body.
    Usage (example):
      curl -X POST http://<ALB>/ingest/dim_merchant/csv \
        -H "Content-Type: text/csv" --data-binary @dim_merchant.csv
    """
    try:
        data = await request.body()
        if not data:
            raise HTTPException(status_code=400, detail="empty_body")

        with psycopg.connect(DATABASE_URL, autocommit=False, row_factory=dict_row) as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    # temp staging for idempotent upsert
                    cur.execute(
                        """
                        CREATE TEMP TABLE IF NOT EXISTS tmp_dim_merchant
                        (LIKE mrp.dim_merchant INCLUDING DEFAULTS)
                        ON COMMIT DROP;
                        """
                    )
                    cur.execute("TRUNCATE tmp_dim_merchant;")

                    with cur.copy("COPY tmp_dim_merchant FROM STDIN WITH (FORMAT csv, HEADER true)") as cp:
                        cp.write(data)

                    cur.execute("SELECT COUNT(*) AS n FROM tmp_dim_merchant;")
                    n = cur.fetchone()["n"]

                    cur.execute(
                        """
                        INSERT INTO mrp.dim_merchant (
                          merchant_id, merchant_name, onboarding_date, merchant_segment, mcc, industry_name,
                          risk_tier, kyc_status, account_status, country, state, timezone, fee_plan_id,
                          processor_fee_rate, fixed_fee, settlement_schedule, payout_frequency, payout_method,
                          reserve_rate, avg_order_value_usd_est, avg_daily_txn_est, created_at_utc
                        )
                        SELECT
                          merchant_id, merchant_name, onboarding_date, merchant_segment, mcc, industry_name,
                          risk_tier, kyc_status, account_status, country, state, timezone, fee_plan_id,
                          processor_fee_rate, fixed_fee, settlement_schedule, payout_frequency, payout_method,
                          reserve_rate, avg_order_value_usd_est, avg_daily_txn_est, created_at_utc
                        FROM tmp_dim_merchant
                        ON CONFLICT (merchant_id) DO UPDATE SET
                          merchant_name = EXCLUDED.merchant_name,
                          onboarding_date = EXCLUDED.onboarding_date,
                          merchant_segment = EXCLUDED.merchant_segment,
                          mcc = EXCLUDED.mcc,
                          industry_name = EXCLUDED.industry_name,
                          risk_tier = EXCLUDED.risk_tier,
                          kyc_status = EXCLUDED.kyc_status,
                          account_status = EXCLUDED.account_status,
                          country = EXCLUDED.country,
                          state = EXCLUDED.state,
                          timezone = EXCLUDED.timezone,
                          fee_plan_id = EXCLUDED.fee_plan_id,
                          processor_fee_rate = EXCLUDED.processor_fee_rate,
                          fixed_fee = EXCLUDED.fixed_fee,
                          settlement_schedule = EXCLUDED.settlement_schedule,
                          payout_frequency = EXCLUDED.payout_frequency,
                          payout_method = EXCLUDED.payout_method,
                          reserve_rate = EXCLUDED.reserve_rate,
                          avg_order_value_usd_est = EXCLUDED.avg_order_value_usd_est,
                          avg_daily_txn_est = EXCLUDED.avg_daily_txn_est,
                          created_at_utc = EXCLUDED.created_at_utc;
                        """
                    )

        return {"status": "ok", "rows": n}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"dim_merchant_ingest_failed: {e}")

@app.post("/ingest/fact_payment_events/s3")
def ingest_fact_payment_events_s3(body: S3CsvIn) -> Dict[str, Any]:
    """
    Ingest fact_payment_events from a CSV stored in S3 (header required).
    Example:
      curl -X POST http://<ALB>/ingest/fact_payment_events/s3 \
        -H "Content-Type: application/json" \
        -d '{"bucket":"mrp-raw-326148035022-us-east-1","key":"synthetic/fact_payment_events.csv"}'
    """
    try:
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=body.bucket, Key=body.key)
        data = obj["Body"].read()
        if not data:
            raise HTTPException(status_code=400, detail="empty_s3_object")

        with psycopg.connect(DATABASE_URL, autocommit=False, row_factory=dict_row) as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TEMP TABLE IF NOT EXISTS tmp_fact_payment_events
                        (LIKE mrp.fact_payment_events INCLUDING DEFAULTS)
                        ON COMMIT DROP;
                        """
                    )
                    cur.execute("TRUNCATE tmp_fact_payment_events;")

                    with cur.copy(
                        "COPY tmp_fact_payment_events ("
                        "event_id,event_type,event_time_utc,merchant_id,order_id,status,amount_usd,"
                        "payer_id_hash,merchant_country,merchant_mcc,source_system,ingested_at_utc,"
                        "label_bad_outcome,drift_regime_id"
                        ") FROM STDIN WITH (FORMAT csv, HEADER true)"
                    ) as cp:
                        cp.write(data)

                    cur.execute("SELECT COUNT(*) AS n FROM tmp_fact_payment_events;")
                    staged = cur.fetchone()["n"]

                    cur.execute(
                        """
                        INSERT INTO mrp.fact_payment_events (
                        event_id,event_type,event_time_utc,merchant_id,order_id,status,amount_usd,
                        payer_id_hash,merchant_country,merchant_mcc,source_system,ingested_at_utc,
                        label_bad_outcome,drift_regime_id
                        )
                        SELECT
                        event_id,event_type,event_time_utc,merchant_id,order_id,status,amount_usd,
                        payer_id_hash,merchant_country,merchant_mcc,source_system,ingested_at_utc,
                        label_bad_outcome,drift_regime_id
                        FROM tmp_fact_payment_events
                        ON CONFLICT (event_id) DO NOTHING;
                        """
                    )

                    cur.execute("SELECT COUNT(*) AS total FROM mrp.fact_payment_events;")
                    total = cur.fetchone()["total"]

        return {"status": "ok", "staged_rows": staged, "fact_total_rows": total}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"s3_read_failed: {e}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"fact_ingest_failed: {e}")

@app.get("/merchants/{merchant_id}")
def get_merchant(merchant_id: str) -> Dict[str, Any]:
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM mrp.dim_merchant WHERE merchant_id = %s",
                    (merchant_id,),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="merchant_not_found")
                return row
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"db_read_failed: {e}")


@app.get("/merchants/{merchant_id}/features/latest")
def get_latest_features(merchant_id: str) -> Dict[str, Any]:
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM mrp.merchant_feature_snapshots
                    WHERE merchant_id = %s
                    ORDER BY snapshot_time_utc DESC
                    LIMIT 1
                    """,
                    (merchant_id,),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="features_not_found")
                return row
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"db_read_failed: {e}")
