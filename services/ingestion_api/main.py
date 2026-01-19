from __future__ import annotations

import json
import os
from typing import Any, Dict

import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://app:app@postgres:5432/appdb")

app = FastAPI(title="Merchant Risk Platform - Ingestion API", version="0.1.0")


def db_conn():
    # dict_row makes cursor.fetchone() return a dict (nice for JSON APIs)
    return psycopg.connect(DATABASE_URL, autocommit=True, row_factory=dict_row)


@app.get("/healthz")
def healthz() -> Dict[str, str]:
    return {"status": "ok"}


class RawEventIn(BaseModel):
    source: str = Field(default="HTTP", description="Origin of the event (SYNTHETIC, WEBHOOK, etc.)")
    payload: Dict[str, Any] = Field(..., description="Raw event JSON object to store as jsonb")


@app.post("/ingest/raw")
def ingest_raw(body: RawEventIn) -> Dict[str, Any]:
    """
    Minimal production-style ingestion:
    - store the raw event in mrp.raw_realtime_events(payload jsonb)
    - return the inserted row id + timestamp
    """
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
                return {"status": "ok", "id": row["id"], "received_at": row["received_at"].isoformat()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"db_insert_failed: {e}")


@app.get("/merchants/{merchant_id}")
def get_merchant(merchant_id: str) -> Dict[str, Any]:
    """
    Read merchant profile (dimension) from mrp.dim_merchant.
    """
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
    """
    Read the latest computed feature snapshot for a merchant.
    """
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
