-- MRP bootstrap: schema + core tables + core functions + seed data
-- Runs automatically on fresh Postgres init (docker-entrypoint-initdb.d)

\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS mrp;

-- 1) Raw events landing zone
CREATE TABLE IF NOT EXISTS mrp.raw_realtime_events (
  id          BIGSERIAL PRIMARY KEY,
  source      TEXT NOT NULL,
  payload     JSONB NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_realtime_events_source_id
  ON mrp.raw_realtime_events (source, id);

-- 2) Watermark for incremental ingestion per source
CREATE TABLE IF NOT EXISTS mrp.raw_ingest_watermark (
  source      TEXT PRIMARY KEY,
  last_raw_id BIGINT NOT NULL DEFAULT 0,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 3) Merchant dimension (minimal)
CREATE TABLE IF NOT EXISTS mrp.dim_merchant (
  merchant_id   TEXT PRIMARY KEY,
  merchant_name TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata      JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- 4) Fact table derived from raw events (minimal)
CREATE TABLE IF NOT EXISTS mrp.fact_payment_events (
  event_id        TEXT PRIMARY KEY,
  event_type      TEXT,
  ingested_at_utc TIMESTAMPTZ NOT NULL,
  merchant_id     TEXT NOT NULL,
  order_id        TEXT,
  amount_usd      NUMERIC(18,2),
  status          TEXT,

  -- lineage
  raw_id      BIGINT NOT NULL,
  source      TEXT NOT NULL,
  received_at TIMESTAMPTZ NOT NULL,
  payload     JSONB NOT NULL,

  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fact_payment_events_mid_time
  ON mrp.fact_payment_events (merchant_id, ingested_at_utc);

CREATE INDEX IF NOT EXISTS idx_fact_payment_events_mid_event_time
  ON mrp.fact_payment_events (merchant_id, event_time_utc);

CREATE INDEX IF NOT EXISTS idx_fact_payment_events_source_raw_id
  ON mrp.fact_payment_events (source, raw_id);

-- 5) Feature snapshots (15m + 1h windows) + risk score v1
CREATE TABLE IF NOT EXISTS mrp.merchant_feature_snapshots (
  merchant_id       TEXT NOT NULL,
  snapshot_time_utc TIMESTAMPTZ NOT NULL,
  computed_at_utc   TIMESTAMPTZ NOT NULL DEFAULT now(),
  txn_count_15m     INTEGER NOT NULL DEFAULT 0,
  gmv_usd_15m       NUMERIC(18,2) NOT NULL DEFAULT 0,
  txn_count_1h      INTEGER NOT NULL DEFAULT 0,
  gmv_usd_1h        NUMERIC(18,2) NOT NULL DEFAULT 0,

  -- risk model v1 (simple heuristic score + band)
  risk_score_v1     NUMERIC(5,2) NOT NULL DEFAULT 0,
  risk_band_v1      TEXT NOT NULL DEFAULT 'low',

  PRIMARY KEY (merchant_id, snapshot_time_utc)
);

-- Upgrade-safe: if the table already existed (persistent docker volume), add new columns.
ALTER TABLE mrp.merchant_feature_snapshots
  ADD COLUMN IF NOT EXISTS risk_score_v1 NUMERIC(5,2) NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS risk_band_v1  TEXT         NOT NULL DEFAULT 'low';

CREATE INDEX IF NOT EXISTS idx_mfs_mid_computed_desc
  ON mrp.merchant_feature_snapshots (merchant_id, computed_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_mfs_snapshot_time_desc
  ON mrp.merchant_feature_snapshots (snapshot_time_utc DESC);

-- 6) Incremental raw -> fact processor (idempotent via PK on event_id)
CREATE OR REPLACE FUNCTION mrp.process_raw_events(p_source TEXT, p_batch_size INTEGER)
RETURNS TABLE(processed_rows INTEGER, inserted_fact INTEGER, updated_watermark BIGINT)
LANGUAGE plpgsql
AS $$
DECLARE
  v_last_raw_id BIGINT;
  v_new_last_raw_id BIGINT;
  v_processed INT;
  v_inserted INT;
BEGIN
  -- Ensure watermark row exists
  INSERT INTO mrp.raw_ingest_watermark(source, last_raw_id)
  VALUES (p_source, 0)
  ON CONFLICT (source) DO NOTHING;

  SELECT last_raw_id
    INTO v_last_raw_id
    FROM mrp.raw_ingest_watermark
   WHERE source = p_source;

  WITH batch AS (
    SELECT id, source, payload, received_at
      FROM mrp.raw_realtime_events
     WHERE source = p_source
       AND id > v_last_raw_id
     ORDER BY id
     LIMIT p_batch_size
  ),
  ins_dim AS (
    INSERT INTO mrp.dim_merchant (merchant_id)
    SELECT DISTINCT (payload->>'merchant_id') AS merchant_id
      FROM batch
     WHERE (payload ? 'merchant_id')
       AND (payload->>'merchant_id') IS NOT NULL
       AND (payload->>'merchant_id') <> ''
    ON CONFLICT (merchant_id) DO NOTHING
    RETURNING 1
  ),
  ins_fact AS (
    INSERT INTO mrp.fact_payment_events (
      event_id, event_type, ingested_at_utc, merchant_id, order_id,
      amount_usd, status, raw_id, source, received_at, payload
    )
    SELECT
      COALESCE(NULLIF(payload->>'event_id',''), 'raw_'||id::text) AS event_id,
      payload->>'event_type' AS event_type,
      COALESCE(
        NULLIF(payload->>'ingested_at_utc','')::timestamptz,
        NULLIF(payload->>'event_time_utc','')::timestamptz,
        received_at
      ) AS ingested_at_utc,
      payload->>'merchant_id' AS merchant_id,
      payload->>'order_id' AS order_id,
      NULLIF(payload->>'amount_usd','')::numeric AS amount_usd,
      payload->>'status' AS status,
      id AS raw_id,
      source,
      received_at,
      payload
    FROM batch
    WHERE (payload ? 'merchant_id')
      AND (payload->>'merchant_id') IS NOT NULL
      AND (payload->>'merchant_id') <> ''
    ON CONFLICT (event_id) DO NOTHING
    RETURNING 1
  ),
  stats AS (
    SELECT
      (SELECT COUNT(*) FROM batch) AS processed,
      (SELECT COUNT(*) FROM ins_fact) AS inserted,
      (SELECT COALESCE(MAX(id), v_last_raw_id) FROM batch) AS new_last_raw_id
  )
  SELECT processed, inserted, new_last_raw_id
    INTO v_processed, v_inserted, v_new_last_raw_id
    FROM stats;

  UPDATE mrp.raw_ingest_watermark
     SET last_raw_id = v_new_last_raw_id,
         updated_at  = now()
   WHERE source = p_source;

  processed_rows := COALESCE(v_processed, 0);
  inserted_fact := COALESCE(v_inserted, 0);
  updated_watermark := COALESCE(v_new_last_raw_id, v_last_raw_id);

  RETURN NEXT;
END;
$$;

-- 7) Snapshot recompute for a given 15-min bucket end time (UTC) + risk score v1
CREATE OR REPLACE FUNCTION mrp.recompute_feature_snapshot(p_bucket_time_utc TIMESTAMPTZ)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  v_rows INT;
BEGIN
  WITH base AS (
    SELECT
      merchant_id,
      p_bucket_time_utc AS snapshot_time_utc,
      COUNT(*) FILTER (
        WHERE event_time_utc >= p_bucket_time_utc - interval '15 minutes'
          AND event_time_utc <  p_bucket_time_utc
      ) AS txn_count_15m,
      COALESCE(SUM(amount_usd) FILTER (
        WHERE event_time_utc >= p_bucket_time_utc - interval '15 minutes'
          AND event_time_utc <  p_bucket_time_utc
      ), 0) AS gmv_usd_15m,
      COUNT(*) FILTER (
        WHERE event_time_utc >= p_bucket_time_utc - interval '1 hour'
          AND event_time_utc <  p_bucket_time_utc
      ) AS txn_count_1h,
      COALESCE(SUM(amount_usd) FILTER (
        WHERE event_time_utc >= p_bucket_time_utc - interval '1 hour'
          AND event_time_utc <  p_bucket_time_utc
      ), 0) AS gmv_usd_1h
    FROM mrp.fact_payment_events
    WHERE event_time_utc <  p_bucket_time_utc
      AND event_time_utc >= p_bucket_time_utc - interval '1 hour'
    GROUP BY merchant_id
  ),
  scored AS (
    SELECT
      b.*,
      ROUND(s.score::numeric, 2)::numeric(5,2) AS risk_score_v1,
      CASE
        WHEN s.score >= 80 THEN 'high'
        WHEN s.score >= 50 THEN 'medium'
        ELSE 'low'
      END AS risk_band_v1
    FROM base b
    CROSS JOIN LATERAL (
      SELECT LEAST(
        100.0,
        -- velocity (counts)
        12.0 * b.txn_count_15m
        + 3.0 * b.txn_count_1h
        -- value (log-scaled GMV) - clamp at 0 so ln() never sees negatives
        + 15.0 * ln(1.0 + GREATEST(0.0, b.gmv_usd_15m::double precision))
        + 5.0  * ln(1.0 + GREATEST(0.0, b.gmv_usd_1h::double precision))
        -- burstiness (15m share of 1h, capped)
        + 40.0 * LEAST(
            1.0,
            COALESCE(
              b.txn_count_15m::double precision / NULLIF(b.txn_count_1h::double precision, 0.0),
              0.0
            )
          )
      ) AS score
    ) s
  ),
  up AS (
    INSERT INTO mrp.merchant_feature_snapshots (
      merchant_id, snapshot_time_utc, computed_at_utc,
      txn_count_15m, gmv_usd_15m, txn_count_1h, gmv_usd_1h,
      risk_score_v1, risk_band_v1
    )
    SELECT
      merchant_id,
      snapshot_time_utc,
      now(),
      txn_count_15m,
      gmv_usd_15m,
      txn_count_1h,
      gmv_usd_1h,
      risk_score_v1,
      risk_band_v1
    FROM scored
    ON CONFLICT (merchant_id, snapshot_time_utc)
    DO UPDATE SET
      computed_at_utc = EXCLUDED.computed_at_utc,
      txn_count_15m   = EXCLUDED.txn_count_15m,
      gmv_usd_15m     = EXCLUDED.gmv_usd_15m,
      txn_count_1h    = EXCLUDED.txn_count_1h,
      gmv_usd_1h      = EXCLUDED.gmv_usd_1h,
      risk_score_v1   = EXCLUDED.risk_score_v1,
      risk_band_v1    = EXCLUDED.risk_band_v1
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_rows FROM up;

  RETURN COALESCE(v_rows, 0);
END;
$$;

-- 8) Seed a SYNTH_LIVE template row so mrp_e2e.sh can clone it on fresh DBs
WITH seed AS (
  SELECT jsonb_build_object(
    'event_id',        'evt_seed_0001',
    'event_type',      'payment_captured',
    'merchant_id',     'm_seed_0001',
    'order_id',        'ord_seed_0001',
    'amount_usd',      42.50,
    'status',          'captured',
    'trace_id',        'tr_seed_0001',
    'correlation_id',  'cor_seed_0001',
    'capture_id',      'cap_seed_0001',
    'event_time_utc',  to_char(now() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
    'ingested_at_utc', to_char(now() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
  ) AS payload
)
INSERT INTO mrp.raw_realtime_events (source, payload)
SELECT 'SYNTH_LIVE', payload
FROM seed
WHERE NOT EXISTS (
  SELECT 1
  FROM mrp.raw_realtime_events
  WHERE source = 'SYNTH_LIVE'
    AND (payload ? 'event_id')
    AND (payload ? 'event_type')
    AND (payload ? 'merchant_id')
);

-- Optional seed merchant row
INSERT INTO mrp.dim_merchant (merchant_id, merchant_name)
SELECT 'm_seed_0001', 'Seed Merchant'
WHERE NOT EXISTS (
  SELECT 1 FROM mrp.dim_merchant WHERE merchant_id = 'm_seed_0001'
);
