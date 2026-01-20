from pathlib import Path
import re

def _first_existing(*candidates: str) -> Path:
    for c in candidates:
        p = Path(c)
        if p.exists():
            return p
    raise AssertionError(f"None of these paths exist: {candidates}")

def test_bootstrap_schema_contains_expected_snapshot_columns():
    sql_path = _first_existing(
        "infra/local/postgres-init/02_mrp_bootstrap.sql",
        "infra/local/postgres-init/02_mrp_bootstrap.sql",
    )
    sql = sql_path.read_text(encoding="utf-8", errors="ignore")

    # Ensure snapshot table exists in bootstrap
    assert "merchant_feature_snapshots" in sql

    # Ensure current snapshot columns exist
    for col in ("txn_count_15m", "gmv_usd_15m", "txn_count_1h", "gmv_usd_1h"):
        assert re.search(rf"\b{re.escape(col)}\b", sql), f"Missing column in bootstrap SQL: {col}"

    # Ensure removed/unsupported columns are not referenced in bootstrap
    assert "txn_count_24h" not in sql
    assert "gmv_usd_24h" not in sql

def test_ci_scripts_do_not_reference_missing_24h_columns_or_invalid_docker_exec_T():
    # Pick whichever script location exists in this repo layout
    dedup = _first_existing("scripts/mrp_dedup.sh", "infra/local/scripts/mrp_dedup.sh")
    e2e = _first_existing("scripts/mrp_e2e.sh", "infra/local/scripts/mrp_e2e.sh")
    idem = _first_existing("scripts/mrp_idempotency.sh", "infra/local/scripts/mrp_idempotency.sh")

    for p in (dedup, e2e, idem):
        txt = p.read_text(encoding="utf-8", errors="ignore")
        assert "txn_count_24h" not in txt
        assert "gmv_usd_24h" not in txt
        assert "docker exec -T" not in txt  # -T is a docker compose exec flag, not docker exec
