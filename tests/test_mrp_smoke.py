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


def test_ingestion_api_snapshot_schema_matches_structured_bootstrap_shape():
    py_path = _first_existing("services/ingestion_api/main.py")
    txt = py_path.read_text(encoding="utf-8", errors="ignore")

    assert "CREATE TABLE IF NOT EXISTS mrp.merchant_feature_snapshots" in txt
    for col in (
        "txn_count_15m",
        "gmv_usd_15m",
        "txn_count_1h",
        "gmv_usd_1h",
        "risk_score_v1",
        "risk_band_v1",
        "computed_at_utc",
    ):
        assert re.search(rf"\b{re.escape(col)}\b", txt), f"Missing ingestion API snapshot column: {col}"

    assert "features JSONB" not in txt
    assert "feature_time_utc" not in txt


def test_bootstrap_contains_training_dataset_export_function():
    sql_path = _first_existing("infra/local/postgres-init/02_mrp_bootstrap.sql")
    sql = sql_path.read_text(encoding="utf-8", errors="ignore")

    assert "CREATE OR REPLACE FUNCTION mrp.export_risk_training_dataset" in sql
    for token in (
        "snapshot_time_utc",
        "txn_count_15m",
        "gmv_usd_15m",
        "txn_count_1h",
        "gmv_usd_1h",
        "risk_score_v1",
        "risk_band_v1",
        "label_bad_outcome_24h",
        "label_anomaly_24h",
        "label_dispute_lost_24h",
        "snapshot_time_utc + interval '24 hours'",
        "<= now()",
    ):
        assert token in sql, f"Missing training export token in bootstrap SQL: {token}"


def test_airflow_dag_contains_training_dataset_export_task():
    dag_path = _first_existing("infra/local/airflow/dags/mrp_pipeline_dag.py")
    txt = dag_path.read_text(encoding="utf-8", errors="ignore")

    assert 'task_id="export_risk_training_dataset"' in txt
    assert "mrp.export_risk_training_dataset(" in txt


def test_airflow_compose_mounts_training_export_directory():
    compose_path = _first_existing("infra/local/docker-compose.airflow.yml")
    txt = compose_path.read_text(encoding="utf-8", errors="ignore")

    assert "./../../data:/workspace/data" in txt


def test_e2e_local_ci_prepares_training_export_directory():
    script_path = _first_existing("infra/local/scripts/e2e_local_ci.sh")
    txt = script_path.read_text(encoding="utf-8", errors="ignore")

    assert 'mkdir -p "${REPO_ROOT}/data/training_exports"' in txt
    assert 'mkdir -p /workspace/data/training_exports' in txt
    assert 'chown -R ${AIRFLOW_UID}:0 /workspace/data || true' in txt


def test_local_risk_model_training_script_contract_exists():
    script_path = _first_existing("scripts/train_local_risk_model.py")
    txt = script_path.read_text(encoding="utf-8", errors="ignore")

    assert "risk_training_dataset_latest.csv" in txt
    assert "data/models" in txt
    assert "risk_model_latest.pkl" in txt


def test_airflow_dag_contains_local_model_training_task():
    dag_path = _first_existing("infra/local/airflow/dags/mrp_pipeline_dag.py")
    txt = dag_path.read_text(encoding="utf-8", errors="ignore")

    assert 'task_id="train_local_risk_model"' in txt
    assert "scripts/train_local_risk_model.py" in txt


def test_airflow_compose_mounts_local_training_scripts_directory():
    compose_path = _first_existing("infra/local/docker-compose.airflow.yml")
    txt = compose_path.read_text(encoding="utf-8", errors="ignore")

    assert "./../../scripts:/workspace/scripts" in txt


def test_airflow_dag_gates_local_model_training_on_exported_rows():
    dag_path = _first_existing("infra/local/airflow/dags/mrp_pipeline_dag.py")
    txt = dag_path.read_text(encoding="utf-8", errors="ignore")

    assert 'task_id="gate_training_dataset_rows"' in txt
    assert 'key="exported_rows"' in txt
    assert "export_risk_training_dataset >> gate_training_dataset_rows >> train_local_risk_model" in txt


def test_local_risk_model_scoring_script_contract_exists():
    script_path = _first_existing("scripts/score_local_risk_model.py")
    txt = script_path.read_text(encoding="utf-8", errors="ignore")

    assert "data/models" in txt
    assert "risk_model_latest.pkl" in txt
    assert "risk_training_dataset_latest.csv" in txt


def test_local_model_score_persistence_contract_exists():
    dag_path = _first_existing("infra/local/airflow/dags/mrp_pipeline_dag.py")
    txt = dag_path.read_text(encoding="utf-8", errors="ignore")

    assert "persist_local_model_scores" in txt
    assert "risk_model_latest.pkl" in txt
    assert "merchant_feature_snapshots" in txt


def test_local_model_score_schema_columns_exist_in_bootstraps():
    bootstrap_path = _first_existing("infra/local/postgres-init/02_mrp_bootstrap.sql")
    api_path = _first_existing("services/ingestion_api/main.py")

    bootstrap_txt = bootstrap_path.read_text(encoding="utf-8", errors="ignore")
    api_txt = api_path.read_text(encoding="utf-8", errors="ignore")

    for needle in (
        "local_model_score",
        "local_model_band",
        "local_model_version",
        "local_model_scored_at_utc",
    ):
        assert needle in bootstrap_txt
        assert needle in api_txt


def test_local_model_score_persistence_is_gated_by_model_artifact():
    dag_path = _first_existing("infra/local/airflow/dags/mrp_pipeline_dag.py")
    txt = dag_path.read_text(encoding="utf-8", errors="ignore")

    assert 'task_id="gate_local_model_artifact_exists"' in txt
    assert "LOCAL_MODEL_PATH.exists()" in txt
    assert "recompute_snapshot >> gate_local_model_artifact_exists >> persist_local_model_scores" in txt


def test_latest_features_api_exposes_local_model_score_fields():
    api_path = _first_existing("services/ingestion_api/main.py")
    txt = api_path.read_text(encoding="utf-8", errors="ignore")

    assert '"/merchants/{merchant_id}/features/latest"' in txt
    assert "local_model_score" in txt
    assert "local_model_band" in txt
    assert "local_model_version" in txt
    assert "local_model_scored_at_utc" in txt


def test_e2e_script_checks_local_model_score_fields_from_latest_features_api():
    e2e_path = _first_existing("scripts/mrp_e2e.sh", "infra/local/scripts/mrp_e2e.sh")
    txt = e2e_path.read_text(encoding="utf-8", errors="ignore")

    assert "/features/latest" in txt
    assert "local_model_score" in txt
    assert "local_model_band" in txt
    assert "local_model_version" in txt
    assert "local_model_scored_at_utc" in txt


def test_local_model_backtest_script_contract_exists():
    script_path = _first_existing("scripts/backtest_local_model_vs_v1.py")
    txt = script_path.read_text(encoding="utf-8", errors="ignore")

    assert "risk_model_latest.pkl" in txt
    assert "risk_training_dataset_latest.csv" in txt
    assert "precision" in txt
    assert "recall" in txt
    assert "accuracy" in txt
    assert "confusion" in txt


def test_historical_training_seed_script_contract_exists():
    script_path = _first_existing("infra/local/scripts/mrp_seed_historical_training_data.sh")
    txt = script_path.read_text(encoding="utf-8", errors="ignore")

    assert "generate_synthetic_data.py" in txt
    assert "fact_payment_events.csv" in txt
    assert "recompute_feature_snapshot" in txt
    assert "export_risk_training_dataset" in txt
    assert "risk_training_dataset_latest.csv" in txt


def test_historical_training_seed_script_auto_trains_local_model():
    script_path = _first_existing("infra/local/scripts/mrp_seed_historical_training_data.sh")
    txt = script_path.read_text(encoding="utf-8", errors="ignore")

    assert "train_local_risk_model.py" in txt
    assert "risk_model_latest.pkl" in txt

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
