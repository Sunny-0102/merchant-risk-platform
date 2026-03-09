#!/usr/bin/env python3
from __future__ import annotations

import csv
import pickle
from pathlib import Path
from typing import Any

TRAINING_DATASET_PATH = Path("data/training_exports/risk_training_dataset_latest.csv")
MODEL_DIR = Path("data/models")
MODEL_PATH = MODEL_DIR / "risk_model_latest.pkl"

TARGET_COLUMN = "label_bad_outcome_24h"
FEATURE_COLUMNS = [
    "txn_count_15m",
    "gmv_usd_15m",
    "txn_count_1h",
    "gmv_usd_1h",
    "risk_score_v1",
]


def to_float(value: Any) -> float:
    if value in (None, "", "null"):
        return 0.0
    return float(value)


def load_training_rows(path: Path) -> list[dict[str, float]]:
    if not path.exists():
        raise SystemExit(f"Training dataset not found: {path}")

    rows: list[dict[str, float]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            row = {feature: to_float(raw.get(feature)) for feature in FEATURE_COLUMNS}
            row[TARGET_COLUMN] = to_float(raw.get(TARGET_COLUMN))
            rows.append(row)

    if not rows:
        raise SystemExit(f"Training dataset is empty: {path}")

    return rows


def mean_for(rows: list[dict[str, float]], feature: str) -> float:
    if not rows:
        return 0.0
    return sum(r[feature] for r in rows) / len(rows)


def train_baseline_model(rows: list[dict[str, float]]) -> dict[str, Any]:
    positive_rows = [r for r in rows if r[TARGET_COLUMN] >= 1.0]
    negative_rows = [r for r in rows if r[TARGET_COLUMN] < 1.0]

    positive_rate = len(positive_rows) / len(rows)

    feature_stats: dict[str, dict[str, float]] = {}
    for feature in FEATURE_COLUMNS:
        pos_mean = mean_for(positive_rows, feature)
        neg_mean = mean_for(negative_rows, feature)
        feature_stats[feature] = {
            "positive_mean": pos_mean,
            "negative_mean": neg_mean,
            "weight": pos_mean - neg_mean,
        }

    return {
        "model_type": "baseline_mean_difference",
        "target_column": TARGET_COLUMN,
        "feature_columns": FEATURE_COLUMNS,
        "training_rows": len(rows),
        "positive_rows": len(positive_rows),
        "negative_rows": len(negative_rows),
        "positive_rate": positive_rate,
        "feature_stats": feature_stats,
        "training_dataset_path": str(TRAINING_DATASET_PATH),
    }


def main() -> None:
    rows = load_training_rows(TRAINING_DATASET_PATH)
    model = train_baseline_model(rows)

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    with MODEL_PATH.open("wb") as f:
        pickle.dump(model, f)

    print(f"trained_rows={model['training_rows']}")
    print(f"positive_rows={model['positive_rows']}")
    print(f"negative_rows={model['negative_rows']}")
    print(f"model_path={MODEL_PATH}")


if __name__ == "__main__":
    main()
