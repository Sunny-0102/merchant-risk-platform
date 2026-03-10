#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import pickle
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "data/models"
MODEL_PATH = MODEL_DIR / "risk_model_latest.pkl"
DEFAULT_INPUT_PATH = REPO_ROOT / "data/training_exports/risk_training_dataset_latest.csv"


def to_float(value: Any) -> float:
    if value in (None, "", "null"):
        return 0.0
    return float(value)


def load_model(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Model artifact not found: {path}")

    with path.open("rb") as f:
        model = pickle.load(f)

    if not isinstance(model, dict):
        raise SystemExit(f"Unexpected model format in: {path}")

    required = {"feature_columns", "feature_stats", "positive_rate"}
    missing = required - set(model.keys())
    if missing:
        raise SystemExit(f"Model artifact missing required keys: {sorted(missing)}")

    return model


def load_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise SystemExit(f"Input dataset not found: {path}")

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        raise SystemExit(f"Input dataset is empty: {path}")

    return rows


def sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def score_row(model: dict[str, Any], raw_row: dict[str, Any]) -> tuple[float, str]:
    feature_columns: list[str] = list(model["feature_columns"])
    feature_stats: dict[str, dict[str, float]] = dict(model["feature_stats"])
    base_rate = float(model["positive_rate"])

    raw_score = 0.0
    weight_scale = 0.0

    for feature in feature_columns:
        stats = feature_stats.get(feature, {})
        pos_mean = float(stats.get("positive_mean", 0.0))
        neg_mean = float(stats.get("negative_mean", 0.0))
        weight = float(stats.get("weight", 0.0))
        midpoint = (pos_mean + neg_mean) / 2.0
        value = to_float(raw_row.get(feature))
        raw_score += (value - midpoint) * weight
        weight_scale += abs(weight)

    normalized = raw_score / max(weight_scale, 1.0)
    score = sigmoid(normalized + math.log(max(base_rate, 1e-6) / max(1.0 - base_rate, 1e-6)))

    if score >= 0.80:
        band = "high"
    elif score >= 0.50:
        band = "medium"
    else:
        band = "low"

    return score, band


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Score rows with the local baseline risk model.")
    parser.add_argument(
        "--input-csv",
        type=Path,
        default=DEFAULT_INPUT_PATH,
        help="CSV to score. Defaults to the latest exported training dataset.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Maximum number of rows to print.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    model = load_model(MODEL_PATH)
    rows = load_rows(args.input_csv)

    print(f"model_path={MODEL_PATH}")
    print(f"input_csv={args.input_csv}")
    print(f"rows_available={len(rows)}")

    for idx, row in enumerate(rows[: max(args.limit, 0)], start=1):
        score, band = score_row(model, row)
        merchant_id = row.get("merchant_id", "")
        snapshot_time_utc = row.get("snapshot_time_utc", "")
        print(
            f"row={idx} "
            f"merchant_id={merchant_id} "
            f"snapshot_time_utc={snapshot_time_utc} "
            f"score={score:.6f} "
            f"band={band}"
        )


if __name__ == "__main__":
    main()
