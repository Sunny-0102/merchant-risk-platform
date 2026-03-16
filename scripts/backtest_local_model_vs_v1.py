#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import importlib.util
from datetime import datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_PATH = REPO_ROOT / "data/models/risk_model_latest.pkl"
INPUT_CSV_PATH = REPO_ROOT / "data/training_exports/risk_training_dataset_latest.csv"
SCORING_SCRIPT_PATH = REPO_ROOT / "scripts/score_local_risk_model.py"
TRAINING_SCRIPT_PATH = REPO_ROOT / "scripts/train_local_risk_model.py"

TARGET_COLUMN = "label_bad_outcome_24h"


def to_float(value: Any) -> float:
    if value in (None, "", "null"):
        return 0.0
    return float(value)


def parse_snapshot_time(value: Any) -> datetime:
    txt = str(value or "").strip()
    if not txt:
        raise SystemExit("snapshot_time_utc missing from input row")
    return datetime.fromisoformat(txt.replace("Z", "+00:00"))


def load_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise SystemExit(f"Input dataset not found: {path}")

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        raise SystemExit(f"Input dataset is empty: {path}")

    return rows


def load_module(module_name: str, script_path: Path):
    if not script_path.exists():
        raise SystemExit(f"Script not found: {script_path}")

    spec = importlib.util.spec_from_file_location(module_name, script_path)
    if spec is None or spec.loader is None:
        raise SystemExit(f"Unable to load script from: {script_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def split_rows_time_based(
    rows: list[dict[str, Any]], time_split: float
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], datetime]:
    if not 0.0 < time_split < 1.0:
        raise SystemExit(f"time_split must be between 0 and 1, got: {time_split}")

    ordered = sorted(rows, key=lambda row: parse_snapshot_time(row.get("snapshot_time_utc")))
    unique_snapshot_times = sorted({parse_snapshot_time(row.get("snapshot_time_utc")) for row in ordered})

    if len(unique_snapshot_times) < 2:
        raise SystemExit("time-based split requires at least two distinct snapshot_time_utc values")

    boundary_idx = int(len(unique_snapshot_times) * time_split)
    boundary_idx = max(1, min(boundary_idx, len(unique_snapshot_times) - 1))
    boundary_snapshot_time = unique_snapshot_times[boundary_idx]

    train_rows = [
        row for row in ordered
        if parse_snapshot_time(row.get("snapshot_time_utc")) < boundary_snapshot_time
    ]
    test_rows = [
        row for row in ordered
        if parse_snapshot_time(row.get("snapshot_time_utc")) >= boundary_snapshot_time
    ]

    if not train_rows or not test_rows:
        raise SystemExit("strict time-boundary split produced an empty train or test partition")

    return train_rows, test_rows, boundary_snapshot_time


def build_training_rows(raw_rows: list[dict[str, Any]], feature_columns: list[str]) -> list[dict[str, float]]:
    training_rows: list[dict[str, float]] = []
    for raw in raw_rows:
        row = {feature: to_float(raw.get(feature)) for feature in feature_columns}
        row[TARGET_COLUMN] = to_float(raw.get(TARGET_COLUMN))
        training_rows.append(row)
    return training_rows


def confusion_counts(y_true: list[int], y_pred: list[int]) -> dict[str, int]:
    tp = fp = tn = fn = 0
    for actual, predicted in zip(y_true, y_pred):
        if actual == 1 and predicted == 1:
            tp += 1
        elif actual == 0 and predicted == 1:
            fp += 1
        elif actual == 0 and predicted == 0:
            tn += 1
        else:
            fn += 1
    return {"tp": tp, "fp": fp, "tn": tn, "fn": fn}


def precision(counts: dict[str, int]) -> float:
    denom = counts["tp"] + counts["fp"]
    return counts["tp"] / denom if denom else 0.0


def recall(counts: dict[str, int]) -> float:
    denom = counts["tp"] + counts["fn"]
    return counts["tp"] / denom if denom else 0.0


def accuracy(counts: dict[str, int]) -> float:
    total = counts["tp"] + counts["fp"] + counts["tn"] + counts["fn"]
    return (counts["tp"] + counts["tn"]) / total if total else 0.0


def positive_rate(y_true: list[int]) -> float:
    return sum(y_true) / len(y_true) if y_true else 0.0


def predict_v1(row: dict[str, Any]) -> int:
    risk_band_v1 = str(row.get("risk_band_v1", "")).strip().lower()
    if risk_band_v1:
        return 1 if risk_band_v1 in {"medium", "high"} else 0

    risk_score_v1 = to_float(row.get("risk_score_v1"))
    return 1 if risk_score_v1 >= 50.0 else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backtest local model vs heuristic v1 using a time_split holdout on the latest training dataset."
    )
    parser.add_argument(
        "--input-csv",
        type=Path,
        default=INPUT_CSV_PATH,
        help="CSV to backtest. Defaults to the latest exported training dataset.",
    )
    parser.add_argument(
        "--model-path",
        type=Path,
        default=MODEL_PATH,
        help="Reference model artifact path. Defaults to risk_model_latest.pkl.",
    )
    parser.add_argument(
        "--time-split",
        type=float,
        default=0.8,
        help="Fraction of oldest rows used for training; newest rows are used for testing.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    raw_rows = load_rows(args.input_csv)
    train_rows_raw, test_rows_raw, boundary_snapshot_time = split_rows_time_based(raw_rows, args.time_split)

    scoring_module = load_module("score_local_risk_model", SCORING_SCRIPT_PATH)
    training_module = load_module("train_local_risk_model", TRAINING_SCRIPT_PATH)

    feature_columns: list[str] = list(training_module.FEATURE_COLUMNS)
    train_rows = build_training_rows(train_rows_raw, feature_columns)
    model = training_module.train_baseline_model(train_rows)

    y_true: list[int] = []
    local_preds: list[int] = []
    v1_preds: list[int] = []

    for row in test_rows_raw:
        actual = 1 if to_float(row.get(TARGET_COLUMN)) >= 1.0 else 0
        local_score, _local_band = scoring_module.score_row(model, row)
        local_pred = 1 if local_score >= 0.50 else 0
        v1_pred = predict_v1(row)

        y_true.append(actual)
        local_preds.append(local_pred)
        v1_preds.append(v1_pred)

    local_confusion = confusion_counts(y_true, local_preds)
    v1_confusion = confusion_counts(y_true, v1_preds)

    print(f"model_path_reference={args.model_path}")
    print(f"input_csv={args.input_csv}")
    print(f"time_split={args.time_split:.2f}")
    print(f"rows={len(raw_rows)}")
    print(f"train_rows={len(train_rows_raw)}")
    print(f"test_rows={len(test_rows_raw)}")
    print(f"boundary_snapshot_time={boundary_snapshot_time.isoformat()}")
    print(f"train_end_snapshot_time={train_rows_raw[-1]['snapshot_time_utc']}")
    print(f"test_start_snapshot_time={test_rows_raw[0]['snapshot_time_utc']}")
    print(f"positive_rate={positive_rate(y_true):.6f}")
    print()

    print("[local_model]")
    print(f"precision={precision(local_confusion):.6f}")
    print(f"recall={recall(local_confusion):.6f}")
    print(f"accuracy={accuracy(local_confusion):.6f}")
    print(
        "confusion="
        f"tp:{local_confusion['tp']},"
        f"fp:{local_confusion['fp']},"
        f"tn:{local_confusion['tn']},"
        f"fn:{local_confusion['fn']}"
    )
    print()

    print("[risk_score_v1]")
    print(f"precision={precision(v1_confusion):.6f}")
    print(f"recall={recall(v1_confusion):.6f}")
    print(f"accuracy={accuracy(v1_confusion):.6f}")
    print(
        "confusion="
        f"tp:{v1_confusion['tp']},"
        f"fp:{v1_confusion['fp']},"
        f"tn:{v1_confusion['tn']},"
        f"fn:{v1_confusion['fn']}"
    )


if __name__ == "__main__":
    main()
