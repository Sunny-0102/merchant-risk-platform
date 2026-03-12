#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import importlib.util
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_PATH = REPO_ROOT / "data/models/risk_model_latest.pkl"
INPUT_CSV_PATH = REPO_ROOT / "data/training_exports/risk_training_dataset_latest.csv"
SCORING_SCRIPT_PATH = REPO_ROOT / "scripts/score_local_risk_model.py"

TARGET_COLUMN = "label_bad_outcome_24h"


def to_float(value: Any) -> float:
    if value in (None, "", "null"):
        return 0.0
    return float(value)


def load_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise SystemExit(f"Input dataset not found: {path}")

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        raise SystemExit(f"Input dataset is empty: {path}")

    return rows


def load_scoring_module():
    if not SCORING_SCRIPT_PATH.exists():
        raise SystemExit(f"Scoring script not found: {SCORING_SCRIPT_PATH}")

    spec = importlib.util.spec_from_file_location("score_local_risk_model", SCORING_SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise SystemExit(f"Unable to load scoring script from: {SCORING_SCRIPT_PATH}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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
    parser = argparse.ArgumentParser(description="Backtest local model vs heuristic v1 on the latest training dataset.")
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
        help="Model artifact path. Defaults to the latest local trained model artifact.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    rows = load_rows(args.input_csv)
    scoring_module = load_scoring_module()
    model = scoring_module.load_model(args.model_path)

    y_true: list[int] = []
    local_preds: list[int] = []
    v1_preds: list[int] = []

    for row in rows:
        actual = 1 if to_float(row.get(TARGET_COLUMN)) >= 1.0 else 0
        local_score, _local_band = scoring_module.score_row(model, row)
        local_pred = 1 if local_score >= 0.50 else 0
        v1_pred = predict_v1(row)

        y_true.append(actual)
        local_preds.append(local_pred)
        v1_preds.append(v1_pred)

    local_confusion = confusion_counts(y_true, local_preds)
    v1_confusion = confusion_counts(y_true, v1_preds)

    print(f"model_path={args.model_path}")
    print(f"input_csv={args.input_csv}")
    print(f"rows={len(rows)}")
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
