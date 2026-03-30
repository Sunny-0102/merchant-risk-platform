#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3

REPO_ROOT = Path(__file__).resolve().parents[1]
TRAINING_DATASET_PATH = REPO_ROOT / "data/training_exports/risk_training_dataset_latest.csv"
MODEL_DIR = REPO_ROOT / "data/models"
MODEL_ARTIFACT_PATH = MODEL_DIR / "risk_model_latest.pkl"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Launch a scaffold SageMaker training job for merchant risk model training."
    )
    parser.add_argument(
        "--job-name-prefix",
        default="mrp-risk-training",
        help="Prefix for the SageMaker training job name.",
    )
    parser.add_argument(
        "--image-uri",
        default=os.getenv("SAGEMAKER_TRAINING_IMAGE_URI", ""),
        help="Training container image URI.",
    )
    parser.add_argument(
        "--role-arn",
        default=os.getenv("SAGEMAKER_EXECUTION_ROLE_ARN", ""),
        help="IAM role ARN used by the SageMaker training job.",
    )
    parser.add_argument(
        "--input-s3-uri",
        default=os.getenv("SAGEMAKER_INPUT_S3_URI", ""),
        help="S3 URI for the uploaded training dataset.",
    )
    parser.add_argument(
        "--output-s3-uri",
        default=os.getenv("SAGEMAKER_OUTPUT_S3_URI", ""),
        help="S3 URI where SageMaker should write model artifacts.",
    )
    parser.add_argument(
        "--instance-type",
        default=os.getenv("SAGEMAKER_INSTANCE_TYPE", "ml.m5.large"),
        help="SageMaker training instance type.",
    )
    parser.add_argument(
        "--instance-count",
        type=int,
        default=int(os.getenv("SAGEMAKER_INSTANCE_COUNT", "1")),
        help="Number of training instances.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the SageMaker create_training_job payload without submitting it.",
    )
    return parser


def build_training_job_name(prefix: str) -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
    return f"{prefix}-{timestamp}"


def validate_inputs(args: argparse.Namespace) -> None:
    if not TRAINING_DATASET_PATH.exists():
        raise SystemExit(f"Training dataset not found: {TRAINING_DATASET_PATH}")

    required_fields = {
        "image_uri": args.image_uri,
        "role_arn": args.role_arn,
        "input_s3_uri": args.input_s3_uri,
        "output_s3_uri": args.output_s3_uri,
    }
    missing = [name for name, value in required_fields.items() if not value]
    if missing:
        raise SystemExit(
            "Missing required SageMaker configuration: "
            + ", ".join(sorted(missing))
            + ". Set flags or environment variables first."
        )


def build_create_training_job_request(
    args: argparse.Namespace,
    training_job_name: str,
) -> dict[str, Any]:
    return {
        "TrainingJobName": training_job_name,
        "RoleArn": args.role_arn,
        "AlgorithmSpecification": {
            "TrainingImage": args.image_uri,
            "TrainingInputMode": "File",
        },
        "InputDataConfig": [
            {
                "ChannelName": "training",
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": args.input_s3_uri,
                        "S3DataDistributionType": "FullyReplicated",
                    }
                },
                "ContentType": "text/csv",
                "CompressionType": "None",
            }
        ],
        "OutputDataConfig": {
            "S3OutputPath": args.output_s3_uri,
        },
        "ResourceConfig": {
            "InstanceType": args.instance_type,
            "InstanceCount": args.instance_count,
            "VolumeSizeInGB": 30,
        },
        "StoppingCondition": {
            "MaxRuntimeInSeconds": 3600,
        },
        "Environment": {
            "TRAINING_DATASET_BASENAME": "risk_training_dataset_latest.csv",
            "MODEL_ARTIFACT_BASENAME": "risk_model_latest.pkl",
            "LOCAL_TRAINING_DATASET_PATH": str(TRAINING_DATASET_PATH),
            "LOCAL_MODEL_ARTIFACT_PATH": str(MODEL_ARTIFACT_PATH),
        },
    }


def main() -> None:
    args = build_parser().parse_args()
    validate_inputs(args)

    training_job_name = build_training_job_name(args.job_name_prefix)
    request = build_create_training_job_request(args, training_job_name)

    print(f"training_dataset_path={TRAINING_DATASET_PATH}")
    print(f"expected_model_artifact_path={MODEL_ARTIFACT_PATH}")
    print("aws_service=sagemaker")
    print(f"training_job_name={training_job_name}")
    print(f"input_s3_uri={args.input_s3_uri}")
    print(f"output_s3_uri={args.output_s3_uri}")
    print(f"dry_run={args.dry_run}")

    if args.dry_run:
        print("create_training_job_request_ready=true")
        return

    client = boto3.client("sagemaker", region_name=os.getenv("AWS_REGION", "us-east-1"))
    response = client.create_training_job(**request)
    print(f"create_training_job_arn={response.get('TrainingJobArn', '')}")


if __name__ == "__main__":
    main()
