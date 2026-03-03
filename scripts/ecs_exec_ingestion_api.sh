#!/usr/bin/env bash
set -Eeuo pipefail

# Purpose: Production-grade ECS Exec helper for mrp-ingestion-api-dev
# Usage:
#   source scripts/aws_env.sh
#   ./scripts/ecs_exec_ingestion_api.sh "/bin/sh -lc 'id -un; env | head'"

# Require the repo-standard AWS env to be sourced in the current shell.
: "${AWS_PROFILE:?ERROR: AWS_PROFILE not set. Run: source scripts/aws_env.sh}"
: "${AWS_REGION:?ERROR: AWS_REGION not set. Run: source scripts/aws_env.sh}"
: "${AWS_SDK_LOAD_CONFIG:?ERROR: AWS_SDK_LOAD_CONFIG not set. Run: source scripts/aws_env.sh}"

CLUSTER="${CLUSTER:-mrp-ecs-dev-326148035022-us-east-1}"
SERVICE="${SERVICE:-mrp-ingestion-api-dev}"
CONTAINER="${CONTAINER:-ingestion-api}"

# Ensure caller identity is valid (catches ExpiredToken before we do anything else).
aws sts get-caller-identity >/dev/null

TASK_ARN="$(
  aws ecs list-tasks \
    --cluster "$CLUSTER" \
    --service-name "$SERVICE" \
    --desired-status RUNNING \
    --query 'taskArns[0]' \
    --output text
)"

if [[ -z "$TASK_ARN" || "$TASK_ARN" == "None" ]]; then
  echo "ERROR: No RUNNING task found for service=$SERVICE in cluster=$CLUSTER" >&2
  exit 1
fi

COMMAND="${1:-/bin/sh}"

echo "ECS Exec → profile=$AWS_PROFILE region=$AWS_REGION cluster=$CLUSTER service=$SERVICE task=$TASK_ARN container=$CONTAINER" >&2
exec aws ecs execute-command \
  --cluster "$CLUSTER" \
  --task "$TASK_ARN" \
  --container "$CONTAINER" \
  --interactive \
  --command "$COMMAND"
