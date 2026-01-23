#!/usr/bin/env bash
set -euo pipefail

echo "Running local E2E..."
bash infra/local/scripts/e2e_local_ci.sh
