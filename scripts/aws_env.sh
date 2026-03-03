# Usage: source scripts/aws_env.sh
# Purpose: keep AWS auth clean (profile-based) and consistent across this repo.

# Never rely on exported temp keys (these commonly cause ExpiredToken surprises).
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_SECURITY_TOKEN

# Repo-standard defaults (override by setting these before sourcing if needed).
export AWS_PROFILE="${AWS_PROFILE:-mrp-admin}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_SDK_LOAD_CONFIG=1
