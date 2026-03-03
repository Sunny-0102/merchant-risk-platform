#!/usr/bin/env bash
set -Eeuo pipefail

# Verifies:
# 1) Direct ALB access is NOT reachable from the internet (network blocked -> HTTP code "000")
# 2) CloudFront can reach the service and /healthz returns 200

: "${AWS_PROFILE:?Run: source scripts/aws_env.sh}"
: "${AWS_REGION:?Run: source scripts/aws_env.sh}"
: "${CF_DOMAIN:?Set CF_DOMAIN (e.g. d39o3dsau905g2.cloudfront.net)}"

command -v aws  >/dev/null || { echo "ERROR: aws CLI not found" >&2; exit 1; }
command -v curl >/dev/null || { echo "ERROR: curl not found" >&2; exit 1; }

CLUSTER="${CLUSTER:-mrp-ecs-dev-326148035022-us-east-1}"
SERVICE="${SERVICE:-mrp-ingestion-api-dev}"

TG_ARN="$(aws ecs describe-services --cluster "$CLUSTER" --services "$SERVICE" --query 'services[0].loadBalancers[0].targetGroupArn' --output text)"
[[ -n "$TG_ARN" && "$TG_ARN" != "None" ]] || { echo "ERROR: targetGroupArn not found for service=$SERVICE" >&2; exit 1; }

ALB_ARN="$(aws elbv2 describe-target-groups --target-group-arns "$TG_ARN" --query 'TargetGroups[0].LoadBalancerArns[0]' --output text)"
[[ -n "$ALB_ARN" && "$ALB_ARN" != "None" ]] || { echo "ERROR: ALB ARN not found for target group" >&2; exit 1; }

ALB_DNS="$(aws elbv2 describe-load-balancers --load-balancer-arns "$ALB_ARN" --query 'LoadBalancers[0].DNSName' --output text)"
[[ -n "$ALB_DNS" && "$ALB_DNS" != "None" ]] || { echo "ERROR: ALB DNS not found" >&2; exit 1; }

echo "ALB_DNS=$ALB_DNS"
echo "CF_DOMAIN=$CF_DOMAIN"
echo

echo "== Direct ALB (must be BLOCKED at network layer) =="
alb_code="$(curl -sS --connect-timeout 2 -m 5 -o /dev/null -w "%{http_code}" "http://$ALB_DNS/healthz" || true)"
echo "ALB /healthz -> HTTP $alb_code"
if [[ "$alb_code" != "000" ]]; then
  echo "ERROR: Direct ALB is reachable (HTTP $alb_code). It must be blocked." >&2
  exit 1
fi
echo "OK: Direct ALB blocked (expected)."
echo

echo "== Via CloudFront (must be 200) =="
cb="$(date +%s)"
cf_code="$(curl -sS --connect-timeout 2 -m 10 -o /dev/null -w "%{http_code}" "https://$CF_DOMAIN/healthz?cb=$cb" || true)"
echo "CF /healthz -> HTTP $cf_code"
if [[ "$cf_code" != "200" ]]; then
  echo "ERROR: CloudFront healthcheck failed (HTTP $cf_code)." >&2
  exit 1
fi

echo
echo "OK: Edge verification passed."
