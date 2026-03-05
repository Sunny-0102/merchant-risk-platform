# Merchant Risk Platform

AWS-based merchant transaction analytics platform with a production-grade ingestion API and hardened edge.

## What’s running in AWS (dev)

- Ingestion API on ECS Fargate behind an ALB
- CloudFront in front of the ALB (default CloudFront domain, no custom cert)
- WAFv2 WebACL attached to CloudFront (rate-based + AWS managed groups in COUNT mode)
- ALB ingress is restricted to CloudFront only using the AWS-managed origin-facing prefix list
- Data buckets (dev): mrp-raw-*, mrp-curated-*, mrp-errors-*, mrp-archive-*

## Edge security posture

Goal: No direct internet access to the ALB.

- Direct http://<ALB_DNS>/healthz should timeout / be blocked
- https://<CloudFrontDomain>/healthz should return HTTP 200

## Repo scripts

### AWS environment
source scripts/aws_env.sh

### Verify edge is hardened
source scripts/aws_env.sh
CF_DOMAIN="$(cd infra/aws/tofu/envs/dev && tofu output -raw ingestion_api_cloudfront_domain)" ./scripts/verify_edge.sh

### ECS Exec into ingestion API
source scripts/aws_env.sh
./scripts/ecs_exec_ingestion_api.sh "/bin/sh"

## IaC (OpenTofu) — dev

cd infra/aws/tofu/envs/dev
source ../../../../../scripts/aws_env.sh
tofu init
tofu plan

Useful outputs:
tofu output
# ingestion_api_cloudfront_domain
# ingestion_api_cloudfront_distribution_id
# ingestion_api_alb_dns

## Local snapshots (not committed)

infra/.local/ is ignored via .gitignore and can store AWS config snapshots for debugging.
