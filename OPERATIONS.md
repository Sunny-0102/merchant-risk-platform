# OPERATIONS / Runbook — Merchant Risk Platform

Dev edge posture: **CloudFront → ALB → ECS (Fargate)** with **NO direct internet access to the ALB**.

## Quick start

### 1) Load AWS env
```bash
source scripts/aws_env.sh
```
### 2) Verify edge posture (CloudFront-only ALB)
```bash
source scripts/aws_env.sh
CF_DOMAIN="$(cd infra/aws/tofu/envs/dev && tofu output -raw ingestion_api_cloudfront_domain)" \
  ./scripts/verify_edge.sh
```
Expected:

Direct http://<ALB_DNS>/healthz times out / is blocked

https://<CloudFrontDomain>/healthz returns HTTP 200

### 3) Discover endpoints via OpenTofu outputs (dev)
```bash
cd infra/aws/tofu/envs/dev
source ../../../../../scripts/aws_env.sh
tofu output
```
Key outputs:

ingestion_api_cloudfront_domain

ingestion_api_cloudfront_distribution_id

ingestion_api_alb_dns (should NOT be reachable directly)

ingestion_api_waf_web_acl_arn

ingestion_api_waf_web_acl_id

ingestion_api_waf_web_acl_name

### 4) ECS Exec into ingestion API task
```bash
source scripts/aws_env.sh
./scripts/ecs_exec_ingestion_api.sh "/bin/sh"
```
### IaC (OpenTofu) — dev workflow
```bash
cd infra/aws/tofu/envs/dev
source ../../../../../scripts/aws_env.sh
tofu init
tofu plan
```
### WAF verification (dev)
### Confirm WAF is attached to the CloudFront distribution
```bash
source scripts/aws_env.sh
CF_DOMAIN="$(cd infra/aws/tofu/envs/dev && tofu output -raw ingestion_api_cloudfront_domain)"
DIST_ID="$(aws cloudfront list-distributions --query "DistributionList.Items[?DomainName=='${CF_DOMAIN}'].Id | [0]" --output text)"
aws cloudfront get-distribution --id "$DIST_ID" \
  --query 'Distribution.DistributionConfig.WebACLId' --output text
```
### Confirm WAF rules are in COUNT mode
```bash
source scripts/aws_env.sh
aws wafv2 get-web-acl \
  --name "CreatedByCloudFront-5a5510e1" \
  --scope CLOUDFRONT \
  --id "8f967164-e7fa-44e9-aef2-15adf72d6fd4" \
  --region us-east-1 \
  --query 'WebACL.Rules[].{Priority:Priority,Name:Name,Mode:(Action.Count!=null && `COUNT`) || (OverrideAction.Count!=null && `COUNT`) || `OTHER`}' \
  --output table
```


## Method allowlist (dev)

CloudFront AllowedMethods must remain broad (AWS constraint), so we enforce the effective allowlist at the ALB.

- Allowed: GET, HEAD, OPTIONS, POST
- Blocked at ALB: PUT, PATCH, DELETE (returns HTTP 405)

### Verify method block (via CloudFront)
```bash
source scripts/aws_env.sh
CF_DOMAIN="$(cd infra/aws/tofu/envs/dev && tofu output -raw ingestion_api_cloudfront_domain)"

curl -sS -o /dev/null -w "GET /healthz -> HTTP %{http_code}\n" "https://${CF_DOMAIN}/healthz"
curl -sS -o /dev/null -w "PUT /healthz -> HTTP %{http_code}\n" -X PUT "https://${CF_DOMAIN}/healthz"
```
Expected:

- GET /healthz -> 200

- PUT /healthz -> 405

### Verify rule exists in OpenTofu (dev)
```bash
cd infra/aws/tofu/envs/dev
source ../../../../../scripts/aws_env.sh
tofu state show aws_lb_listener_rule.ingestion_api_block_nonstandard_methods | sed -n "1,200p"
```

Local snapshots (NOT committed)

infra/.local/ is ignored via .gitignore. Store AWS snapshots there for debugging, e.g.:

infra/.local/cloudfront_<DIST_ID>_config.json

infra/.local/waf_web_acl_createdbycloudfront.json

GitHub repo rules (IMPORTANT)

main is protected:

changes must go through a PR

required status checks must pass


