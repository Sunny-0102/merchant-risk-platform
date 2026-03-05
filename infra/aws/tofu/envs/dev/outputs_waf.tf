output "ingestion_api_waf_web_acl_arn" {
  description = "WAFv2 WebACL ARN attached to ingestion-api CloudFront distribution (dev)"
  value       = aws_wafv2_web_acl.ingestion_api.arn
}

output "ingestion_api_waf_web_acl_id" {
  description = "WAFv2 WebACL ID (dev)"
  value       = aws_wafv2_web_acl.ingestion_api.id
}

output "ingestion_api_waf_web_acl_name" {
  description = "WAFv2 WebACL name (dev)"
  value       = aws_wafv2_web_acl.ingestion_api.name
}
