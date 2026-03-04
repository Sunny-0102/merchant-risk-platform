output "ingestion_api_cloudfront_domain" {
  description = "CloudFront domain for ingestion-api edge (dev)"
  value       = aws_cloudfront_distribution.ingestion_api.domain_name
}

output "ingestion_api_cloudfront_distribution_id" {
  description = "CloudFront distribution ID (dev)"
  value       = aws_cloudfront_distribution.ingestion_api.id
}

output "ingestion_api_alb_dns" {
  description = "ALB DNS name (dev) - should NOT be reachable directly from the internet"
  value       = aws_lb.ingestion_api.dns_name
}
