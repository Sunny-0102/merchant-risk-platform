# CloudFront edge for ingestion-api (dev)
# Note: We use CloudFront default cert (no custom domain), and ALB origin over HTTP (locked down to CloudFront prefix list).

data "aws_cloudfront_cache_policy" "caching_disabled" {
  name = "Managed-CachingDisabled"
}

data "aws_cloudfront_origin_request_policy" "all_viewer_except_host_header" {
  name = "Managed-AllViewerExceptHostHeader"
}



data "aws_wafv2_web_acl" "ingestion_api" {
  name  = "CreatedByCloudFront-5a5510e1"
  scope = "CLOUDFRONT"
}
resource "aws_cloudfront_distribution" "ingestion_api" {
  web_acl_id      = data.aws_wafv2_web_acl.ingestion_api.arn
  enabled         = true
  is_ipv6_enabled = true
  comment         = "mrp-ingestion-api-dev edge"
  http_version    = "http2and3"
  price_class     = "PriceClass_All"

  origin {
    domain_name = aws_lb.ingestion_api.dns_name
    origin_id   = "ingestion-alb"

    custom_origin_config {
      http_port                = 80
      https_port               = 443
      origin_protocol_policy   = "http-only"
      origin_ssl_protocols     = ["TLSv1.2"]
      origin_read_timeout      = 30
      origin_keepalive_timeout = 5
    }
  }

  default_cache_behavior {
    target_origin_id       = "ingestion-alb"
    viewer_protocol_policy = "redirect-to-https"
    compress               = true

    allowed_methods = ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"]
    cached_methods  = ["GET", "HEAD", "OPTIONS"]

    cache_policy_id          = data.aws_cloudfront_cache_policy.caching_disabled.id
    origin_request_policy_id = data.aws_cloudfront_origin_request_policy.all_viewer_except_host_header.id
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }

  tags = {
    environment = "dev"
    managed_by  = "opentofu"
    project     = "merchant-risk-platform"
    component   = "edge"
  }
}
