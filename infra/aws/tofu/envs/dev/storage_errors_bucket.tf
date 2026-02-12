locals {
  # reuse local.region and local.account_id from storage_raw_bucket.tf
  errors_bucket = "mrp-errors-${local.account_id}-${local.region}"
}

resource "aws_s3_bucket" "errors" {
  bucket        = local.errors_bucket
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "errors" {
  bucket                  = aws_s3_bucket.errors.id
  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "errors" {
  bucket = aws_s3_bucket.errors.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_versioning" "errors" {
  bucket = aws_s3_bucket.errors.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "errors" {
  bucket = aws_s3_bucket.errors.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "errors" {
  bucket = aws_s3_bucket.errors.id

  rule {
    id     = "abort-incomplete-mpu"
    status = "Enabled"
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  rule {
    id     = "expire-errors-dev"
    status = "Enabled"
    expiration {
      days = 30
    }
  }
}
