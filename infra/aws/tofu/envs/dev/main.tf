terraform {
  required_version = ">= 1.6.0"

  backend "s3" {
    bucket         = "mrp-tfstate-326148035022-us-east-1"
    key            = "mrp/envs/dev/tofu.tfstate"
    region         = "us-east-1"

    # Preferred locking mechanism
    use_lockfile   = true

    # Keep DynamoDB locking for now (we can remove later if desired)
    dynamodb_table = "mrp-tofu-locks-326148035022"

    encrypt        = true
  }
}

provider "aws" {
  region = "us-east-1"

  default_tags {
    tags = {
      project     = "merchant-risk-platform"
      environment = "dev"
      managed_by  = "opentofu"
    }
  }
}
