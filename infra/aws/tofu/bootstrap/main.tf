terraform {
  required_version = ">= 1.6.0"

  backend "s3" {
    bucket         = "mrp-tfstate-326148035022-us-east-1"
    key            = "mrp/bootstrap/tofu.tfstate"
    region         = "us-east-1"
    dynamodb_table = "mrp-tofu-locks-326148035022"
    encrypt        = true
  }
}

provider "aws" {
  region = "us-east-1"
}
