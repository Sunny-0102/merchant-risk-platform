locals {
  # reuse local.region and local.account_id from storage_raw_bucket.tf
  ecs_cluster_name             = "mrp-ecs-dev-${local.account_id}-${local.region}"
  ingestion_api_log_group_name = "/ecs/mrp-ingestion-api-dev"
}

# CloudWatch log group for ECS awslogs driver
resource "aws_cloudwatch_log_group" "ingestion_api" {
  name              = local.ingestion_api_log_group_name
  retention_in_days = 14
}

# ECS cluster (Fargate capacity lives in the service; cluster is the control plane)
resource "aws_ecs_cluster" "dev" {
  name = local.ecs_cluster_name
}

# Task Execution Role (pull image from ECR + write logs to CloudWatch)
resource "aws_iam_role" "ecs_task_execution" {
  name = "mrp-ecs-task-execution-dev-${local.account_id}-${local.region}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution_managed" {
  role       = aws_iam_role.ecs_task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# App Task Role (your app permissions go here later: S3, secrets, etc.)
resource "aws_iam_role" "ingestion_api_task" {
  name = "mrp-ingestion-api-task-dev-${local.account_id}-${local.region}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}
resource "aws_iam_role_policy" "ingestion_api_task_s3_read_raw" {
  name = "mrp-ingestion-api-task-dev-s3-read-raw-${local.account_id}-${local.region}"
  role = aws_iam_role.ingestion_api_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListRawBucketSyntheticPrefix"
        Effect = "Allow"
        Action = ["s3:ListBucket"]
        Resource = aws_s3_bucket.raw.arn
        Condition = {
          StringLike = {
            "s3:prefix" = ["synthetic/*"]
          }
        }
      },
      {
        Sid    = "ReadRawObjectsSyntheticPrefix"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = "${aws_s3_bucket.raw.arn}/synthetic/*"
      }
    ]
  })
}
