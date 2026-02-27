locals {
  # Deploy by immutable tag, but resolve to a digest for deterministic ECS task defs
  ingestion_api_tag   = "git-312e4b3-amd64"
  ingestion_api_image = "${aws_ecr_repository.ingestion_api.repository_url}@${data.aws_ecr_image.ingestion_api.image_digest}"
  # Postgres sidecar (dev only)
  postgres_image    = "postgres:15"
  postgres_user     = "app"
  postgres_password = "app"
  postgres_db       = "appdb"

  # IMPORTANT: in awsvpc/Fargate, containers in the same task talk over localhost
  ingestion_api_database_url = "postgresql://${local.postgres_user}:${local.postgres_password}@127.0.0.1:5432/${local.postgres_db}"
}

data "aws_ecr_image" "ingestion_api" {
  repository_name = aws_ecr_repository.ingestion_api.name
  image_tag       = local.ingestion_api_tag
}

resource "aws_ecs_task_definition" "ingestion_api" {
  family                   = "mrp-ingestion-api-dev"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512

  execution_role_arn = aws_iam_role.ecs_task_execution.arn
  task_role_arn      = aws_iam_role.ingestion_api_task.arn

  container_definitions = jsonencode([
    {
      name      = "postgres"
      image     = local.postgres_image
      essential = true

      environment = [
        { name = "POSTGRES_USER", value = local.postgres_user },
        { name = "POSTGRES_PASSWORD", value = local.postgres_password },
        { name = "POSTGRES_DB", value = local.postgres_db }
      ]

      portMappings = [
        { containerPort = 5432, protocol = "tcp" }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.ingestion_api.name
          awslogs-region        = local.region
          awslogs-stream-prefix = "ecs"
        }
      }

      healthCheck = {
        command     = ["CMD-SHELL", "pg_isready -U app -d appdb || exit 1"]
        interval    = 10
        timeout     = 5
        retries     = 5
        startPeriod = 10
      }
    },
    {
      name      = "ingestion-api"
      image     = local.ingestion_api_image
      essential = true

      dependsOn = [
        { containerName = "postgres", condition = "HEALTHY" }
      ]

      environment = [
        { name = "DATABASE_URL", value = local.ingestion_api_database_url }
      ]

      portMappings = [
        { containerPort = 8000, protocol = "tcp" }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.ingestion_api.name
          awslogs-region        = local.region
          awslogs-stream-prefix = "ecs"
        }
      }

      healthCheck = {
        command     = ["CMD-SHELL", "curl -fsS http://localhost:8000/healthz || exit 1"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 15
      }
    }
  ])
}

resource "aws_ecs_service" "ingestion_api" {
  name                   = "mrp-ingestion-api-dev"
  cluster                = aws_ecs_cluster.dev.id
  task_definition        = aws_ecs_task_definition.ingestion_api.arn
  desired_count          = 1
  enable_execute_command = true
  launch_type            = "FARGATE"

  network_configuration {
    subnets          = aws_subnet.private[*].id
    security_groups  = [aws_security_group.ingestion_api_tasks.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.ingestion_api.arn
    container_name   = "ingestion-api"
    container_port   = 8000
  }

  health_check_grace_period_seconds = 30

  depends_on = [aws_lb_listener.ingestion_api_http]
}
