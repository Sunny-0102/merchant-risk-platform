locals {
  ingestion_api_alb_name = "mrp-ing-dev-${local.account_id}"
  ingestion_api_tg_name  = "mrp-i-dev-${local.account_id}"
}

# ALB Security Group: allow inbound HTTP from the internet
resource "aws_security_group" "ingestion_api_alb" {
  name        = "mrp-ingestion-alb-dev-${local.account_id}-${local.region}"
  description = "ALB SG for ingestion-api (dev)"
  vpc_id      = aws_vpc.dev.id

  ingress {
    description = "HTTP from anywhere (dev)"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    environment = "dev"
    managed_by  = "opentofu"
    project     = "merchant-risk-platform"
  }
}

# Task Security Group: allow inbound ONLY from the ALB to container port 8000
resource "aws_security_group" "ingestion_api_tasks" {
  name        = "mrp-ingestion-tasks-dev-${local.account_id}-${local.region}"
  description = "ECS tasks SG for ingestion-api (dev)"
  vpc_id      = aws_vpc.dev.id

  ingress {
    description     = "App traffic from ALB"
    from_port       = 8000
    to_port         = 8000
    protocol        = "tcp"
    security_groups = [aws_security_group.ingestion_api_alb.id]
  }

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    environment = "dev"
    managed_by  = "opentofu"
    project     = "merchant-risk-platform"
  }
}

resource "aws_lb" "ingestion_api" {
  name               = local.ingestion_api_alb_name
  load_balancer_type = "application"
  internal           = false

  security_groups = [aws_security_group.ingestion_api_alb.id]
  subnets         = [aws_subnet.public[0].id, aws_subnet.public[1].id]

  tags = {
    environment = "dev"
    managed_by  = "opentofu"
    project     = "merchant-risk-platform"
  }
}

resource "aws_lb_target_group" "ingestion_api" {
  name        = local.ingestion_api_tg_name
  port        = 8000
  protocol    = "HTTP"
  vpc_id      = aws_vpc.dev.id
  target_type = "ip"

  health_check {
    enabled             = true
    path                = "/healthz"
    matcher             = "200-399"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 2
  }

  tags = {
    environment = "dev"
    managed_by  = "opentofu"
    project     = "merchant-risk-platform"
  }
}

resource "aws_lb_listener" "ingestion_api_http" {
  load_balancer_arn = aws_lb.ingestion_api.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.ingestion_api.arn
  }
}
