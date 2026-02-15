# SSM Interface Endpoints for ECS Exec (private subnets)
# Services:
# - com.amazonaws.<region>.ssm
# - com.amazonaws.<region>.ssmmessages
# - com.amazonaws.<region>.ec2messages

locals {
  ssm_endpoint_services = [
    "com.amazonaws.${local.region}.ssm",
    "com.amazonaws.${local.region}.ssmmessages",
    "com.amazonaws.${local.region}.ec2messages",
  ]
}

resource "aws_security_group" "ssm_endpoints" {
  name        = "mrp-ssm-endpoints-sg"
  description = "Interface endpoints for SSM / ECS Exec"
  vpc_id      = aws_vpc.dev.id

  ingress {
    description     = "HTTPS from within VPC"
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    security_groups = [aws_security_group.ingestion_api_tasks.id]
  }

  egress {
    description = "All egress"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "mrp-ssm-endpoints-sg"
    project     = local.project
    environment = local.env
    managed_by  = "opentofu"
  }
}

resource "aws_vpc_endpoint" "ssm" {
  for_each            = toset(local.ssm_endpoint_services)
  vpc_id              = aws_vpc.dev.id
  service_name        = each.value
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true

  subnet_ids         = [for s in aws_subnet.private : s.id]
  security_group_ids = [aws_security_group.ssm_endpoints.id]

  tags = {
    Name        = "${local.name_prefix}-vpce-${replace(each.value, "com.amazonaws.${local.region}.", "")}"
    project     = local.project
    environment = local.env
    managed_by  = "opentofu"
  }
}
