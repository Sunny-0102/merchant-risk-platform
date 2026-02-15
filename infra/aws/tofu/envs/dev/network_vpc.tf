data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  env         = "dev"
  project     = "merchant-risk-platform"
  name_prefix = "mrp-${local.env}-${local.account_id}-${local.region}"

  # 2 AZs for ALB + ECS (ALB requires subnets in at least two AZs)
  azs = slice(data.aws_availability_zones.available.names, 0, 2)

  vpc_cidr = "10.20.0.0/16"

  public_subnet_cidrs  = ["10.20.0.0/20", "10.20.16.0/20"]
  private_subnet_cidrs = ["10.20.32.0/20", "10.20.48.0/20"]
}

resource "aws_vpc" "dev" {
  cidr_block           = local.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name        = "${local.name_prefix}-vpc"
    project     = local.project
    environment = local.env
    managed_by  = "opentofu"
  }
}

resource "aws_internet_gateway" "dev" {
  vpc_id = aws_vpc.dev.id

  tags = {
    Name        = "${local.name_prefix}-igw"
    project     = local.project
    environment = local.env
    managed_by  = "opentofu"
  }
}

resource "aws_subnet" "public" {
  count                   = 2
  vpc_id                  = aws_vpc.dev.id
  availability_zone       = local.azs[count.index]
  cidr_block              = local.public_subnet_cidrs[count.index]
  map_public_ip_on_launch = true

  tags = {
    Name        = "${local.name_prefix}-public-${count.index + 1}"
    project     = local.project
    environment = local.env
    managed_by  = "opentofu"
    tier        = "public"
  }
}

resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = aws_vpc.dev.id
  availability_zone = local.azs[count.index]
  cidr_block        = local.private_subnet_cidrs[count.index]

  tags = {
    Name        = "${local.name_prefix}-private-${count.index + 1}"
    project     = local.project
    environment = local.env
    managed_by  = "opentofu"
    tier        = "private"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.dev.id

  tags = {
    Name        = "${local.name_prefix}-rt-public"
    project     = local.project
    environment = local.env
    managed_by  = "opentofu"
  }
}

resource "aws_route" "public_internet" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.dev.id
}

resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# NAT (single NAT for dev to control cost)
resource "aws_eip" "nat" {
  domain = "vpc"

  tags = {
    Name        = "${local.name_prefix}-nat-eip"
    project     = local.project
    environment = local.env
    managed_by  = "opentofu"
  }
}

resource "aws_nat_gateway" "dev" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id

  tags = {
    Name        = "${local.name_prefix}-nat"
    project     = local.project
    environment = local.env
    managed_by  = "opentofu"
  }

  depends_on = [aws_internet_gateway.dev]
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.dev.id

  tags = {
    Name        = "${local.name_prefix}-rt-private"
    project     = local.project
    environment = local.env
    managed_by  = "opentofu"
  }
}

resource "aws_route" "private_nat" {
  route_table_id         = aws_route_table.private.id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.dev.id
}

resource "aws_route_table_association" "private" {
  count          = 2
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}
