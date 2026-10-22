data "aws_availability_zone" "wal" {
  zone_id = var.wal_az_id
}
data "aws_availability_zones" "available" {
  state = "available"
}
locals {
  zones = [data.aws_availability_zone.wal.name, [for zone in data.aws_availability_zones.available.names : zone if zone != data.aws_availability_zone.wal.name][0]]
}
resource "aws_vpc" "lab" {
  cidr_block           = "10.40.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags = {
    Name = var.project
  }

}
resource "aws_internet_gateway" "lab" {
  vpc_id = aws_vpc.lab.id
}
resource "aws_subnet" "public" {
  count                   = 2
  vpc_id                  = aws_vpc.lab.id
  cidr_block              = cidrsubnet(aws_vpc.lab.cidr_block, 8, count.index)
  availability_zone       = local.zones[count.index]
  map_public_ip_on_launch = true
  tags = {
    Name = "${var.project}-workers-${count.index}", "kubernetes.io/role/elb" = "1"
  }

}
resource "aws_subnet" "database" {
  count             = 2
  vpc_id            = aws_vpc.lab.id
  cidr_block        = cidrsubnet(aws_vpc.lab.cidr_block, 8, 10 + count.index)
  availability_zone = local.zones[count.index]
  tags = {
    Name = "${var.project}-database-${count.index}"
  }

}
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.lab.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.lab.id
  }

}
resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}
# No NAT gateway. Database subnets have no internet default route.
resource "aws_route_table" "database" {
  vpc_id = aws_vpc.lab.id
}
resource "aws_route_table_association" "database" {
  count          = 2
  subnet_id      = aws_subnet.database[count.index].id
  route_table_id = aws_route_table.database.id
}
