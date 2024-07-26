resource "aws_vpc" "my_vpc" {
  cidr_block = var.vpc_cidr_block
  enable_dns_hostnames = true

  tags = merge({"Name" = var.vpc_name}, var.tags)
}

resource "aws_internet_gateway" "my_internet_gateway" {
  vpc_id = aws_vpc.my_vpc.id
  tags = merge({"Name" = "${var.vpc_name}-internet-gateway"}, var.tags)
}

resource "aws_subnet" "public_subnets" {
  count      = length(var.public_subnet_cidrs)
  vpc_id     = aws_vpc.my_vpc.id
  cidr_block = element(var.public_subnet_cidrs, count.index)
  availability_zone = element(var.azs, count.index)
  
  tags = merge({"Name" = "${var.public_subnet_name}-${count.index + 1}"}, var.tags)
}
 
resource "aws_subnet" "private_subnets" {
  count      = length(var.private_subnet_cidrs)
  vpc_id     = aws_vpc.my_vpc.id
  cidr_block = element(var.private_subnet_cidrs, count.index)
  availability_zone = element(var.azs, count.index)
  
  tags = merge({"Name" = "${var.private_subnet_name}-${count.index + 1}"}, var.tags)
}

// Create a public route table named "public_route_table"
resource "aws_route_table" "public_route_table" {
  // Put the route table in the VPC
  vpc_id = aws_vpc.my_vpc.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.my_internet_gateway.id
  }
}

resource "aws_route_table_association" "public" {
  count          = length(var.public_subnet_cidrs)
  route_table_id = aws_route_table.public_route_table.id
  subnet_id      = 	aws_subnet.public_subnets[count.index].id
}

resource "aws_route_table" "private_route_table" {
  // Put the route table in the "my_vpc" VPC
  vpc_id = aws_vpc.my_vpc.id
  
  // this is going to be a private route table, 
  // will not be adding a route
}

resource "aws_route_table_association" "private" {
  count          = length(var.private_subnet_cidrs)
  route_table_id = aws_route_table.private_route_table.id
  subnet_id      = aws_subnet.private_subnets[count.index].id
}

