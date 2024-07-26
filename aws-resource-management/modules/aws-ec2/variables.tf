variable "ec2_name" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "tags" {
  type = map(string)
}

variable "aws_region_code" {
  type = string
}

variable "private_subnets" {
  type = list(string)
}

variable "public_subnets" {
  type = list(string)
}

variable "aws_account_id" {
  type = string
}

variable "intro_db_host_name" {
  type = string
}

variable "intro_db_port" {
  type = string
}

variable "intro_db_username" {
  type = string
}

variable "intro_db_password" {
  type = string
}