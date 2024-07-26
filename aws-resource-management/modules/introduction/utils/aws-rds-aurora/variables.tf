variable "environment_code" {
    description = "environment_code"
    type        = string
}

variable "tags" {
    description = "A map of tags to add to all resources"
    type        = map(string)
}

variable "aws_region_code" {
    type = string
}

variable "vpc_id" {
    description = "aurora_name"
    type        = string
}

variable "private_subnets" {
    description = "private_subnets"
    type        = list(string)
}

variable "public_subnets" {
    description = "public_subnets"
    type        = list(string)
}

variable "aurora_name" {
    description = "aurora_name"
    type        = string
}

variable "settings_database" {
    description = "aurora_name"
    type        = map(any)
}

variable "ec2_sg_id" {
    description = "ec2_sg_id"
    type        = string
}