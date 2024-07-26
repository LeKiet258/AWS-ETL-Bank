variable "name_prefix" {
  type = string
  description = "name_prefix of this project"
}

variable "aws_profile" {
  type = string
  description = "aws_profile of this account"
}

variable "environment_code" {
  description = "environment_code"
  type        = string
}

variable "tags" {
  description = "A map of tags to add to all resources"
  type        = map(string)
}

variable "namespace" {
  type = string
}

variable "aws_region_code" {
  type = string
}

variable "code_artifact_bucket_arn" {
  type = string
}

variable "code_artifact_bucket" {
  type = string
}

variable "data_landing_bucket_arn" {
  type = string
}

variable "data_landing_bucket" {
  type = string
}

variable "temporary_bucket" {
  type = string
}

variable "temporary_bucket_arn" {
  type = string
}

variable "default_security_group_id" {
  type = string
}

variable "private_subnets" {
  type = list(string)
}