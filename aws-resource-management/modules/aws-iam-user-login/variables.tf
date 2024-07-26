variable "aws_profile" {
  type = string
  description = "aws_profile of this account"
}

variable "usernames" {
  type = list(string)
  description = "list user of a class"
}

variable "usernames_bites" {
  type = list(string)
  description = "list user of a class"
}

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

variable "code_artifact_bucket_arn" {
  type = string
}

variable "data_landing_bucket_arn" {
  type = string
}

variable "temporary_bucket_arn" {
  type = string
}