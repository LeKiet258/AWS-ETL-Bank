variable "name_prefix" {
  type = string
  description = "name_prefix of this project"
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