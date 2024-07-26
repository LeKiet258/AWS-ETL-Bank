variable "glue_crawlers_list" {
  type = list
}

variable "role_arn" {
  type = string
}

variable "name_prefix" {
  type = string
}

variable "bucket_name" {
  type = string
}

variable "tags" {
  description = "A map of tags to add to all resources"
  type        = map(string)
}