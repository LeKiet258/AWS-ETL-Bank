variable "glue_job_list" {
  type = list
}

variable "role_arn" {
  type = string
}

variable "namespace" {
  type = string
}

variable "name_prefix" {
  type = string
}

variable "code_artifact_bucket" {
  type = string
}

variable "temporary_bucket" {
  type = string
}

variable "tags" {
  description = "A map of tags to add to all resources"
  type        = map(string)
}