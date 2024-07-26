variable "name" {
  description = "A name can be reused across resources in this module."
  type        = string
}

variable "environment_code" {
  description = "env code"
  type        = string
}

variable "aws_account_id" {
  description = "aws_account_id"
  type        = string
}

variable "tags" {
  description = "A map of tags to add to all resources"
  type        = map(string)
}

variable "namespace" {
  description = "namespace"
  type = string
}

variable "code_artifact_bucket" {
  description = "code artifacts to build code"
  type        = string
}

variable "code_artifact_bucket_arn" {
  description = "code artifacts bucket arn to build code"
  type        = string
}

variable "code_pipeline_bucket" {
  description = "code pipeline to build code"
  type        = string
}

variable "code_pipeline_bucket_arn" {
  description = "code pipeline bucket arn to build code"
  type        = string
}

variable "data_landing_bucket" {
  description = "data_landing_bucket"
  type        = string
}

variable "data_landing_bucket_arn" {
  description = "data_landing_bucket arn"
  type        = string
}

variable "lambda_deployment_bucket" {
  description = "code pipeline to build code"
  type        = string
}

variable "lambda_deployment_bucket_arn" {
  description = "code pipeline bucket arn to build code"
  type        = string
}

variable "python_repo_name" {
  description = "python_repo_name is name of repo"
  type        = string
}

variable "python_lambda_repo_name" {
  description = "python_repo_name is name of repo"
  type        = string
}

variable "code_branch" {
  description = "branch code of the repo"
  type        = string
}

variable "codebuild_image" {
  description = "codebuild_image of the repo"
  type        = string
}

variable "codebuild_container_type" {
  description = "codebuild_container_type of the repo"
  type        = string
}

variable "codebuild_image_pull_credentials_type" {
  description = "codebuild_image_pull_credentials_type of the repo"
  type        = string
}

variable "codebuild_general1_small_compute_type" {
  description = "codebuild_general1_small_compute_type of the repo"
  type        = string
}

variable "codebuild_type" {
  description = "codebuild_type of the repo"
  type        = string
}

variable "codebuild_buildspec" {
  description = "codebuild_buildspec of the repo"
  type        = string
}
