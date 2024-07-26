locals {
    local_env = read_terragrunt_config(find_in_parent_folders("env.hcl"))

    # extract local_env variables
    environment           = local.local_env.locals.environment
    environment_code      = local.local_env.locals.environment_code
    aws_account_id        = local.local_env.locals.aws_account_id
    aws_profile           = local.local_env.locals.aws_profile
    aws_region_code       = local.local_env.locals.aws_region_code
    environment_branch    = local.local_env.locals.environment_branch

}

include {
  path = find_in_parent_folders()
}

terraform {
    source = "../../../modules//aws-code-pipelines"
}

dependency "s3" {
    config_path = "../aws-s3"
}

inputs = {
    name = "${local.aws_profile}-${local.aws_region_code}-${local.environment_code}"
    environment_code = local.environment_code
    aws_region_code = local.aws_region_code
    aws_account_id = local.aws_account_id

    code_artifact_bucket = dependency.s3.outputs.code_artifact_bucket
    code_artifact_bucket_arn = dependency.s3.outputs.code_artifact_bucket_arn
    code_pipeline_bucket = dependency.s3.outputs.code_pipeline_bucket
    code_pipeline_bucket_arn = dependency.s3.outputs.code_pipeline_bucket_arn
    lambda_deployment_bucket = dependency.s3.outputs.lambda_deployment_bucket
    lambda_deployment_bucket_arn = dependency.s3.outputs.lambda_deployment_bucket_arn
    data_landing_bucket = dependency.s3.outputs.data_landing_bucket
    data_landing_bucket_arn = dependency.s3.outputs.data_landing_bucket_arn

    # branch
    // code_branch = local.environment_branch
    code_branch = "develop"

    namespace = "${local.aws_profile}-${local.aws_region_code}-${local.environment_code}"

    # codebuild docker image general
    codebuild_image                         = "aws/codebuild/amazonlinux2-x86_64-standard:5.0"
    codebuild_container_type                = "LINUX_CONTAINER"
    codebuild_image_pull_credentials_type   = "CODEBUILD"
    codebuild_general1_small_compute_type   = "BUILD_GENERAL1_SMALL"
    codebuild_type                          = "CODEPIPELINE"
    codebuild_buildspec                     = "buildspec.yaml"

    # repo names
    python_repo_name = "data-engineering-basic"
    python_lambda_repo_name = "data-engineering-basic-lambda"

    tags = {
        "environment_code" : local.local_env.locals.environment_code
        "stage_name": "share-service"
        "function_code": "share-service"
    }
}