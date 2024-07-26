locals {
    local_env = read_terragrunt_config(find_in_parent_folders("env.hcl"))

    # extract local_env variables
    environment           = local.local_env.locals.environment
    environment_code      = local.local_env.locals.environment_code
    aws_account_id        = local.local_env.locals.aws_account_id
    aws_account_owner     = local.local_env.locals.aws_account_owner
    aws_profile           = local.local_env.locals.aws_profile
    aws_region_code       = local.local_env.locals.aws_region_code

}

include {
  path = find_in_parent_folders()
}

terraform {
    source = "../../../modules//aws-s3"
}

inputs = {
    tf_state_bucket = "${local.aws_account_owner}-${local.aws_region_code}-${local.environment_code}-s3"
    name = "${local.aws_profile}-${local.aws_region_code}-${local.environment_code}-s3"
    environment_code = local.environment_code
    aws_region_code = local.aws_region_code

    tags = {
        "environment_code" : local.local_env.locals.environment_code
        "stage_name": "share-service"
        "function_code": "share-service"
    }
}