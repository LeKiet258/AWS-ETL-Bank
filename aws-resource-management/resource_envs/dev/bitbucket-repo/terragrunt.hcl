locals {
    local_env = read_terragrunt_config(find_in_parent_folders("env.hcl"))

    # extract local_env variables
    environment           = local.local_env.locals.environment
    environment_code      = local.local_env.locals.environment_code
    aws_account_id        = local.local_env.locals.aws_account_id
    aws_profile           = local.local_env.locals.aws_profile
    aws_region_code       = local.local_env.locals.aws_region_code
    bitbucket_workspace   = local.local_env.locals.bitbucket_workspace
}

terraform {
    source = "../../../modules//bitbucket-repo"
}

inputs = {

    bitbucket_workspace = local.bitbucket_workspace
    
    tags = {
        "environment_code" : local.local_env.locals.environment_code
        "stage_name": "share-service"
        "function_code": "share-service"
    }
}