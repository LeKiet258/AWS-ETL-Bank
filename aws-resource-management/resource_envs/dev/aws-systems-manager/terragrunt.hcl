locals {
    local_env = read_terragrunt_config(find_in_parent_folders("env.hcl"))

    # extract local_env variables
    environment           = local.local_env.locals.environment
    environment_code      = local.local_env.locals.environment_code
    aws_account_id        = local.local_env.locals.aws_account_id
    aws_profile           = local.local_env.locals.aws_profile
    aws_region_code       = local.local_env.locals.aws_region_code
}

include {
  path = find_in_parent_folders()
}

terraform {
    source = "../../../modules//aws-systems-manager"
}

dependency "intro_db" {
  config_path = "../introduction/utils/aws-rds-aurora"
}

dependency "intro_ec2" {
  config_path = "../aws-ec2"
}

inputs = {

    rds_instance_id = dependency.intro_db.outputs.db_instance_identifier
    metabase_instance_id = dependency.intro_ec2.outputs.ec2_metabase_instance_id

    tags = {
        "environment_code" : local.local_env.locals.environment_code
        "stage_name": "share-service"
        "function_code": "share-service"
    }
}