locals {
    local_env = read_terragrunt_config(find_in_parent_folders("env.hcl"))
    local_stage = read_terragrunt_config(find_in_parent_folders("stage.hcl"))
    local_function = read_terragrunt_config(find_in_parent_folders("function.hcl"))

    # extract local_env variables
    environment           = local.local_env.locals.environment
    environment_code      = local.local_env.locals.environment_code
    aws_account_id        = local.local_env.locals.aws_account_id
    aws_profile           = local.local_env.locals.aws_profile
    aws_region_code       = local.local_env.locals.aws_region_code

    # extract local_stage variables
    stage_name           = local.local_stage.locals.stage_name

    # extract local_function variables
    function_name = local.local_function.locals.function_name
    function_code = local.local_function.locals.function_code

}

dependency "share_vpc" {
  config_path = "../aws-vpc"
}

dependency "intro_db" {
  config_path = "../introduction/utils/aws-rds-aurora"
}

include {
  path = find_in_parent_folders()
}

terraform {
    source = "../../../modules//aws-ec2"
}

inputs = {
    ec2_name = "${local.stage_name}-${local.function_code}-${local.aws_region_code}-${local.environment_code}-ec2"
    aws_region_code = local.aws_region_code

    vpc_id = dependency.share_vpc.outputs.vpc_id
    private_subnets = dependency.share_vpc.outputs.private_subnets
    public_subnets = dependency.share_vpc.outputs.public_subnets
    aws_account_id = local.aws_account_id

    # DB conf
    intro_db_host_name = dependency.intro_db.outputs.db_hostname
    intro_db_port = dependency.intro_db.outputs.db_port
    intro_db_username = dependency.intro_db.outputs.db_username
    intro_db_password = dependency.intro_db.outputs.db_password

    tags = {
        "environment_code" : local.local_env.locals.environment_code
        "stage_name": local.local_stage.locals.stage_name
        "function_code": local.local_function.locals.function_code
    }
}