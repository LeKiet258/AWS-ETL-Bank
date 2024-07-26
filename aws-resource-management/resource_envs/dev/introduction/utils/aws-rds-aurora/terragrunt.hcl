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
  config_path = "../../../aws-vpc"
}

# dependency "ec2" {
#   config_path = "../aws-ec2"
# }

include {
  path = find_in_parent_folders()
}

terraform {
    source = "../../../../../modules/introduction/utils//aws-rds-aurora"
}

inputs = {
    aurora_name = "${local.stage_name}-${local.function_code}-${local.aws_region_code}-${local.environment_code}-introduction-db"
    aws_region_code = local.aws_region_code

    settings_database = {
      allocated_storage   = 10            // storage in gigabytes
      engine              = "postgres"       // engine type
      engine_version      = "14.10"      // engine version
      instance_class      = "db.t3.micro" // rds instance type
      db_name             = "introduction_db"    // database name
      skip_final_snapshot = true
      username = "postgres"
      password = "postgres123"
    }

    vpc_id = dependency.share_vpc.outputs.vpc_id
    private_subnets = dependency.share_vpc.outputs.private_subnets
    public_subnets = dependency.share_vpc.outputs.public_subnets

    # ec2_sg_id = dependency.ec2.outputs.ec2_sg_id # use for security access (stardard config)
    ec2_sg_id = "none" # use for public access

    tags = {
        "environment_code" : local.local_env.locals.environment_code
        "stage_name": local.local_stage.locals.stage_name
        "function_code": local.local_function.locals.function_code
    }
}