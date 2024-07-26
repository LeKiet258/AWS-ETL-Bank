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
    source = "../../../modules//aws-vpc"
}

inputs = {
    vpc_name = "${local.aws_profile}-${local.aws_region_code}-${local.environment_code}-vpc"
    vpc_cidr_block = "10.0.0.0/16"

    public_subnet_name = "${local.aws_profile}-${local.aws_region_code}-${local.environment_code}-public-subnet"
    private_subnet_name = "${local.aws_profile}-${local.aws_region_code}-${local.environment_code}-private-subnet"
    public_subnet_cidrs = ["10.0.1.0/24", "10.0.2.0/24"]
    private_subnet_cidrs = ["10.0.101.0/24", "10.0.102.0/24"]
    azs = ["${local.aws_region_code}a", "${local.aws_region_code}b", "${local.aws_region_code}c"]

    aws_region_code = local.aws_region_code

    tags = {
        "environment_code" : local.local_env.locals.environment_code
        "stage_name": "share-service"
        "function_code": "share-service"
    }
}