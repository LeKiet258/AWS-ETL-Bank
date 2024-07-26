locals {
  # Automatically load environment-level variables
  environment_vars = read_terragrunt_config(find_in_parent_folders("env.hcl"))

  # extract local_env variables
  environment           = local.environment_vars.locals.environment
  environment_code      = local.environment_vars.locals.environment_code
  aws_account_id        = local.environment_vars.locals.aws_account_id
  aws_account_owner        = local.environment_vars.locals.aws_account_owner
  aws_profile           = local.environment_vars.locals.aws_profile
  aws_region_code       = local.environment_vars.locals.aws_region_code
}

# Generate an AWS provider block
generate "provider" {
  path      = "provider.tf"
  if_exists = "overwrite_terragrunt"
  contents  = <<EOF
provider "aws" {
  region = "${local.aws_region_code}"
  # Only these AWS Account IDs may be operated on by this template
  allowed_account_ids = ["${local.aws_account_id}"]
}
EOF
}

 generate "versions" {
   path      = "versions.tf"
   if_exists = "overwrite_terragrunt"
   contents  = <<EOF
 terraform {
   required_providers {
     aws = "4.65.0" # latest version 3 (1 May 2023)
   }
 }
 EOF
 }

# Configure Terragrunt to automatically store tfstate files in an S3 bucket
remote_state {
  backend = "s3"
  config = {
    encrypt        = true
    bucket         = "${local.aws_account_owner}-${local.aws_region_code}-${local.environment_code}-s3-terraform-state"
    key            = "${path_relative_to_include()}/terraform.tfstate"
    region         = "${local.aws_region_code}"
    dynamodb_table = "terraform-locks"
  }
  generate = {
    path      = "backend.tf"
    if_exists = "overwrite_terragrunt"
  }
}


# ---------------------------------------------------------------------------------------------------------------------
# GLOBAL PARAMETERS
# These variables apply to all configurations in this subfolder. These are automatically merged into the child
# `terragrunt.hcl` config via the include block.
# ---------------------------------------------------------------------------------------------------------------------

# Configure root level variables that all resources can inherit. This is especially helpful with multi-account configs
# where terraform_remote_state data sources are placed directly into the modules.
inputs = merge(
  # local.account_vars.locals,
  # local.region_vars.locals,
  local.environment_vars.locals,
)
