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
    source = "../../../modules//aws-iam-user-login"
}

dependency "s3" {
    config_path = "../aws-s3"
}

inputs = {
    aws_profile = "${local.aws_profile}"
    usernames = [
        "class_tester",
        "leyennhi112",
        "lekietvn",
        "quanghuy4919",
        "vutran1908",
        "nlong2232003",
        "anhkiet10h1nd",
        "giakhanh2091",
        "phuonganh180402",
        "minhtanpham379",
        "kiet.tt0710",
        "trinhbinhnguyen308",
        "phamductai.0803.24",
        "it.nccuong",
        "thaihoa.i2r",
        "sontungkieu412",
        "henryph.it",
        "nguyenducnam2632002",
        "hoangngoc250298",
        "nmq31012004",
        "cungtronghau",
        "vansang97.khtn"
    ]

    usernames_bites = [
        "bites",
        "tester",
        "phuongdth"
    ]
    environment_code = local.environment_code
    aws_region_code = local.aws_region_code

    code_artifact_bucket_arn = dependency.s3.outputs.code_artifact_bucket_arn
    data_landing_bucket_arn = dependency.s3.outputs.data_landing_bucket_arn
    temporary_bucket_arn = dependency.s3.outputs.temporary_bucket_arn

    tags = {
        "environment_code" : local.local_env.locals.environment_code
        "stage_name": "share-service"
        "function_code": "share-service"
    }
}