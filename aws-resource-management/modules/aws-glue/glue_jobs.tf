
module "demo_glue_module" {
  source = "./modules/glue_job_module"

  glue_job_list = jsondecode(file("glue_jobs_python.json"))
  role_arn = aws_iam_role.iam_glue_job_role.arn
  name_prefix = var.name_prefix
  code_artifact_bucket = var.code_artifact_bucket
  temporary_bucket = var.temporary_bucket
  namespace = var.namespace
  tags = var.tags

}