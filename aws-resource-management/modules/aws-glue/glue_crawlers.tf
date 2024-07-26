
module "demo_crawler_module" {
  source = "./modules/glue_crawler_module"

  glue_crawlers_list = jsondecode(file("glue_crawlers.json"))
  role_arn = aws_iam_role.iam_glue_job_role.arn
  name_prefix = var.name_prefix
  bucket_name = var.data_landing_bucket
  tags = var.tags

}