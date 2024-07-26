output "database_arn" {
  value = { for k, v in aws_glue_catalog_database.ai4i_demo_database: v.id => v.arn }
}
