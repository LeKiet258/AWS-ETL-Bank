locals {
  glue_crawlers_list = var.glue_crawlers_list
}

resource "aws_glue_crawler" "this" {
  count = length(local.glue_crawlers_list.*.table)
  database_name = format("%s-%s", local.glue_crawlers_list[count.index].namespace, local.glue_crawlers_list[count.index].database)
  name = format("%s-glue-cwl-%s-%s", var.name_prefix, local.glue_crawlers_list[count.index].namespace, element(local.glue_crawlers_list.*.table, count.index))
  role = var.role_arn
  schedule = lookup(local.glue_crawlers_list[count.index], "schedule", "") != "" ? local.glue_crawlers_list[count.index].schedule : ""
  
  s3_target {
    path = format("s3://%s/%s/%s", var.bucket_name, local.glue_crawlers_list[count.index].namespace, local.glue_crawlers_list[count.index].table)
  }

  schema_change_policy {
    update_behavior = lookup(local.glue_crawlers_list[count.index], "update_behavior", "") != "" ? local.glue_crawlers_list[count.index].update_behavior : "LOG"
    delete_behavior = lookup(local.glue_crawlers_list[count.index], "delete_behavior", "") != "" ? local.glue_crawlers_list[count.index].delete_behavior : "DEPRECATE_IN_DATABASE"
  }

  configuration = jsonencode(
    {
        Grouping = {
            TableGroupingPolicy = "CombineCompatibleSchemas"
        },
        Version = 1
    }
  )

  tags = var.tags
}