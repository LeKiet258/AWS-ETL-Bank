locals {
  glue_catalog_info = jsondecode(file("glue_catalog_databases.json"))

  data_location = [
    var.data_landing_bucket_arn
  ]

  lk_fmt_roles = [
    var.iam_glue_job_role
  ]

  new_catalog = distinct(
    flatten([
        for item in local.glue_catalog_info : [format("%s-%s", item.prefix, item.database)] if item.create_new
    ])
  )

  catalog_infos = setproduct(local.lk_fmt_roles, local.glue_catalog_info)
  location_infos =  setproduct(local.lk_fmt_roles, local.data_location)
}

resource "aws_glue_catalog_database" "ai4i_demo_database" {
  count = length(local.new_catalog)
  name = local.new_catalog[count.index]
}


resource "aws_lakeformation_permissions" "database_permissions" {
  count = length(local.catalog_infos)
  principal = local.catalog_infos[count.index][0]
  permissions = ["ALTER", "CREATE_TABLE", "DESCRIBE", "DROP"]
  permissions_with_grant_option = ["ALTER", "CREATE_TABLE", "DESCRIBE", "DROP"]

  database {
    name = format("%s-%s", local.catalog_infos[count.index][1].prefix, local.catalog_infos[count.index][1].database)
    catalog_id = data.aws_caller_identity.current.account_id
  }

  depends_on = [ aws_glue_catalog_database.ai4i_demo_database ]
}

resource "aws_lakeformation_permissions" "table_permissions" {
  count = length(local.catalog_infos)
  principal = local.catalog_infos[count.index][0]
  permissions = ["ALTER", "DELETE", "DESCRIBE", "DROP", "INSERT", "SELECT"]
  permissions_with_grant_option = ["ALTER", "DELETE", "DESCRIBE", "DROP", "INSERT", "SELECT"]

  table {
    database_name = format("%s-%s", local.catalog_infos[count.index][1].prefix, local.catalog_infos[count.index][1].database)
    wildcard = true
    catalog_id = data.aws_caller_identity.current.account_id
  }

  depends_on = [ aws_glue_catalog_database.ai4i_demo_database ]
}

resource "aws_lakeformation_resource" "data_location_register" {
    count = length(local.data_location)
    arn = local.data_location[count.index]
}

resource "aws_lakeformation_permissions" "data_location_permissions" {
  count = length(local.location_infos)
  principal = local.location_infos[count.index][0]
  permissions = ["DATA_LOCATION_ACCESS"]

  data_location {
    arn = local.location_infos[count.index][1]
    catalog_id = data.aws_caller_identity.current.account_id
  }

  depends_on = [ aws_glue_catalog_database.ai4i_demo_database, aws_lakeformation_resource.data_location_register]
}