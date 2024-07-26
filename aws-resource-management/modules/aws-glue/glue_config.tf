# resource "aws_glue_security_configuration" "glue_security_config" {
#   name = format("%s-glue-security-config", var.name_prefix)

#   encryption_configuration {
#     cloudwatch_encryption {
#       cloudwatch_encryption_mode = "DISABLED"
#     }
#     job_bookmarks_encryption {
#       job_bookmarks_encryption_mode = "DISABLED"
#     }
#   }
# }

resource "aws_glue_connection" "glue_connection" {
  name = "${var.name_prefix}-data-subnet-b"
  connection_type = "NETWORK"

  physical_connection_requirements {
    availability_zone = var.aws_region_code
    security_group_id_list = [var.default_security_group_id]
    subnet_id = var.private_subnets[0]
  }
}