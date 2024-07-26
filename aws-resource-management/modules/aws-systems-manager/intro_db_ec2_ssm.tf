# NOTE: !IMPORTANT
# Each member have to be applied separately - DO NOT apply all
# GMT 0 -> minus 7 to be UTC+7


resource "aws_ssm_association" "start_ec2_metabase" {
  association_name = "Start-Metabase-EC2"
  name = "AWS-StartEC2Instance"

  # automation_target_parameter_name = "InstanceId"

  # Cron expression example
  schedule_expression = "cron(30 2 * * ? *)"

  parameters = {
    AutomationAssumeRole = aws_iam_role.managed_ssm.arn
    InstanceId = var.metabase_instance_id
  }

  # targets {
  #   key    = "tag:Name"
  #   values = ["ec2_metabase"]
  # }
}

resource "aws_ssm_association" "stop_ec2_metabase" {
  association_name = "Stop-Metabase-EC2"
  name = "AWS-StopEC2Instance"

  # automation_target_parameter_name = "InstanceId"

  # Cron expression example
  schedule_expression = "cron(0 15 * * ? *)"

  parameters = {
    AutomationAssumeRole = aws_iam_role.managed_ssm.arn
    InstanceId = var.metabase_instance_id
  }

  # targets {
  #   key    = "tag:Name"
  #   values = ["ec2_metabase"]
  # }
}

resource "aws_ssm_association" "start_rds_metabase" {
  association_name = "Start-Metabase-RDS"
  name = "AWS-StartRdsInstance"

  # automation_target_parameter_name = "InstanceId"

  # Cron expression example
  schedule_expression = "cron(15 2 * * ? *)"

  parameters = {
    AutomationAssumeRole = aws_iam_role.managed_ssm.arn
    InstanceId = var.rds_instance_id
  }

  # targets {
  #   key    = "tag:Name"
  #   values = ["introduction_database"]
  # }
}

resource "aws_ssm_association" "stop_rds_metabase" {
  association_name = "Stop-Metabase-RDS"
  name = "AWS-StopRdsInstance"

  # automation_target_parameter_name = "InstanceId"

  # Cron expression example
  schedule_expression = "cron(0 15 * * ? *)"

  parameters = {
    AutomationAssumeRole = aws_iam_role.managed_ssm.arn
    InstanceId = var.rds_instance_id
  }

  # targets {
  #   key    = "tag:Name"
  #   values = ["introduction_database"]
  # }
}