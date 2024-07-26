data "template_file" "step_function_config" {
  for_each = fileset(path.module, "json_config/*.json")
  template = file("${path.module}/${each.value}")
  vars = {
    name_prefix = var.name_prefix
  }
}

resource "aws_cloudwatch_log_group" "sfn_state_machine_cloudwatch_log_group" {
  name = format("%s-%s",var.name_prefix, "sfn_state_machines")
}

resource "aws_sfn_state_machine" "sfn_state_machine" {
  for_each   = fileset(path.module, "json_config/*.json")
  name       = format("%s-%s", var.name_prefix, replace(basename(each.value), ".json", ""))
  role_arn   = aws_iam_role.sfn_state_machine_role.arn
  definition = data.template_file.step_function_config[each.value].rendered
  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn_state_machine_cloudwatch_log_group.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }
}