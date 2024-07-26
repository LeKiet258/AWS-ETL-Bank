resource "aws_iam_policy" "sfn_state_machine_policy" {
  name = format("%s-%s", var.name_prefix, "sfn-state-machine-policy")
  policy = jsonencode({
      "Statement": [
          {
              "Action": [
                  "glue:*"
              ],
              "Effect": "Allow",
              "Resource": "*"
          },
          {
              "Action": [
                  "logs:CreateLogDelivery",
                  "logs:GetLogDelivery",
                  "logs:UpdateLogDelivery",
                  "logs:DeleteLogDelivery",
                  "logs:ListLogDeliveries",
                  "logs:PutResourcePolicy",
                  "logs:DescribeResourcePolicies",
                  "logs:DescribeLogGroups",
                  "Lambda:*"
              ],
              "Effect": "Allow",
              "Resource": "*"
          },
          {
              "Action": [
                  "states:GetExecutionHistory",
                  "states:StartExecution",
                  "states:DescribeExecution",
                  "states:StartSyncExecution",
                  "states:DescribeStateMachine",
                  "states:ListExecutions",
                  "states:StopExecution"
              ],
              "Effect": "Allow",
              "Resource": [
                  "*"
              ]
          }
      ],
      "Version": "2012-10-17"
  })
}


resource "aws_iam_role" "sfn_state_machine_role" {
  name = format("%s-%s", var.name_prefix, "state-machine-role")

  assume_role_policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Action" : "sts:AssumeRole",
        "Principal" : {
          "Service" : "states.amazonaws.com"
        },
        "Effect" : "Allow",
        "Sid" : ""
      }
    ]
  })
  managed_policy_arns = [
    aws_iam_policy.sfn_state_machine_policy.arn
  ]
}