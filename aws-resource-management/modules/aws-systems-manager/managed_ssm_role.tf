resource "aws_iam_role" "managed_ssm" {
  path               = "/"
  assume_role_policy = <<EOL
      {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Principal": {
              "Service": ["ec2.amazonaws.com", "ssm.amazonaws.com", "rds.amazonaws.com"]
            },
            "Action": "sts:AssumeRole"
          }
        ]
      }
      EOL

  managed_policy_arns = ["arn:aws:iam::aws:policy/service-role/AmazonSSMAutomationRole"]

  inline_policy {
    name = "managed_ssm_inline_policy"

    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [
        {
          Action   = [
            "iam:PassRole",
            "rds:DescribeDBClusterParameters",
            "rds:StartDBCluster",
            "rds:StopDBCluster",
            "rds:DescribeDBEngineVersions",
            "rds:DescribeGlobalClusters",
            "rds:DescribePendingMaintenanceActions",
            "rds:DescribeDBLogFiles",
            "rds:StopDBInstance",
            "rds:StartDBInstance",
            "rds:DescribeReservedDBInstancesOfferings",
            "rds:DescribeReservedDBInstances",
            "rds:ListTagsForResource",
            "rds:DescribeValidDBInstanceModifications",
            "rds:DescribeDBInstances",
            "rds:DescribeSourceRegions",
            "rds:DescribeDBClusterEndpoints",
            "rds:DescribeDBClusters",
            "rds:DescribeDBClusterParameterGroups",
            "rds:DescribeOptionGroups"
          ]
          Effect   = "Allow"
          Resource = "*"
        },
      ]
    })
  }
}