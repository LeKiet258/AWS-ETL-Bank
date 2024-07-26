
data "aws_iam_policy_document" "user_class_policy_document" {
    # iam
    statement {
        effect    = "Allow"
        actions   = [
            # iam allow change password at first login
            "iam:ChangePassword"
        ]
        resources = [
            "arn:aws:iam::*"
        ]
    }

    # rds policy
    statement {
        effect    = "Allow"
        actions   = [
            "rds:Describe*",
            "rds:DownloadCompleteDBLogFile",
            "rds:DownloadDBLogFilePortion",
            "rds:ListTagsForResource"
        ]

        resources = [
            "arn:aws:rds::*"
        ]
    }

    # ec2 policy
    statement {
        effect    = "Allow"
        actions   = [
            "ec2:Describe*",
            "ec2:Get*",
            "ec2:Export*",
        ]

        resources = [
            "arn:aws:ec2::"
        ]
    }

    # s3 policy
    statement {
        effect    = "Allow"
        actions   = [
            "s3:ListBucket",
            "s3:ListAllMyBuckets"
        ]

        resources = [
            "arn:aws:s3:::*"
        ]
    }
    statement {
        effect    = "Allow"
        actions   = [
            "s3:GetObject",
            "s3:*"
        ]

        resources = [
            var.code_artifact_bucket_arn,
            "${var.code_artifact_bucket_arn}/*",
            var.data_landing_bucket_arn,
            "${var.data_landing_bucket_arn}/*",
            var.temporary_bucket_arn,
            "${var.temporary_bucket_arn}/*"

        ]
    }

    # lambda policy
    statement {
        effect    = "Allow"
        actions   = [
            "lambda:*"
        ]

        resources = [
            "*"
        ]
    }

    # stepfunction policy
    statement {
        effect    = "Allow"
        actions   = [
            "states:*"
        ]

        resources = [
            "*"
        ]
    }

    # glue policy
    statement {
        effect    = "Allow"
        actions   = [
            "glue:Get*",
            "glue:List*",
            "glue:SearchTables"
        ]

        resources = [
            "arn:aws:glue:ap-southeast-1:*:catalog",
            "arn:aws:glue:ap-southeast-1:*:database/ai4e-*",
            "arn:aws:glue:ap-southeast-1:*:table/ai4e-*/*",
        ]
    }

    # code pipelines policy
    statement {
        effect    = "Allow"
        actions   = [
            "codecommit:Get*",
            "codecommit:List*",
            "codecommit:Describe*",
            "codepipeline:Get*",
            "codepipeline:List*",
            "codepipeline:Describe*",
            "codebuild:Get*",
            "codebuild:List*",
            "codebuild:Describe*"

        ]

        resources = [
            "*"
        ]
    }

    # lakeformation policy
    statement {
        effect    = "Allow"
        actions   = [
            "lakeformation:Get*",
            "lakeformation:List*",
            "lakeformation:Search*"
        ]

        resources = [
            "*"
        ]
    }

    statement {
        effect    = "Allow"
        actions   = [
            "athena:StartQueryExecution",
            "athena:Get*",
            "athena:DeleteNamedQuery",
            "athena:List*",
            "athena:StopQueryExecution",
            "athena:CreateNamedQuery",
            "athena:BatchGetNamedQuery",
            "athena:BatchGetQueryExecution"
        ]

        resources = [
            "arn:aws:athena:*:*:workgroup/*"
        ]
    }

    statement {
        effect    = "Allow"
        actions   = [
            "s3:PutObject",
            "s3:GetObject",
            "s3:ListBucketMultipartUploads",
            "s3:AbortMultipartUpload",
            "s3:CreateBucket",
            "s3:ListBucket",
            "s3:GetBucketLocation",
            "s3:ListMultipartUploadParts"
        ]

        resources = [
            "arn:aws:s3:::aws-athena-query-results-*"
        ]
    }

    # cloudwatch/user policy
    statement {
        effect    = "Allow"
        actions   = [
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents",
            "logs:DescribeLogStreams",
            "logs:DescribeLogGroups",
            "logs:GetLogEvents",
            "logs:PutRetentionPolicy",
            "cloudwatch:*"
        ]

        resources = [
            "*"
        ]
    }

    # Eventbridge
    statement {
        effect    = "Allow"
        actions   = [
            "events:Get*",
            "events:List*",
            "events:TestEventPattern",
            "schemas:DescribeRegistry",
            "pipes:*"
        ]

        resources = [
            "*"
        ]
    }

    # AccessKey
    statement {
        effect    = "Allow"
        actions   = [
            "iam:CreateAccessKey",
            "iam:DeleteAccessKey",
            "iam:GetAccessKeyLastUsed",
            "iam:GetUser",
            "iam:ListAccessKeys",
            "iam:UpdateAccessKey",
            "iam:TagUser"
        ]

        resources = [for i in var.usernames: "arn:aws:iam::*:user/${i}"]
    }
}



data "aws_iam_policy_document" "user_bites_policy_document" {
    # iam
    statement {
        effect    = "Allow"
        actions   = [
            # iam allow change password at first login
            "iam:ChangePassword"
        ]
        resources = [
            "arn:aws:iam::*"
        ]
    }

    # rds policy
    statement {
        effect    = "Allow"
        actions   = [
            "rds:Describe*",
            "rds:DownloadCompleteDBLogFile",
            "rds:DownloadDBLogFilePortion",
            "rds:ListTagsForResource"
        ]

        resources = [
            "arn:aws:rds::*"
        ]
    }

    # ec2 policy
    statement {
        effect    = "Allow"
        actions   = [
            "ec2:Describe*",
            "ec2:Get*",
            "ec2:Export*",
        ]

        resources = [
            "arn:aws:ec2::"
        ]
    }

    # s3 policy
    statement {
        effect    = "Allow"
        actions   = [
            "s3:ListBucket",
            "s3:ListAllMyBuckets"
        ]

        resources = [
            "arn:aws:s3:::*"
        ]
    }
    statement {
        effect    = "Allow"
        actions   = [
            "s3:GetObject",
            "s3:*"
        ]

        resources = [
            var.code_artifact_bucket_arn,
            "${var.code_artifact_bucket_arn}/*",
            var.data_landing_bucket_arn,
            "${var.data_landing_bucket_arn}/*",
            var.temporary_bucket_arn,
            "${var.temporary_bucket_arn}/*"

        ]
    }

    # lambda policy
    statement {
        effect    = "Allow"
        actions   = [
            "lambda:*"
        ]

        resources = [
            "*"
        ]
    }

    # stepfunction policy
    statement {
        effect    = "Allow"
        actions   = [
            "states:*"
        ]

        resources = [
            "*"
        ]
    }

    # glue policy
    statement {
        effect    = "Allow"
        actions   = [
            "glue:Get*",
            "glue:List*",
            "glue:SearchTables"
        ]

        resources = [
            "arn:aws:glue:ap-southeast-1:*:catalog",
            "arn:aws:glue:ap-southeast-1:*:database/bites-*",
            "arn:aws:glue:ap-southeast-1:*:table/bites-*/*",
        ]
    }

    # code pipelines policy
    statement {
        effect    = "Allow"
        actions   = [
            "codecommit:Get*",
            "codecommit:List*",
            "codecommit:Describe*",
            "codepipeline:Get*",
            "codepipeline:List*",
            "codepipeline:Describe*",
            "codebuild:Get*",
            "codebuild:List*",
            "codebuild:Describe*"

        ]

        resources = [
            "*"
        ]
    }

    # lakeformation policy
    statement {
        effect    = "Allow"
        actions   = [
            "lakeformation:Get*",
            "lakeformation:List*",
            "lakeformation:Search*"
        ]

        resources = [
            "*"
        ]
    }

    statement {
        effect    = "Allow"
        actions   = [
            "athena:StartQueryExecution",
            "athena:Get*",
            "athena:DeleteNamedQuery",
            "athena:List*",
            "athena:StopQueryExecution",
            "athena:CreateNamedQuery",
            "athena:BatchGetNamedQuery",
            "athena:BatchGetQueryExecution"
        ]

        resources = [
            "arn:aws:athena:*:*:workgroup/*"
        ]
    }

    statement {
        effect    = "Allow"
        actions   = [
            "s3:PutObject",
            "s3:GetObject",
            "s3:ListBucketMultipartUploads",
            "s3:AbortMultipartUpload",
            "s3:CreateBucket",
            "s3:ListBucket",
            "s3:GetBucketLocation",
            "s3:ListMultipartUploadParts"
        ]

        resources = [
            "arn:aws:s3:::aws-athena-query-results-*"
        ]
    }

    # cloudwatch/user policy
    statement {
        effect    = "Allow"
        actions   = [
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents",
            "logs:DescribeLogStreams",
            "logs:DescribeLogGroups",
            "logs:GetLogEvents",
            "logs:PutRetentionPolicy",
            "cloudwatch:*"
        ]

        resources = [
            "*"
        ]
    }

    # Eventbridge
    statement {
        effect    = "Allow"
        actions   = [
            "events:Get*",
            "events:List*",
            "events:TestEventPattern",
            "schemas:DescribeRegistry",
            "pipes:*"
        ]

        resources = [
            "*"
        ]
    }
}
