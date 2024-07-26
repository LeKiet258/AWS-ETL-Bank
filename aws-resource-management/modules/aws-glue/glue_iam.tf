resource "aws_iam_policy" "datalake_read_write_access" {
  name = format("%s-datalake-read-write-access", var.aws_profile)
  path = "/"
  description = "policy read write access for athena"
  policy = jsonencode(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "athena:GetQueryResults",
                    "athena:GetQueryResultsStream",
                    "athena:ListNameQueries",
                    "athena:GetNamedQuery",
                    "athena:CreateNamedQuery",
                    "athena:DeleteNamedQuery",
                    "athena:BatchGetNamedQuery",
                    "athena:BatchGetQueryExecution",
                    "athena:StartQueryExecution",
                    "athena:ListQueryExecutions",
                    "athena:GetQueryExecution",
                    "athena:StopQueryExecution",
                    "athena:ListPreparedStatements",
                    "athena:CreatePreparedStatement",
                    "athena:UpdatePreparedStatement",
                    "athena:GetPreparedStatement",
                    "athena:DeletePreparedStatement"
                ],
                "Resource": [
                    "arn:aws:athena:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:workgroup/primary"
                ]
            }, {
                "Effect": "Allow",
                "Action": [
                    "s3:ListBucket",
                    "s3:AbortMultipartUpload",
                    "s3:ListBucketMultipartUploads",
                    "s3:ListMultipartUploadParts",
                    "s3:PutObject",
                    "s3:PutObjectAcl",
                    "s3:GetObject",
                    "s3:GetObjectLocation",
                    "s3:GetBucketAcl",
                    "s3:DeleteObject"
                ],
                "Resource": [
                    "${var.data_landing_bucket_arn}/*",
                    "${var.code_artifact_bucket_arn}/*"
                ]
            }
        ]
    }
  )

  tags = merge(
    {
        "Name" = format("%s-datalake-read-write-access", var.aws_profile)
    },
    var.tags
  )
}


resource "aws_iam_role" "iam_glue_job_role" {
  name = format("%s-iam-glue-job-role", var.name_prefix)

  assume_role_policy = jsonencode(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "glue.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
  )

  managed_policy_arns = [
    aws_iam_policy.datalake_read_write_access.arn,
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  ]

  tags = merge(
    {
        "Name" = format("%s-iam-glue-job-role", var.name_prefix)
    },
    var.tags
  )
}