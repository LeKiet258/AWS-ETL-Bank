resource "aws_s3_bucket" "temporary" {
  lifecycle {
    ignore_changes = [lifecycle_rule]
  }

  bucket = format("%s-%s", var.name, var.temporary[0].bucket_name)
  acl    = "private"

  versioning {
    enabled = var.temporary[0].versioning_enabled
  }

  lifecycle_rule {
    id      = "s3_lifecycle_config"
    prefix  = ""
    enabled = true

    dynamic "transition" {
      for_each = var.temporary[0].current_ver_transitions
      content {
        days          = transition.value["days"]
        storage_class = transition.value["storage_class"]
      }
    }

    dynamic "noncurrent_version_transition" {
      for_each = var.temporary[0].non_current_ver_transitions
      content {
        days          = noncurrent_version_transition.value["days"]
        storage_class = noncurrent_version_transition.value["storage_class"]
      }
    }

    dynamic "expiration" {
      for_each = var.temporary[0].expiration
      content {
        days = expiration.value["days"]
      }
    }

    dynamic "noncurrent_version_expiration" {
      for_each = var.temporary[0].non_current_ver_expiration
      content {
        days = noncurrent_version_expiration.value["days"]
      }
    }
  }

  tags = merge(
    {
      "Name" = format("%s-%s", var.name, var.temporary[0].bucket_name)
    },
    var.tags,
    var.s3_tags,
  )
}

// Block Public Access
resource "aws_s3_bucket_public_access_block" "temporary" {

  bucket                  = format("%s-%s", var.name, var.temporary[0].bucket_name)
  block_public_acls       = true
  block_public_policy     = true
  restrict_public_buckets = true
  ignore_public_acls      = true
  depends_on = [
    aws_s3_bucket.temporary
  ]
}

resource "aws_s3_bucket_policy" "temporary" {
  depends_on = [
    aws_s3_bucket_public_access_block.temporary
  ]

  bucket = aws_s3_bucket.temporary.id
  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Deny",
        "Principal" : "*",
        "Action" : "s3:*",
        "Resource" : "arn:aws:s3:::${aws_s3_bucket.temporary.id}/*",
        "Condition" : {
          "Bool" : {
            "aws:SecureTransport" : "false"
          }
        }
      }
    ]
  })
}
