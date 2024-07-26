resource "aws_s3_bucket" "public_data" {
  lifecycle {
    ignore_changes = [lifecycle_rule]
  }

  bucket = format("%s-%s", var.name, var.public_data[0].bucket_name)
  acl    = "public-read"

  versioning {
    enabled = var.public_data[0].versioning_enabled
  }

  lifecycle_rule {
    id      = "s3_lifecycle_config"
    prefix  = ""
    enabled = true

    dynamic "transition" {
      for_each = var.public_data[0].current_ver_transitions
      content {
        days          = transition.value["days"]
        storage_class = transition.value["storage_class"]
      }
    }

    dynamic "noncurrent_version_transition" {
      for_each = var.public_data[0].non_current_ver_transitions
      content {
        days          = noncurrent_version_transition.value["days"]
        storage_class = noncurrent_version_transition.value["storage_class"]
      }
    }

    dynamic "expiration" {
      for_each = var.public_data[0].expiration
      content {
        days = expiration.value["days"]
      }
    }

    dynamic "noncurrent_version_expiration" {
      for_each = var.public_data[0].non_current_ver_expiration
      content {
        days = noncurrent_version_expiration.value["days"]
      }
    }
  }

  tags = merge(
    {
      "Name" = format("%s-%s", var.name, var.public_data[0].bucket_name)
    },
    var.tags,
    var.s3_tags,
  )
}

// Allow Public Access
resource "aws_s3_bucket_ownership_controls" "public_data" {
  depends_on = [ 
    aws_s3_bucket.public_data
  ]
  bucket = aws_s3_bucket.public_data.id

  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

resource "aws_s3_bucket_public_access_block" "public_data" {

  bucket                  = format("%s-%s", var.name, var.public_data[0].bucket_name)
  block_public_acls       = false
  block_public_policy     = false
  restrict_public_buckets = false
  ignore_public_acls      = false
  depends_on = [
    aws_s3_bucket.public_data
  ]
}

resource "aws_s3_bucket_policy" "public_data" {
  depends_on = [
    aws_s3_bucket_public_access_block.public_data
  ]

  bucket = aws_s3_bucket.public_data.id
  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Principal": {
            "AWS": [
            "*"
            ]
        },
        "Action" : ["s3:GetObject"],
        "Resource" : "arn:aws:s3:::${aws_s3_bucket.public_data.id}/*"
      }
    ]
  })
}

# create public access s3 folder
resource "aws_s3_bucket_object" "public_s3_folder" {
    bucket = "${aws_s3_bucket.public_data.id}"
    acl    = "public-read"
    key    = "${var.name}_public_data_folder/"
    source = "/dev/null"
}
