
//
// Environment Prefix
//
variable "name" {
  description = "A name can be reused across resources in this module."
  type        = string
}

variable "tf_state_bucket" {
  description = "A tf_state_bucket name can be reused across resources in this module."
  type        = string
}

variable "environment_code" {
  description = "Prefix for the S3 buckets"
  type        = string
}

variable "tags" {
  description = "A map of tags to add to all resources"
  type        = map(string)
}

variable "s3_tags" {
  description = "Additional tags for the S3 Buckets"
  type        = map(string)
  default     = {}
}

variable "aws_region_code" {
  type = string
}

//
// S3 Buckets
//

variable "logs" {
  description = ""
  type        = list(any)
  default = [{
    bucket_name = "logs"
    # versioning_enabled = true
  }]
}

variable "terraform_state" {
  description = ""
  type        = list(any)
  default = [{
    bucket_name = "terraform-state"
    versioning_enabled = true
    expiration         = []

    current_ver_transitions = [
      {
        days          = 0
        storage_class = "INTELLIGENT_TIERING"
      }
    ]
    non_current_ver_transitions = []
    expiration                  = []
    non_current_ver_expiration  = []
    intelligent_tiering = [
      {
        days        = 180
        access_tier = "ARCHIVE_ACCESS"
      },
      {
        days        = 360
        access_tier = "DEEP_ARCHIVE_ACCESS"
      }
    ]
  }]
}

variable "code_artifact" {
  description = ""
  type        = list(any)
  default = [{
    bucket_name = "code-artifact"
    versioning_enabled = true
    expiration         = []

    current_ver_transitions = [
      {
        days          = 0
        storage_class = "INTELLIGENT_TIERING"
      }
    ]
    non_current_ver_transitions = []
    expiration                  = []
    non_current_ver_expiration  = []
    intelligent_tiering = [
      {
        days        = 180
        access_tier = "ARCHIVE_ACCESS"
      },
      {
        days        = 360
        access_tier = "DEEP_ARCHIVE_ACCESS"
      }
    ]
  }]
}

variable "code_pipeline" {
  description = ""
  type        = list(any)
  default = [{
    bucket_name = "code-pipeline"
    versioning_enabled = true
    expiration         = []

    current_ver_transitions = [
      {
        days          = 0
        storage_class = "INTELLIGENT_TIERING"
      }
    ]
    non_current_ver_transitions = []
    expiration                  = []
    non_current_ver_expiration  = []
    intelligent_tiering = [
      {
        days        = 180
        access_tier = "ARCHIVE_ACCESS"
      },
      {
        days        = 360
        access_tier = "DEEP_ARCHIVE_ACCESS"
      }
    ]
  }]
}

variable "data_landing" {
  description = ""
  type        = list(any)
  default = [{
    bucket_name = "data-landing"
    versioning_enabled = true
    expiration         = []

    current_ver_transitions = [
      {
        days          = 0
        storage_class = "INTELLIGENT_TIERING"
      }
    ]
    non_current_ver_transitions = []
    expiration                  = []
    non_current_ver_expiration  = []
    intelligent_tiering = [
      {
        days        = 180
        access_tier = "ARCHIVE_ACCESS"
      },
      {
        days        = 360
        access_tier = "DEEP_ARCHIVE_ACCESS"
      }
    ]
  }]
}

variable "temporary" {
  description = ""
  type        = list(any)
  default = [{
    bucket_name = "temporary"
    versioning_enabled = true
    expiration         = []

    current_ver_transitions = [
      {
        days          = 0
        storage_class = "INTELLIGENT_TIERING"
      }
    ]
    non_current_ver_transitions = []
    expiration                  = []
    non_current_ver_expiration  = []
    intelligent_tiering = [
      {
        days        = 180
        access_tier = "ARCHIVE_ACCESS"
      },
      {
        days        = 360
        access_tier = "DEEP_ARCHIVE_ACCESS"
      }
    ]
  }]
}

variable "lambda_deployment" {
  description = ""
  type        = list(any)
  default = [{
    bucket_name = "lambda-deployment"
    versioning_enabled = true
    expiration         = []

    current_ver_transitions = [
      {
        days          = 0
        storage_class = "INTELLIGENT_TIERING"
      }
    ]
    non_current_ver_transitions = []
    expiration                  = []
    non_current_ver_expiration  = []
    intelligent_tiering = [
      {
        days        = 180
        access_tier = "ARCHIVE_ACCESS"
      },
      {
        days        = 360
        access_tier = "DEEP_ARCHIVE_ACCESS"
      }
    ]
  }]
}

variable "public_data" {
  description = ""
  type        = list(any)
  default = [{
    bucket_name = "public-data"
    versioning_enabled = true
    expiration         = []

    current_ver_transitions = [
      {
        days          = 0
        storage_class = "INTELLIGENT_TIERING"
      }
    ]
    non_current_ver_transitions = []
    expiration                  = []
    non_current_ver_expiration  = []
    intelligent_tiering = [
      {
        days        = 180
        access_tier = "ARCHIVE_ACCESS"
      },
      {
        days        = 360
        access_tier = "DEEP_ARCHIVE_ACCESS"
      }
    ]
  }]
}