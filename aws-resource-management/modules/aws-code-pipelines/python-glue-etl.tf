
resource "aws_codecommit_repository" "python_repo" {
  repository_name = var.python_repo_name
  description     = "This is the code commit Repository for ${var.python_repo_name}"
}

resource "aws_codebuild_project" "code_build_python_repo" {
  name            = format("%s-%s-code-blt", var.name, var.python_repo_name)
  description     = "code build project for ${var.python_repo_name} repo"
  build_timeout   = "30"
  queued_timeout  = 60

  service_role    = aws_iam_role.codebuild_role.arn

  artifacts {
    type = var.codebuild_type
  }

  cache {
    type = "S3"
    location = var.code_pipeline_bucket
  }

  environment {
    compute_type                      = var.codebuild_general1_small_compute_type
    image                             = var.codebuild_image
    type                              = var.codebuild_container_type
    image_pull_credentials_type       = var.codebuild_image_pull_credentials_type
    privileged_mode                   = false

    environment_variable {
      name = "ENV_CODE"
      value = var.environment_code
    }

    environment_variable {
      name = "NAMESPACE"
      value = var.namespace
    }
    environment_variable {
      name = "REPO_NAME"
      value = var.python_repo_name
    }

    environment_variable {
      name = "ARTIFACT_BUCKET_NAME"
      value = var.code_artifact_bucket
    }

    environment_variable {
      name = "DATA_LANDING_BUCKET_NAME"
      value = var.data_landing_bucket
    }
  }

  logs_config {
    cloudwatch_logs {
      
    }
    s3_logs {
      status = "ENABLED"
      location = "${var.code_pipeline_bucket}/build-log/${var.python_repo_name}"
    }
  }

  source {
    type = var.codebuild_type
    buildspec = var.codebuild_buildspec
  }

  tags = merge(
    {
      "Name" = format("%s-%s-code-blt", var.name, var.python_repo_name)
    },
    var.tags
  )
}

resource "aws_codepipeline" "codepipeline_python_etl" {
  name     = "${var.name}-python-pipeline"
  role_arn = aws_iam_role.codepipeline_role.arn

  artifact_store {
    location = var.code_artifact_bucket
    type     = "S3"
  }

  stage {
    name = "Source"

    action {
      name             = "Source"
      category         = "Source"
      owner            = "AWS"
      provider         = "CodeCommit"
      version          = "1"
      namespace        = "SourceVariables"
      output_artifacts = ["source_output"]

      configuration = {
        RepositoryName          = aws_codecommit_repository.python_repo.repository_name
        BranchName              = var.code_branch
        OutputArtifactFormat    = "CODE_ZIP"
        PollForSourceChanges    = true
      }
    }
  }

  stage {
    name = "Build"

    action {
      name             = "Build"
      category         = "Build"
      owner            = "AWS"
      provider         = "CodeBuild"
      input_artifacts  = ["source_output"]
      output_artifacts = ["deploy_output"]
      version          = "1"
      namespace        = "BuildVariables"

      configuration    = {
        ProjectName    = aws_codebuild_project.code_build_python_repo.name
        PrimarySource  = "source_output"
      }
    }
  }
}
