
resource "aws_iam_role" "codepipeline_role" {
  name               = "${var.name}-pipelines-role"
  assume_role_policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {
          "Service": "codepipeline.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
      }
    ]
  })
  tags = merge(
    {
      "Name" = "${var.name}-pipelines-role"
    },
    var.tags
  )
}

resource "aws_iam_role" "codebuild_role" {
  name               = "${var.name}-codebuild-role"
  assume_role_policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {
          "Service": "codebuild.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
      }
    ]
  })

  tags = merge(
    {
      "Name" = "${var.name}-codebuild-role"
    },
    var.tags
  )
}


resource "aws_iam_role_policy" "codepipeline_policy" {
  name   = "codepipeline_policy"
  role   = aws_iam_role.codepipeline_role.id
  policy = data.aws_iam_policy_document.codepipeline_policy.json
}

resource "aws_iam_role_policy" "codebuild_policy" {
  name   = "codepipeline_policy"
  role   = aws_iam_role.codebuild_role.id
  policy = data.aws_iam_policy_document.codebuild_policy.json
}

# Create user for code commit - start
resource "aws_iam_user" "user_code_commit" {
  name = "user_code_commit"
}

resource "aws_iam_service_specific_credential" "user_code_commit_service" {
  service_name = "codecommit.amazonaws.com"
  user_name    = aws_iam_user.user_code_commit.name
}

resource "aws_iam_user_policy" "user_code_commit_policy" {
  name = "user_code_commit_policy"
  user = aws_iam_user.user_code_commit.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "codecommit:GitPush",
        ]
        Effect   = "Allow"
        Resource = "*"
      },
    ]
  })
}

resource "aws_iam_user_ssh_key" "user_code_commit" {
  username   = aws_iam_user.user_code_commit.name
  encoding   = "SSH"
  public_key = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDzsK5ZQGkuztxG0LA5CVu2Gg1lfF8w6N1VFvLIZP7K+GXwcYjJsczLq++GwpaLKtLauY6Z8dRYyyb9mQM0GMkhTVaqfDTtnAZDCUjz3FgwC7UG8v7jNPwpKpw2fw2mJx6ilttk6Xq99aRhPmqFnYzvK8PF4WoqAGxYiuqe5gqJPD2wH6MJieQbBOApq1ez0mlKFKQppdk18wkyhZd+laj9FhQHniFoCQNYTkE+cV4wyt+cVVfgIOKWYKaVIU3Wmj0e9IEewBNOUe7YkNLjVlo7Js8zQKDDyXdZkugtir/45nmUigyZIn5q4xtBuOFmL8SRtl21cwwBKHt/tc0D58ZtOORgtc7Imw0k3xUju4BLXBi+MWw9osRnryzLHnVUkZtoO5r5PzYl66ytSZP5mePmHRoAZb4ii4XR6ABRb7XDDQQXMHkUvE4sU+/9qAcJse0R8qqwycMTi2E6RXQHnj1eLG3DWjxtf2kD998Uzp3cG/JswNCFb2Qp1KhyqWh+peM= hanhtd26.id@gmail.com"
}
# Create user for code commit - end