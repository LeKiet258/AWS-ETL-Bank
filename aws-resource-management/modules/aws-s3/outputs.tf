output "terraform_state_bucket" {
  value = aws_s3_bucket.terraform_state.id
}

output "code_artifact_bucket" {
  value = aws_s3_bucket.code_artifact.id
}

output "code_artifact_bucket_arn" {
  value = aws_s3_bucket.code_artifact.arn
}

output "code_pipeline_bucket" {
  value = aws_s3_bucket.code_pipeline.id
}

output "code_pipeline_bucket_arn" {
  value = aws_s3_bucket.code_pipeline.arn
}

output "data_landing_bucket" {
  value = aws_s3_bucket.data_landing.id
}

output "data_landing_bucket_arn" {
  value = aws_s3_bucket.data_landing.arn
}

output "temporary_bucket" {
  value = aws_s3_bucket.temporary.id
}

output "temporary_bucket_arn" {
  value = aws_s3_bucket.temporary.arn
}

output "lambda_deployment_bucket" {
  value = aws_s3_bucket.lambda_deployment.id
}

output "lambda_deployment_bucket_arn" {
  value = aws_s3_bucket.lambda_deployment.arn
}