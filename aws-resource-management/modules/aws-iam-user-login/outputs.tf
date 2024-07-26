output "user_class_arn" {
  value = aws_iam_user.user_class.*.arn
}

output "user_class_password" {
  value = { for k, v in aws_iam_user_login_profile.user_class_profile: v.user => v.password }
  # value = aws_iam_user_login_profile.user_class_profile.*.password
}

output "user_bites_password" {
  value = { for k, v in aws_iam_user_login_profile.user_bites_profile: v.user => v.password }
  # value = aws_iam_user_login_profile.user_class_profile.*.passwords
}