resource "aws_iam_user" "user_class" {
  count = "${length(var.usernames)}"
  name = "${element(var.usernames, count.index)}"
}


resource "aws_iam_group" "class_student_grp" {
  name = "${var.aws_profile}-class-student-grp"
}

resource "aws_iam_policy" "class_student_policy" {
  name        = "${var.aws_profile}-class-student-plcy"
  description = "student policy"
  policy      = data.aws_iam_policy_document.user_class_policy_document.json
}

resource "aws_iam_group_policy_attachment" "class_student_grp_plcy_attachment" {
  group      = aws_iam_group.class_student_grp.name
  policy_arn = aws_iam_policy.class_student_policy.arn
}

resource "aws_iam_group_membership" "class_student_membership" {
  name = "${var.aws_profile}-class-student-membership"
  users = aws_iam_user.user_class[*].name
  group = aws_iam_group.class_student_grp.name
}

resource "aws_iam_user_login_profile" "user_class_profile" {
  count = length(aws_iam_user.user_class[*].name)
  user = element(aws_iam_user.user_class[*].name, count.index)
  password_length = 8
  password_reset_required = true
}