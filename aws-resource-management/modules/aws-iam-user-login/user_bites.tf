resource "aws_iam_user" "user_bites" {
  count = "${length(var.usernames_bites)}"
  name = "${element(var.usernames_bites, count.index)}"
}


resource "aws_iam_group" "bites_grp" {
  name = "${var.aws_profile}-bites-grp"
}

resource "aws_iam_policy" "bites_policy" {
  name        = "${var.aws_profile}-bites-plcy"
  description = "student policy"
  policy      = data.aws_iam_policy_document.user_bites_policy_document.json
}

resource "aws_iam_group_policy_attachment" "bites_grp_plcy_attachment" {
  group      = aws_iam_group.bites_grp.name
  policy_arn = aws_iam_policy.bites_policy.arn
}

resource "aws_iam_group_membership" "bites_membership" {
  name = "${var.aws_profile}-bites-membership"
  users = aws_iam_user.user_bites[*].name
  group = aws_iam_group.bites_grp.name
}

resource "aws_iam_user_login_profile" "user_bites_profile" {
  count = length(aws_iam_user.user_bites[*].name)
  user = element(aws_iam_user.user_bites[*].name, count.index)
  password_length = 8
  password_reset_required = true
}