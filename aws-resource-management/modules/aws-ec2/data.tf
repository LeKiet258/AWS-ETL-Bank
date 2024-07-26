data "aws_ami" "aws_freetie_ec2" {
  most_recent = true

  filter {
    name   = "name"
    values = ["Amazon Linux 2023 AMI 2023.0.20230315.0*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  owners = ["674577138207"]
}