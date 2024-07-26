// Create a security for the EC2 instances called "tutorial_web_sg"
resource "aws_security_group" "ec2_sg" {
  // Basic details like the name and description of the SG
  name        = "${var.ec2_name}-sg"
  description = "Security group for ec2 servers"
  vpc_id      = var.vpc_id

  // The first requirement we need to meet is "EC2 instances should 
  // be accessible anywhere on the internet via HTTP." So we will 
  // create an inbound rule that allows all traffic through
  // TCP port 80.
  ingress {
    description = "Allow all traffic through HTTP"
    from_port   = "80"
    to_port     = "80"
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  // The second requirement we need to meet is "Only you should be 
  // "able to access the EC2 instances via SSH." So we will create an 
  // inbound rule that allows SSH traffic ONLY from your IP address
  ingress {
    description = "Allow SSH from my computer"
    from_port   = "22"
    to_port     = "22"
    protocol    = "tcp"

    # TODO: change with specific ip
    cidr_blocks = ["0.0.0.0/0"]
    // This is using the variable "my_ip"
    # cidr_blocks = ["${var.my_ip}/32"]
  }

  // This outbound rule is allowing all outbound traffic
  // with the EC2 instances
  egress {
    description = "Allow all outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  // Here we are tagging the SG with the name "tutorial_web_sg"
  tags = merge({"Name" = "ec2_security_group"}, var.tags)
}