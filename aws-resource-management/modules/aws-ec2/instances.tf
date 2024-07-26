resource "aws_key_pair" "ec2_metabase_kp" {
  key_name   = "ec2_metabase_key_pair"
  public_key = file("keypair.pub")
}


resource "aws_instance" "ec2_metabase" {
  ami           = data.aws_ami.aws_freetie_ec2.id
  instance_type          = "t3a.small"
  subnet_id              = var.public_subnets[0]
  key_name               = aws_key_pair.ec2_metabase_kp.key_name
  vpc_security_group_ids = [aws_security_group.ec2_sg.id]

  user_data = <<-EOL
  #!/bin/bash -xe

  sudo yum update
  sudo yum install -y docker
  sudo usermod -a -G docker ec2-user
  id ec2-user
  newgrp docker
  sudo systemctl enable docker.service
  sudo systemctl start docker.service
  docker pull metabase/metabase:latest
  docker run -d --restart always -p 80:3000 \
    -e "MB_DB_TYPE=postgres" \
    -e "MB_DB_DBNAME=metabaseappdb" \
    -e "MB_DB_PORT=${var.intro_db_port}" \
    -e "MB_DB_USER=${var.intro_db_username}" \
    -e "MB_DB_PASS=${var.intro_db_password}" \
    -e "MB_DB_HOST=${var.intro_db_host_name}" \
    --name metabase metabase/metabase
  EOL

  tags = {
    Name = "ec2_metabase"
  }
}

# create elastic ip
resource "aws_eip" "ec2_eip" {
  tags = {
    Name = "eip_vpc"
  }
}

resource "aws_eip_association" "metabase_eip_asco" {
  instance_id   = aws_instance.ec2_metabase.id
  allocation_id = aws_eip.ec2_eip.id
  depends_on = [ aws_instance.ec2_metabase ]
}