output "ipv4_ec2_instance" {
  // We are grabbing it from the Elastic IP
  value       = aws_eip.ec2_eip.public_ip
  depends_on = [aws_eip.ec2_eip]
}

output "ec2_sg_id" {
  value = aws_security_group.ec2_sg.id
}

output "ec2_instance_public_dns" {
  description = "The public DNS address of the web server"
  // We are grabbing it from the Elastic IP
  value       = aws_eip.ec2_eip.public_dns
  depends_on = [aws_eip.ec2_eip]
}

output "ec2_metabase_instance_id" {
  value = aws_instance.ec2_metabase.id
  depends_on = [ aws_instance.ec2_metabase ]
}