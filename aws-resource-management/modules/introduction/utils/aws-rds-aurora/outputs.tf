output "db_hostname" {
  value       = aws_db_instance.introduction_db.address
}

output "db_port" {
  value       = aws_db_instance.introduction_db.port
}

output "db_username" {
    value = aws_db_instance.introduction_db.username
}

output "db_password" {
  sensitive = true
  value       = aws_db_instance.introduction_db.password
}

output "db_instance_identifier" {
  value       = aws_db_instance.introduction_db.identifier
}