resource "aws_db_instance" "introduction_db" {

    # db settings
    identifier = var.aurora_name
    allocated_storage      = var.settings_database.allocated_storage
    engine                 = var.settings_database.engine
    engine_version         = var.settings_database.engine_version
    instance_class         = var.settings_database.instance_class
    db_name                = var.settings_database.db_name
    username               = var.settings_database.username
    password               = var.settings_database.password
    skip_final_snapshot    = var.settings_database.skip_final_snapshot
    multi_az               = false
    publicly_accessible    = true # use for public access

    # network db settings
    db_subnet_group_name   = aws_db_subnet_group.introduction_db_subnet_group.id
    vpc_security_group_ids = [aws_security_group.introduction_db_sg.id]

    tags = merge({"Name" = "introduction_database"}, var.tags)
}

resource "aws_db_subnet_group" "introduction_db_subnet_group" {
    name        = "${var.aurora_name}-subnet-group"
    description = "DB subnet group for introduction_db"

    # subnet_ids  = [for subnet in var.private_subnets : subnet] # use for security access (stardard config)
    subnet_ids  = [for subnet in var.public_subnets : subnet] # use for public access
}

// Create a security group for the RDS instances called "introduction_db_sg"
resource "aws_security_group" "introduction_db_sg" {
    name        = "${var.aurora_name}-security-group"
    description = "Security group for tutorial databases"
    vpc_id      = var.vpc_id

    ingress {
        description     = "Allow POSTGRES traffic from only the ec2 sg"
        from_port       = "5432"
        to_port         = "5432"
        protocol        = "tcp"
        cidr_blocks = ["0.0.0.0/0"] # use for public access

        # security_groups = [var.ec2_sg_id] # use for security access (stardard config)
    }

    tags = merge({"Name" = "db_security_group"}, var.tags)
}