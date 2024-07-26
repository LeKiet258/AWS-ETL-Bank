# step for config rds (with security access or public access)
Step 1: Create a VPC
Step 2: Create an Internet Gateway and attach it to the VPC
Step 3: Create 3 subnets: 1 public for EC2 and 2 private for RDS
Step 4: Create 2 route tables: 1 public and 1 private
Step 5: Create 2 security groups: 1 for EC2 and 1 for RDS
Step 6: Create the DB subnet group
Step 7: Create the RDS database
Step 8: Create the EC2 instance
Step 9: Verify

# how to access rds from ec2
ssh -i ynn_key.pem ec2-user@ec2-18-143-41-250.ap-southeast-1.compute.amazonaws.com
psql  --host=introduction-01-intro-ap-southeast-1-dev-introduction-db.cpfm8ml2cxp2.ap-southeast-1.rds.amazonaws.com  --port=5432  --username=postgres  --password  --dbname=postgres




# with private, can use ssm to forward port ec2 to local machine: 
aws ssm start-session --target "i-03eb88c3f81b03c56" --document-name AWS-StartPortForwardingSessionToRemoteHost --parameters "{\"host\":[\"introduction-01-intro-ap-southeast-1-dev-introduction-db.cpfm8ml2cxp2.ap-southeast-1.rds.amazonaws.com\"], \"portNumber\":[\"5432\"],\"localPortNumber\":[\"5432\"]}"

# how to apply
terraform apply 
- vpc -> ec2 -> rds (with security access)
- vpc -> rds (with public access)