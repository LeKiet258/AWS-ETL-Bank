docker pull amazonlinux

docker run -it amazonlinux:latest /bin/bash

# inside docker container
npm install serverless -g

mkdir ai4e-repo && cd ai4e-repo

serverless create --template aws-python

serverless deploy --region ap-southeast-1

serverless invoke --function hello --log --region ap-southeast-1


