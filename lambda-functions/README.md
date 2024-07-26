# README #

This README would normally document whatever steps are necessary to get your application up and running.

### What is this repository for? ###

* Quick summary
* Version
* [Learn Markdown](https://bitbucket.org/tutorials/markdowndemo)

### How do I get set up? ###

* Summary of set up
* Configuration
* Dependencies
* Database configuration
* How to run tests
* Deployment instructions

### Contribution guidelines ###

* Writing tests
* Code review
* Other guidelines

### Who do I talk to? ###

* Repo owner or admin
* Other community or team contact




# NOTE: đảm bảo trước khi merge lên branch develop, branch merge vào develop phải được update từ nhánh develop về (tránh mất code của người khác)
* Step 1: sửa file serverless.yml: Ex - ${file(./src/{username}/sub_serverless.yml)}
* Step 2: tạo sub folder {username} bên trên folder src 
* Step 3: viết code của mình trong folder {username}. 2 file bắt buộc phải có: sub_serverless.yml (có ref đến file python code) + python code

