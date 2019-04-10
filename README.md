# aws-lambda-pgsqltosftp
A Java AWS Lambda function to export a PostgreSQL schema as csv and upload it though SFTP

## How to use
In Eclipse select Run As -> Maven build -> Goals : "package shade:shade"  
Then upload it to AWS using the AWS console

### Environment variables
You will have to set the environment variables to define your PG database settings and SFTP settings  

DB_HOST : your RDS endpoint  
DB_PORT : your RDS port (usually 5432)  
DB_NAME : your database name 
DB_SCHEMA : your database schema  
DB_USER : your database username  
DB_PASSWD : your database password  
SFTP_HOST : your SFTP endpoint  
SFTP_USER : your SFTP username  
SFTP_PASSWD : your SFTP password  
SFTP_PORT : your SFTP port (should be 22)  
TABLE_TYPES : the table types you want to export, separated by a comma (i.e. "TABLE, VIEW"), this is optional  
QUERY_LIMIT : the LIMIT of rows your want to export (optional)  

### Lambda role
You need to create a IAM role for the Lambda function with VPCFullAccess  
In the network section of the Lambda function, define the VPC which your RDS is in, with associated subnets and also allowed security group  

Enjoy !
