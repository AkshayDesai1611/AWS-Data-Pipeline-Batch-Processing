# AWS-Data-Pipeline-Batch-Processing: 
This is an E2E pipeline that extracts consumer finance data from API, apply relevant transformations and dump the data into Dynamo DB

# Architecture: 
Pipeline constitutes:

1. AWS Lambda function: Following are activities are done by lambda script

a. Extract the data from source url
b. Fetch data with specified dates
c. Save data into Mongo DB
d. Lambda handler to filter data based on dates and store it in S3 bucket
