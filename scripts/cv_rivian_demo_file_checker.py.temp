import sys
import boto3
from awsglue.utils import getResolvedOptions

if ('--{}'.format('data_date') in sys.argv):
    args = getResolvedOptions(sys.argv, ['data_date'])
    data_date = args.get("data_date")
    print("data_date is --------------------------",data_date)
    
    bucket='cv-rivian-demo'
    file_key = f'/touch_files/{data_date}/customer.txt'
    s3 = boto3.client('s3')
    try:
        s3.Object(bucket, file_key).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("Object Doesn't exists------------------",file_key)
        else:
            print("Error occurred while fetching a file from S3. Try Again.")
    else:
        print("Object Exists---------------",file_key)