import sys
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import botocore
from awsglue.utils import getResolvedOptions
from botocore.exceptions import *


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
file_dictionary = {'customer': 'False', 'transaction': 'False', 'customer_snapshot': 'False'}
#######
def file_checker(data_date,file_name):
    print('data_date is --------',data_date)
    
    bucket_name = "cv-rivian-demo"   
    file_key = f'touch_files/{data_date}/{file_name}.txt'
    print('file_key is --------',file_key)
    session = boto3.session.Session()
    s3 = session.resource('s3')
    try:
        s3.Object(bucket_name, file_key).load()
        print("checking data  --------------------------")
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("Object Doesn't exists------------------",file_key)
        else:
            print("Error occurred while fetching a file from S3. Try Again.")
    else:
        print("Object Exists---------------",file_key)
        # Dictionary with three items
        file_d = {file_name: 'True'}
        file_dictionary.update(file_d)
        print('file_dictionary----------------------------------',file_dictionary)
        print('file_d----------------------------------',file_d)

##########################

if ('--{}'.format('data_date') in sys.argv):
    args = getResolvedOptions(sys.argv, ['data_date'])
    data_date = args.get("data_date")
    
    file_checker(data_date,'customer')
    file_checker(data_date,'transaction')
    file_checker(data_date,'customer_snapshot')
    
    if (file_dictionary['customer'] =='True') and (file_dictionary['transaction'] =='True') and (file_dictionary['customer_snapshot']=='True'):
        print(f"All files are present at location --- for date {data_date}")
        client = boto3.client('glue')
        response = client.start_job_run(
            JobName = 'cv_customer_transaction_join-copy',Arguments = {'--data_date':data_date})
        
    else:
        print("file is missing")
    
    

    #print('data_date is --------',data_date)
    
    #bucket_name = "cv-rivian-demo"   
    #file_key = f'touch_files/{data_date}/customer.txt'
    #session = boto3.session.Session()
    #s3 = session.resource('s3')
    #try:
    #    s3.Object(bucket_name, file_key).load()
    #    print("checking data  --------------------------")
    #except botocore.exceptions.ClientError as e:
    #    if e.response['Error']['Code'] == "404":
    #        print("Object Doesn't exists------------------",file_key)
    #    else:
    #        print("Error occurred while fetching a file from S3. Try Again.")
    #else:
    #    print("Object Exists---------------",file_key)
    
job.commit()