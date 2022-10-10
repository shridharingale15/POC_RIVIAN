import sys
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import sys
import boto3
import botocore
from awsglue.utils import getResolvedOptions
from botocore.exceptions import *

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
a='1'
file_key = "touch_files/" + data_date + "/customer.txt"
#if ('--{}'.format('data_date') in sys.argv):
if(a==1):
    c_df = glueContext.create_dynamic_frame.from_catalog(database = "rivian_db", table_name = "riviandb_public_customer", redshift_tmp_dir = args["TempDir"], transformation_ctx = "c_df")
    c_df.toDF().printSchema()
    c_df.toDF().printSchema().show(
    #args = getResolvedOptions(sys.argv, ['data_date'])
    #data_date = args.get("data_date")
    data_date='09202022'
    #print(data_date)
    
    #bucket_name = "cv-rivian-demo"   
    #file_key = f'touch_files/{data_date}/customer.txt'
    
    s3 = boto3.client('s3')
    try:
        s3.Object('cv-rivian-demo', file_key).load()
        print("checking data  --------------------------")
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("Object Doesn't exists------------------",file_key)
        else:
            print("Error occurred while fetching a file from S3. Try Again.")
    else:
        print("Object Exists---------------",file_key)
        
job.commit()