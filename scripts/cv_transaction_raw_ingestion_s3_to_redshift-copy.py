import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame
import boto3

touchfile_flag='False'
 

## @params: [JOB_NAME]
#args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

if ('--{}'.format('data_date') in sys.argv):
    args = getResolvedOptions(sys.argv, ['data_date'])
    data_date = args.get("data_date")

    #logger.info(f"data_date is :  {data_date}")
    path_to_read = "s3://cv-rivian-demo/input/transaction/" + data_date + "/*"

    df = spark.read.csv(path_to_read,header=True)
    
    transform_df = df.withColumn("transaction_insert_date",F.to_date(F.lit(data_date),'MMddyyyy'))
    transform_df.show()
    transform_df.printSchema()
    
    dynamic_transform_df=DynamicFrame.fromDF(transform_df, glueContext, "dynamic_transformdf")

    
    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dynamic_transform_df, catalog_connection = "Redshift", connection_options = {"dbtable": "transaction", "database": "riviandb"}, redshift_tmp_dir ="s3://cv-rivian-demo/temp/" , transformation_ctx = "datasink4")
    touchfile_flag='True'
    print('touchfile_flag------------------------------',touchfile_flag)
    
    if (touchfile_flag=='True'):
        print('touchfile_flag in if condition------------------------------',touchfile_flag)
        touch_file_path=f'touch_files/{data_date}/transaction.txt'
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket='cv-rivian-demo',
            Key=touch_file_path)
    else:
        print("Touch file flag is false")
job.commit()