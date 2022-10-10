import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import lit,col
import boto3

touchfile_flag='False'

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
#a ='1'
#if (a=='1'):
if ('--{}'.format('data_date') in sys.argv):
    #data_date='2022-09-20'
    args = getResolvedOptions(sys.argv, ['data_date'])
    data_date = args.get("data_date")
    
    
    c_df = glueContext.create_dynamic_frame.from_catalog(database = "rivian_db", table_name = "riviandb_public_customer", redshift_tmp_dir = args["TempDir"], transformation_ctx = "c_df")
    
    snapshot_df = glueContext.create_dynamic_frame.from_catalog(database = "rivian_db", table_name = "riviandb_public_customer_snapshot", redshift_tmp_dir = args["TempDir"], transformation_ctx = "c_df")
    
    if (snapshot_df.toDF().count()) == 0:
        customer_df=c_df.toDF().where(col("insert_date")==data_date).withColumn("status",lit('inserted'))
        print("inside if--------------------------------- creating staging customer_df")
        customer_df.show()
    else:
        print("data is available")
        customer_df=c_df.toDF().where(col("insert_date")==data_date).withColumn("status",lit('updated'))
        customer_df.show()
        
    #customer_df=c_df.toDF().where(col("insert_date")==data_date).withColumn("status",lit('updated'))
    print("out side of if ,customer_df data for date ...............................................................",data_date)
    #customer_df.show()
    print("outside of if ,schema of customer_df...........................................................................................")
    customer_df.printSchema()
    
    dynamic_transform_df=DynamicFrame.fromDF(customer_df, glueContext, "dynamic_transformdf")
    print("tranformed df to dynamic transformed df")
    
    post_query="begin;delete from customer_snapshot using customer_staging where customer_staging.account_no = customer_snapshot.account_no ; insert into customer_snapshot select * from customer_staging; drop table customer_staging; end;"
    
    
    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dynamic_transform_df, catalog_connection = "Redshift", connection_options = {"dbtable":  "customer_staging", "database": "riviandb","postactions": post_query},redshift_tmp_dir = 's3://cv-rivian-demo/temp/', transformation_ctx = "datasink4")
    touchfile_flag='True'
    print('touchfile_flag------------------------------',touchfile_flag)
    
    if (touchfile_flag=='True'):
        t_data_date=data_date[5:7]+data_date[-2:]+data_date[:4]
        print('touchfile_flag in if condition------------------------------',touchfile_flag)
        touch_file_path=f'touch_files/{t_data_date}/customer_snapshot.txt'
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket='cv-rivian-demo',
            Key=touch_file_path)
    else:
        print("Touch file flag is false")
    
    
job.commit()