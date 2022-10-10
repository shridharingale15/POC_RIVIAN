import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame
import boto3

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon Redshift
if ('--{}'.format('data_date') in sys.argv):
    args = getResolvedOptions(sys.argv, ['data_date'])
    data_date = args.get("data_date")
    transform_data_date=data_date[4:9]+"-"+data_date[:2]+"-"+data_date[2:4]
    
    transaction_dynamicframe = glueContext.create_dynamic_frame.from_catalog(
        database="rivian_db",
        additional_options={
            "aws_iam_role": "arn:aws:iam::622527851988:role/RedshiftServiceAuth_RS"
        },
        redshift_tmp_dir="s3://cv-rivian-demo/temp/",
        table_name="riviandb_public_transaction",
        transformation_ctx="AmazonRedshift_node1664171006592",)
    transaction_df = transaction_dynamicframe.toDF().where(col("transaction_insert_date")==transform_data_date)
    transaction_df.show()
    transaction_filtered_dyf=DynamicFrame.fromDF(transaction_df, glueContext, "transaction_df")
    
    # Script generated for node S3 bucket
    S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
        database="rivian_db",
        additional_options={
            "aws_iam_role": "arn:aws:iam::622527851988:role/RedshiftServiceAuth_RS"
        },
        redshift_tmp_dir="s3://cv-rivian-demo/temp/",
        table_name="riviandb_public_customer_snapshot",
        transformation_ctx="S3bucket_node1",
    )
    customer_snapshot_df = S3bucket_node1.toDF().where(col("insert_date")==transform_data_date)
    customer_snapshot_df.show()
    customer_snapshot_filtered_dyf=DynamicFrame.fromDF(customer_snapshot_df, glueContext, "customer_snapshot_df")

    
    # Script generated for node ApplyMapping
    ApplyMapping_node2 = Join.apply(
        frame1=customer_snapshot_filtered_dyf,
        frame2=transaction_filtered_dyf,
        keys1=["account_no"],
        keys2=["account_no"],
        transformation_ctx="ApplyMapping_node2",
    )
    
    datasource1_no_nulls = DropNullFields.apply(ApplyMapping_node2)
    
    # Script generated for node S3 bucket
    S3bucket_node3 = glueContext.write_dynamic_frame.from_catalog(
        frame=datasource1_no_nulls,
        database="rivian_db",
        table_name="riviandb_public_customer_transaction",
        redshift_tmp_dir="s3://cv-rivian-demo/temp/",
        additional_options={
            "aws_iam_role": "arn:aws:iam::622527851988:role/RedshiftServiceAuth_RS"
        },
        transformation_ctx="S3bucket_node3",
    )

job.commit()
