import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read CSV from S3 Raw Zone
df = spark.read.option("header", True).csv("s3://airlines-project2/raw/airlines/")

# Add metadata
df = df.withColumn("load_timestamp", F.current_timestamp())

# Write parquet to S3 Parquet Zone
df.write.mode("overwrite").parquet("s3://airlines-project2/parquet/airlines/")