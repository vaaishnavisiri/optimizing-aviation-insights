import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read flights_latest CSV
df = spark.read.option("header", True).csv("s3://airlines-project2/raw/flights/flights_latest.csv")

# Add metadata
df = df.withColumn("load_timestamp", F.current_timestamp())

# Write Parquet
df.write.mode("overwrite").parquet("s3://airlines-project2/parquet/flights_latest/")