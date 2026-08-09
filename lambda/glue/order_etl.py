import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME',
        'bucket_name',
        'object_key',
        'output_bucket'
    ]
)

bucket_name = args["bucket_name"]
object_key = args["object_key"]
output_bucket = args["output_bucket"]

print(f"Bucket: {bucket_name}")
print(f"Object: {object_key}")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

input_path = f"s3://{bucket_name}/{object_key}"
output_path = f"s3://{output_bucket}/orders/output/"

from pyspark.sql.functions import (
    current_timestamp,
    col,
    when,
    to_timestamp,
    year,
    month
)
df = spark.read.json(input_path)

# Convert event_time from string to timestamp
df = df.withColumn(
    "event_time",
    to_timestamp(
        col("event_time"),
        "yyyy-MM-dd HH:mm:ss 'UTC'"
    )
)

# Replace null brands
df = df.withColumn(
    "brand",
    when(col("brand").isNull(), "Unknown")
    .otherwise(col("brand"))
)

# Remove duplicate rows
df = df.dropDuplicates([
    "event_time",
    "event_type",
    "product_id",
    "user_id",
    "user_session"
])

# Add processing timestamp
df = df.withColumn(
    "processed_at",
    current_timestamp()
)

# Partitioning
df = df.withColumn(
    "year",
    year(col("event_time"))
)

df = df.withColumn(
    "month",
    month(col("event_time"))
)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(output_path)

print("PARQUET WRITTEN SUCCESSFULLY")

job.commit()
