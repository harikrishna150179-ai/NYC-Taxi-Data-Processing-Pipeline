import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import *
import boto3
from datetime import datetime
from dateutil.tz import tzutc

# ------------------------------------------------------------------------------
# Parse Glue job arguments (JOB_NAME is required for job tracking/logging)
# ------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# ------------------------------------------------------------------------------
# Initialize Spark, GlueContext, and Glue Job objects for ETL
# ------------------------------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ------------------------------------------------------------------------------
# Set Spark & Iceberg configurations to use AWS Glue Catalog as Iceberg catalog
# ------------------------------------------------------------------------------
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://ete-project-nyc-taxi-bucket/data/bronze/nyc_taxi/")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.defaultCatalog", "glue_catalog")

# ------------------------------------------------------------------------------
# Declare S3 paths and schema for use throughout ETL
# ------------------------------------------------------------------------------
staging = 's3://ete-project-nyc-taxi-bucket/data/staging/nyc_taxi/'         # S3 location for incoming Parquet files
staging_prefix = 'data/staging/nyc_taxi/'                                   # Used to list S3 objects for ETL
bucket = 'ete-project-nyc-taxi-bucket'                                      # Name of S3 bucket for all data
ingestion_date = datetime.now(tzutc())                                      # Ingestion timestamp in UTC (timezone aware)

# Target schema dict as per Iceberg DDL (column name → data type)
target_schema = {
    "vendorid": "int",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "bigint",
    "trip_distance": "double",
    "ratecodeid": "bigint",
    "store_and_fwd_flag": "string",
    "pulocationid": "int",
    "dolocationid": "int",
    "payment_type": "bigint",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double",
    "source_name": "string",
    "ingestion_date": "timestamp",
    "ingestion_day": "int",
    "ingestion_month": "int",
    "ingestion_year": "int"
}

# ------------------------------------------------------------------------------
# Function to map and cast columns of DataFrame to match the Iceberg DDL exactly
# ------------------------------------------------------------------------------
def map_and_cast_to_schema(df):
    """
    Casts DataFrame columns to types as per target_schema dict,
    and reorders columns to match the Iceberg DDL.
    Any missing column is filled with null of the right type.
    """
    cols = []
    for col_name, dtype in target_schema.items():
        if col_name in df.columns:
            cols.append(col(col_name).cast(dtype).alias(col_name))
        else:
            cols.append(lit(None).cast(dtype).alias(col_name))
    return df.select(*cols)

# ------------------------------------------------------------------------------
# List all objects in the S3 staging location for incremental ETL
# ------------------------------------------------------------------------------
s3 = boto3.client("s3")
staging_objects = s3.list_objects_v2(Bucket=bucket, Prefix=staging_prefix)

# ------------------------------------------------------------------------------
# Read the Iceberg bronze table to get ingestion state (for deduplication/incremental logic)
# ------------------------------------------------------------------------------
target = spark.read.format("iceberg").table("glue_catalog.bronze.nyc_taxi")

# Build set of all previously ingested file paths (source_name) to prevent duplicates
existing_sources = target.select("source_name").distinct().rdd.flatMap(lambda x: x).collect()
existing_sources = set(existing_sources)

# Get the latest ingestion_date for incremental data loading
max_ingestion_date = target.select(max(col('ingestion_date'))).collect()[0][0]
if max_ingestion_date and max_ingestion_date.tzinfo is None:
    # Ensure max_ingestion_date is timezone-aware (UTC) for correct datetime comparison
    max_ingestion_date = max_ingestion_date.replace(tzinfo=tzutc())

source = None  # Will hold the new data to be ingested

# ------------------------------------------------------------------------------
# Main transformation logic — determine which new Parquet files to load
# ------------------------------------------------------------------------------
if not max_ingestion_date:
    # First-time load: ingest all files from staging area
    source = spark.read.format("parquet").load(staging)
else:
    # Incremental load: only ingest Parquet files not already ingested and newer than checkpoint
    new_files = []
    for obj in staging_objects.get('Contents', []):
        obj_path = f"s3://{bucket}/{obj['Key']}"
        # Only load files if:
        # - They are .parquet files
        # - Not previously ingested (by source_name)
        # - LastModified (S3 file creation time) > last successful ingestion checkpoint
        if obj['Key'].endswith(".parquet") and obj['LastModified'] > max_ingestion_date and obj_path not in existing_sources:
            new_files.append(obj_path)
    if new_files:
        # Load only the new files into Spark DataFrame
        source = spark.read.parquet(*new_files)

# ------------------------------------------------------------------------------
# Add ingestion metadata columns and source_name (S3 file path) to DataFrame
# ------------------------------------------------------------------------------
if source:
    source = source.withColumn("ingestion_date", lit(ingestion_date)) \
        .withColumn("ingestion_year", year(lit(ingestion_date))) \
        .withColumn("ingestion_month", month(lit(ingestion_date))) \
        .withColumn("ingestion_day", dayofmonth(lit(ingestion_date))) \
        .withColumn("source_name", input_file_name())

    # Remap/cast all columns as per DDL
    source = map_and_cast_to_schema(source)

# ------------------------------------------------------------------------------
# Append new data to the Iceberg bronze table (if any new data was found)
# ------------------------------------------------------------------------------
if source:
    source.write.format("iceberg").mode("append").save("glue_catalog.bronze.nyc_taxi")

# ------------------------------------------------------------------------------
# Commit the Glue job (required for correct Glue job status/logging)
# ------------------------------------------------------------------------------
job.commit()
