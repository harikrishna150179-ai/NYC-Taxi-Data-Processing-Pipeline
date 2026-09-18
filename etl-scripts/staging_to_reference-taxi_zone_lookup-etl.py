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
staging = 's3://ete-project-nyc-taxi-bucket/data/staging/taxi_zone_lookup/'         # S3 location for incoming Parquet files
staging_prefix = 'data/staging/taxi_zone_lookup/'                                   # Used to list S3 objects for ETL
target_table = 'glue_catalog.reference.taxi_zone'
bucket = 'ete-project-nyc-taxi-bucket'                             # Name of S3 bucket for all data
ingestion_date = datetime.now(tzutc())                             # Ingestion timestamp in UTC (timezone aware)

# ------------------------------------------------------------------------------
# List all objects in the S3 staging location for incremental ETL
# ------------------------------------------------------------------------------
s3 = boto3.client("s3")
staging_objects = s3.list_objects_v2(Bucket=bucket, Prefix=staging_prefix)

# ------------------------------------------------------------------------------
# Read the Iceberg bronze table to get ingestion state (for deduplication/incremental logic)
# ------------------------------------------------------------------------------
target = spark.read.format("iceberg").table(target_table)

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
    source = spark.read.format("csv").option("header", "true").load(staging)
else:
    # Incremental load: only ingest Parquet files not already ingested and newer than checkpoint
    new_files = []
    for obj in staging_objects.get('Contents', []):
        obj_path = f"s3://{bucket}/{obj['Key']}"
        # Only load files if:
        # - They are .parquet files
        # - Not previously ingested (by source_name)
        # - LastModified (S3 file creation time) > last successful ingestion checkpoint
        if obj['Key'].endswith(".csv") and obj['LastModified'] > max_ingestion_date and obj_path not in existing_sources:
            new_files.append(obj_path)
    if new_files:
        # Load only the new files into Spark DataFrame
        source = spark.read.format("csv").option("header", "true").load(*new_files)

# ------------------------------------------------------------------------------
# Add ingestion metadata columns and source_name (S3 file path) to DataFrame
# ------------------------------------------------------------------------------
if source:
    source = source.withColumn("ingestion_date", lit(ingestion_date)) \
        .withColumn("ingestion_year", year(lit(ingestion_date))) \
        .withColumn("ingestion_month", month(lit(ingestion_date))) \
        .withColumn("ingestion_day", dayofmonth(lit(ingestion_date))) \
        .withColumn("source_name", input_file_name())


# ------------------------------------------------------------------------------
# Append new data to the Iceberg bronze table (if any new data was found)
# ------------------------------------------------------------------------------
if source:
    if not max_ingestion_date:
        source.write.format("iceberg").mode("append").save(target_table)
    else:
        source.createOrReplaceTempView("source")
        spark.sql(f"""
            MERGE INTO {target_table} AS target
            USING source AS source
            ON target.locationid = source.locationid
            WHEN MATCHED THEN
              UPDATE SET *
            WHEN NOT MATCHED THEN
              INSERT *
        """)

# ------------------------------------------------------------------------------
# Commit the Glue job (required for correct Glue job status/logging)
# ------------------------------------------------------------------------------
job.commit()
