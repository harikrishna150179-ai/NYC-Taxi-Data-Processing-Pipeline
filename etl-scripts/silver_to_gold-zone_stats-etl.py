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
# 1. Parse Glue job arguments (JOB_NAME used for job tracking/logging)
# ------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# ------------------------------------------------------------------------------
# 2. Initialize SparkContext, GlueContext, Spark session and Glue Job
# ------------------------------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ------------------------------------------------------------------------------
# 3. Configure Spark to use AWS Glue Catalog as an Iceberg catalog
# ------------------------------------------------------------------------------
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://ete-project-nyc-taxi-bucket/data/")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.defaultCatalog", "glue_catalog")

# ------------------------------------------------------------------------------
# 4. Define catalog table names and ingestion metadata
# ------------------------------------------------------------------------------
silver_table = "glue_catalog.silver.nyc_taxi"
gold_table   = "glue_catalog.gold.zone_stats"
# Capture ingestion timestamp (UTC, timezone-aware)
ingestion_date = datetime.now(tzutc())

# ------------------------------------------------------------------------------
# 5. Read the silver Iceberg table
# ------------------------------------------------------------------------------
silver_df = spark.read.format("iceberg").table(silver_table)

# ------------------------------------------------------------------------------
# 6. Compute journey time (seconds) and select relevant columns
# ------------------------------------------------------------------------------
silver_df = silver_df.withColumn(
    "journey_time",
    (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))).cast("double")
).select(
    "pu_zone", "do_zone", "journey_time", "passenger_count", "trip_distance", "total_amount"
)

# ------------------------------------------------------------------------------
# 7. Aggregate statistics by pickup and dropoff zone
# ------------------------------------------------------------------------------
zone_stats = silver_df.groupBy("pu_zone", "do_zone").agg(
    avg("journey_time").alias("avg_journey_time"),
    min("journey_time").alias("min_journey_time"),
    max("journey_time").alias("max_journey_time"),
    avg("passenger_count").cast("long").alias("avg_passenger_count"),
    min("passenger_count").alias("min_passenger_count"),
    max("passenger_count").alias("max_passenger_count"),
    avg("trip_distance").alias("trip_distance"),
    avg("total_amount").alias("avg_total_amount"),
    min("total_amount").alias("min_total_amount"),
    max("total_amount").alias("max_total_amount")
)

# ------------------------------------------------------------------------------
# 8. Enrich with ingestion date components for partitioning
# ------------------------------------------------------------------------------
zone_stats = zone_stats.withColumn("ingestion_date", lit(ingestion_date)) \
    .withColumn("ingestion_year", year(lit(ingestion_date))) \
    .withColumn("ingestion_month", month(lit(ingestion_date))) \
    .withColumn("ingestion_day", dayofmonth(lit(ingestion_date)))

# ------------------------------------------------------------------------------
# 9. Overwrite the gold Iceberg table with the aggregated results
# ------------------------------------------------------------------------------
zone_stats.write.format("iceberg") \
    .mode("overwrite") \
    .save(gold_table)

# ------------------------------------------------------------------------------
# 10. Commit the Glue job
# ------------------------------------------------------------------------------
job.commit()
