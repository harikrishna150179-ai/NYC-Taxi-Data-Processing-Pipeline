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
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://ete-project-nyc-taxi-bucket/data/bronze/nyc_taxi/")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.defaultCatalog", "glue_catalog")

# ------------------------------------------------------------------------------
# 4. Define catalog table names and ingestion metadata
# ------------------------------------------------------------------------------
bronze_table_name = "glue_catalog.bronze.nyc_taxi"      # Bronze layer (raw incoming Parquet files)
silver_table_name = "glue_catalog.silver.nyc_taxi"      # Silver layer (transformed dataset)
taxi_zone_table_name = "glue_catalog.reference.taxi_zone" # Reference data for taxi zones
# Capture ingestion timestamp (UTC, timezone-aware)
ingestion_date = datetime.now(tzutc())

# ------------------------------------------------------------------------------
# 5. Read Iceberg tables via Glue Catalog
# ------------------------------------------------------------------------------
source = spark.read.format("iceberg").table(bronze_table_name)
target = spark.read.format("iceberg").table(silver_table_name)
taxi_zone = spark.read.format("iceberg").table(taxi_zone_table_name)

# ------------------------------------------------------------------------------
# 6. Determine latest processed ingestion_date for incremental load
# ------------------------------------------------------------------------------
max_ingestion_date = target.select(max(col('ingestion_date'))).collect()[0][0]
if max_ingestion_date and max_ingestion_date.tzinfo is None:
    # Make sure it's timezone-aware (UTC)
    max_ingestion_date = max_ingestion_date.replace(tzinfo=tzutc())

# ------------------------------------------------------------------------------
# 7. Filter source to only include new records (>= last ingestion)
# ------------------------------------------------------------------------------
if max_ingestion_date:
    source = source.filter(col('ingestion_date') >= lit(max_ingestion_date))

# ------------------------------------------------------------------------------
# 8. Prepare taxi zone lookup tables for joins
# ------------------------------------------------------------------------------
pu_tz = taxi_zone.alias("pu_taxi_zone")
do_tz = taxi_zone.alias("do_taxi_zone")

# ------------------------------------------------------------------------------
# 9. Join source with pickup and dropoff zone reference data
# ------------------------------------------------------------------------------
source = source.join(
    pu_tz,
    on=(col("pulocationid") == col("pu_taxi_zone.locationid")),
    how='inner'
).withColumnRenamed('zone', 'pu_zone')

source = source.join(
    do_tz,
    on=(col("dolocationid") == col("do_taxi_zone.locationid")),
    how='inner'
).withColumnRenamed('zone', 'do_zone')

# ------------------------------------------------------------------------------
# 10. Select relevant columns and derive additional fields
# ------------------------------------------------------------------------------
source = source.select(
    'vendorid',
    'tpep_pickup_datetime',
    'tpep_dropoff_datetime',
    'passenger_count',
    'trip_distance',
    'pu_zone',
    'do_zone',
    'fare_amount',
    'total_amount'
)
source = source.withColumn('zone_od', concat(col('pu_zone'), lit('-'), col('do_zone')))

# ------------------------------------------------------------------------------
# 11. Enrich with ingestion date components for partitioning
# ------------------------------------------------------------------------------
source = source.withColumn("ingestion_date", lit(ingestion_date)) \
    .withColumn("ingestion_year", year(lit(ingestion_date))) \
    .withColumn("ingestion_month", month(lit(ingestion_date))) \
    .withColumn("ingestion_day", dayofmonth(lit(ingestion_date)))

# ------------------------------------------------------------------------------
# 12. Append newly processed data to the silver (Iceberg) table
# ------------------------------------------------------------------------------
# if max_ingestion_date:
source.write.format("iceberg").mode("append").save(silver_table_name)

# ------------------------------------------------------------------------------
# 13. Commit the Glue job to finalize status/logging
# ------------------------------------------------------------------------------
job.commit()
