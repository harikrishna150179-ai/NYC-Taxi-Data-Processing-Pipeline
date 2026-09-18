CREATE TABLE silver.nyc_taxi (
  vendorid int,
  tpep_pickup_datetime timestamp,
  tpep_dropoff_datetime timestamp,
  passenger_count bigint,
  trip_distance double,
  pu_zone string,
  do_zone string,
  zone_od string,
  fare_amount double,
  total_amount double,
  ingestion_date timestamp,
  ingestion_day int,
  ingestion_month int,
  ingestion_year int)
PARTITIONED BY (`ingestion_day`, `ingestion_month`, `ingestion_year`)
LOCATION 's3://ete-project-nyc-taxi-bucket/data/silver/nyc_taxi'
TBLPROPERTIES (
  'table_type'='iceberg',
  'write_compression'='zstd'
);