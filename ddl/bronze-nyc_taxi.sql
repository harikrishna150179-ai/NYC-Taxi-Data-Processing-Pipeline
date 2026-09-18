CREATE TABLE bronze.nyc_taxi (
  vendorid int,
  tpep_pickup_datetime timestamp,
  tpep_dropoff_datetime timestamp,
  passenger_count bigint,
  trip_distance double,
  ratecodeid bigint,
  store_and_fwd_flag string,
  pulocationid int,
  dolocationid int,
  payment_type bigint,
  fare_amount double,
  extra double,
  mta_tax double,
  tip_amount double,
  tolls_amount double,
  improvement_surcharge double,
  total_amount double,
  congestion_surcharge double,
  airport_fee double,
  source_name string,
  ingestion_date timestamp,
  ingestion_day int,
  ingestion_month int,
  ingestion_year int)
PARTITIONED BY (`ingestion_day`, `ingestion_month`, `ingestion_year`)
LOCATION 's3://ete-project-nyc-taxi-bucket/data/bronze/nyc_taxi'
TBLPROPERTIES (
  'table_type'='iceberg',
  'write_compression'='zstd'
);