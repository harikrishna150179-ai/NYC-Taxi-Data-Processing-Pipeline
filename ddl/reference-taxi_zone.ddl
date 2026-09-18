CREATE TABLE reference.taxi_zone (
  locationid string,
  borough string,
  zone string,
  service_zone string,
  source_name string,
  ingestion_date timestamp,
  ingestion_day int,
  ingestion_month int,
  ingestion_year int)
PARTITIONED BY (`ingestion_day`, `ingestion_month`, `ingestion_year`)
LOCATION 's3://ete-project-nyc-taxi-bucket/data/reference/taxi_zone'
TBLPROPERTIES (
  'table_type'='iceberg',
  'write_compression'='zstd'
);