CREATE TABLE gold.zone_stats (
  pu_zone string,
  do_zone string,
  avg_journey_time double,
  min_journey_time double,
  max_journey_time double,
  avg_passenger_count bigint,
  min_passenger_count bigint,
  max_passenger_count bigint,
  trip_distance double,
  avg_total_amount double,
  min_total_amount double,
  max_total_amount double,
  ingestion_date timestamp,
  ingestion_day int,
  ingestion_month int,
  ingestion_year int)
PARTITIONED BY (`ingestion_day`, `ingestion_month`, `ingestion_year`)
LOCATION 's3://ete-project-nyc-taxi-bucket/data/gold/zone_stats'
TBLPROPERTIES (
  'table_type'='iceberg',
  'write_compression'='zstd'
);