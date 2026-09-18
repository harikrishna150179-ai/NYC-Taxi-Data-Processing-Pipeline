CREATE EXTERNAL TABLE `nyc_taxi`(
  `vendorid` int, 
  `tpep_pickup_datetime` timestamp, 
  `tpep_dropoff_datetime` timestamp, 
  `passenger_count` bigint, 
  `trip_distance` double, 
  `ratecodeid` bigint, 
  `store_and_fwd_flag` string, 
  `pulocationid` int, 
  `dolocationid` int, 
  `payment_type` bigint, 
  `fare_amount` double, 
  `extra` double, 
  `mta_tax` double, 
  `tip_amount` double, 
  `tolls_amount` double, 
  `improvement_surcharge` double, 
  `total_amount` double, 
  `congestion_surcharge` double, 
  `airport_fee` double)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://ete-project-nyc-taxi-bucket/data/staging/nyc_taxi/'
TBLPROPERTIES (
  'CRAWL_RUN_ID'='fed36140-89ba-4759-8c96-b61484810ee3', 
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='ete-project-nyc-taxi-staging-crawler', 
  'averageRecordSize'='26', 
  'classification'='parquet', 
  'compressionType'='none', 
  'objectCount'='1', 
  'recordCount'='2964624', 
  'sizeKey'='49961641', 
  'typeOfData'='file')