# NYC-Taxi-Data-Processing-Pipeline
This project implements an end-to-end data processing pipeline for NYC taxi data using AWS services. The pipeline processes taxi trip data through multiple layers (staging, bronze, silver, gold) using Apache Iceberg tables for data storage and AWS Glue for ETL processing.
## Architecture Overview
<img width="5764" height="3328" alt="architecture" src="https://github.com/user-attachments/assets/6b0b8c61-be81-4e32-835d-ff458900844e" />

The pipeline follows a modern data lakehouse architecture with the following components:


1. **Data Ingestion**: Raw data lands in S3 staging area
2. **Event-Driven Processing**: EventBridge detects new files and triggers Step Functions workflow
3. **Orchestration**: Step Functions coordinates the ETL jobs in sequence
4. **Data Processing**: AWS Glue jobs transform data through bronze, silver, and gold layers
5. **Storage**: Apache Iceberg tables provide ACID transactions and time travel capabilities

## Data Flow
S3 (Raw Data) → Bronze (Raw + Metadata) → Silver (Transformed) → Gold (Aggregated)

## Project Structure
```text
aws-ete-de-project/
├── crawlers/                  # AWS Glue Crawler configurations
├── data/                      # Sample data files
│   ├── nyc_taxi/              # NYC taxi trip data
│   └── reference/             # Reference data, such as taxi zones
├── ddl/                       # Table definitions
├── etl-scripts/               # AWS Glue ETL scripts
├── eventbridge-rule/          # EventBridge rule definitions
└── step-function/             # Step Functions workflow definition
```
## Components

### Data Storage Layers

1. **Staging Layer**: Raw data landing zone (Parquet files)
2. **Bronze Layer**: Raw data with metadata (Iceberg tables)
3. **Silver Layer**: Cleansed and transformed data (Iceberg tables)
4. **Gold Layer**: Aggregated analytics data (Iceberg tables)

### Table Definitions

- **staging-nyc_taxi.sql**: External table pointing to raw Parquet files
- **bronze-nyc_taxi.sql**: Raw data with added metadata
- **reference-taxi_zone.ddl**: Taxi zone lookup reference data
- **silver-nyc_taxi.sql**: Transformed data with zone information
- **gold-zone-stats.sql**: Aggregated statistics by pickup/dropoff zones

### ETL Scripts
**1**. **staging_to_bronze-nyc_taxi-etl.py**

   This script processes raw Parquet files from the staging area into the bronze Iceberg table:

- Reads new Parquet files from S3 staging area
- Adds metadata columns (ingestion date, source file)
- Handles schema mapping and type casting
- Implements incremental loading (only processes new files)
- Writes data to the bronze Iceberg table

**2**. **staging_to_reference-taxi_zone_lookup-etl.py**
  
Loads the taxi zone lookup reference data:

- Processes CSV files containing taxi zone information
- Adds metadata columns
- Implements merge logic for updates to reference data
- Writes to the reference Iceberg table
  
**3**. **bronze_to_silver-nyc_taxi-etl.py**
   
Transforms bronze data into the silver layer:

- Reads from bronze Iceberg table
- Joins with taxi zone reference data to add zone names
- Creates derived fields (zone_od - origin-destination pair)
- Selects relevant columns for analytics
- Writes to silver Iceberg table
  
**4**. **silver_to_gold-zone_stats-etl.py**
  
Aggregates silver data into analytics-ready gold tables:

- Calculates journey time metrics
- Computes statistics by pickup/dropoff zone pairs
- Aggregates passenger counts, trip distances, and fare amounts
- Writes aggregated data to gold Iceberg table
### Orchestration  

**EventBridge Rule (ete-project-nyc-taxi-s3-event.json)**

Configures an event rule that:

- Monitors the S3 bucket for new files in the staging area
- Triggers the Step Functions workflow when new files are detected
**Step Functions Workflow (ete-project-nyc-taxi-pipeline.json)**
  
Defines a workflow that:

- Executes ETL jobs in sequence (staging→bronze→silver→gold)
- Implements error handling with SNS notifications
- Sends success notifications upon completion

## Data Model

### Bronze Layer

Raw data with metadata columns:

- Trip details (pickup/dropoff times, locations, etc.)
- Payment information
- Added metadata (ingestion date, source file)
  
### Silver Layer

Transformed data with business context:

- Enriched with zone names (instead of location IDs)
- Derived fields for analysis
- Subset of relevant columns

### Gold Layer

Aggregated statistics by zone pairs:

- Journey time metrics (avg, min, max)
- Passenger count metrics
- Trip distance metrics
- Fare amount metrics

## Deployment and Execution

The pipeline runs automatically when new data arrives in the S3 staging area:

1. Data is uploaded to s3://ete-project-nyc-taxi-bucket/data/staging/nyc_taxi/
2. EventBridge detects the new file and triggers the Step Functions workflow
3. Step Functions executes the ETL jobs in sequence
4. Data flows through bronze, silver, and gold layers
5. Success/failure notifications are sent via SNS

## Technologies Used

- AWS S3: Data storage
- AWS Glue: ETL processing
- AWS Step Functions: Workflow orchestration
- AWS EventBridge: Event-driven triggers
- AWS SNS: Notifications
- Apache Iceberg: Table format with ACID transactions
- Apache Spark: Data processing engine
