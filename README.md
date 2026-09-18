# NYC-Taxi-Data-Processing-Pipeline
This project implements an end-to-end data processing pipeline for NYC taxi data using AWS services. The pipeline processes taxi trip data through multiple layers (staging, bronze, silver, gold) using Apache Iceberg tables for data storage and AWS Glue for ETL processing.
# Architecture Overview
<img width="5764" height="3328" alt="architecture" src="https://github.com/user-attachments/assets/6b0b8c61-be81-4e32-835d-ff458900844e" />

The pipeline follows a modern data lakehouse architecture with the following components:


1.**Data Ingestion**: Raw data lands in S3 staging area

2.**Event-Driven Processing**: EventBridge detects new files and triggers Step Functions workflow

3.**Orchestration**: Step Functions coordinates the ETL jobs in sequence

4.**Data Processing**: AWS Glue jobs transform data through bronze, silver, and gold layers

5.**Storage**: Apache Iceberg tables provide ACID transactions and time travel capabilities

# Data Flow
S3 (Raw Data) → Bronze (Raw + Metadata) → Silver (Transformed) → Gold (Aggregated)

# Project Structure

aws-ete-de-project/

├── crawlers/                      # AWS Glue Crawler configurations
├── data/                          # Sample data files
│   ├── nyc_taxi/                  # NYC taxi trip data
│   └── reference/                 # Reference data (taxi zones)
├── ddl/                           # Table definitions
├── etl-scripts/                   # AWS Glue ETL scripts
├── eventbridge-rule/              # EventBridge rule definitions
└── step-function/                 # Step Functions workflow definition
