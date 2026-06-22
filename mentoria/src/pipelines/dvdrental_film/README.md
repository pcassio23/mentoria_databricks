# DVD Rental Film Pipeline

This folder defines the data pipeline for DVD rental film data from PostgreSQL.

## Pipeline Structure

This pipeline follows a medallion architecture with three layers:

### Bronze Layer (`transformations/bronze/`)
- Raw data ingestion from PostgreSQL database
- Minimal transformations, preserving source schema
- Data quality checks for completeness

### Silver Layer (`transformations/silver/`)
- Cleaned and validated data
- Business logic transformations
- Data type standardization
- Deduplication

### Gold Layer (`transformations/gold/`)
- Aggregated, business-ready datasets
- Optimized for analytics and reporting
- Denormalized for performance

## Getting Started

1. Configure PostgreSQL connection in bronze layer
2. Define transformations in each layer folder
3. Create a Spark Declarative Pipeline using these transformations

For more information on Spark Declarative Pipelines, see https://docs.databricks.com/dlt/
