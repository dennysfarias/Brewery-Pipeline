# AB Inbev Data Engineering Case - Brewery Pipeline

## Overview
Data pipeline that ingests brewery data from the [Open Brewery DB API](https://www.openbrewerydb.org/), transforms it, and creates aggregated views. Uses the **Medallion Architecture** (Bronze, Silver, Gold) with **Apache Airflow** for orchestration and **PySpark** for processing.

## Architecture

### Medallion Architecture Overview

Three-layer data architecture:

```
┌─────────────────────────────────────────────────────────────┐
│                    Open Brewery DB API                      │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (Raw)                       │
│  • Format: JSON (native API format)                         │
│  • Storage: datalake/bronze/breweries_raw.json              │
│  • Purpose: Raw data preservation                           │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    SILVER LAYER (Curated)                   │
│  • Format: Parquet (columnar, compressed)                   │
│  • Partitioning: By state (location)                        │
│  • Storage: datalake/silver/breweries/state=.../*.parquet   │
│  • Purpose: Cleaned, validated, optimized data              │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    GOLD LAYER (Aggregated)                  │
│  • Format: Parquet (columnar, compressed)                   │
│  • Storage: datalake/gold/brewery_summary.parquet           │
│  • Purpose: Business-ready aggregated metrics               │
└─────────────────────────────────────────────────────────────┘
```

### 1. Ingestion (Bronze Layer)

**Source**: Open Brewery DB API (`/breweries` endpoint)

**Features**:
- Automatic pagination (200 records per page)
- Retry mechanism with exponential backoff (3 retries)
- Rate limiting (0.5s delay between requests)
- Error handling with logging

**Storage**: 
- Format: Raw JSON (native API format)
- Location: `datalake/bronze/breweries_raw.json`
- Purpose: Preserve raw data for audit and reprocessing

**Implementation**: `src/api_client.py` + `src/ingestion.py`

### 2. Transformation (Silver Layer)

**Input**: Bronze JSON file

**Transformations Applied**:
1. **Data Cleaning**:
   - Trim whitespace from state values
   - Replace null/missing values with "Unknown"
   - Sanitize partition names to remove filesystem-unsafe characters

2. **Schema Validation**:
   - Validate DataFrame schema before writing
   - Detect complex nested structures
   - Verify required columns exist

3. **Partition Sanitization**:
   - Remove special characters that cause filesystem issues (`/ \ : * ? " < > |`)
   - Remove control characters
   - Truncate names exceeding 255 character limit
   - Handle empty/null partition values

**Storage**:
- Format: Parquet (columnar storage)
- Compression: Snappy (explicitly configured)
- Partitioning: By `state` (location-based)
- Location: `datalake/silver/breweries/state=.../*.parquet`
- Purpose: Optimized for analytical queries with location filtering

**Optimizations**:
- Columnar storage for efficient column pruning
- Partitioning enables partition elimination in queries
- Snappy compression for good balance between speed and size
- Atomic writes (temp directory + move) to prevent corruption

**Implementation**: `src/transformations.py` - `process_bronze_to_silver()`

### 3. Aggregation (Gold Layer)

**Input**: Silver Parquet files (partitioned by state)

**Aggregations**:
- Group by: `brewery_type` and `state`
- Metric: Count of breweries per group
- Output: `brewery_type`, `state`, `brewery_count`

**Data Quality Validations**:
- Verify aggregation produced results
- Validate all counts are positive
- Handle null values in grouping columns
- Validate total brewery count is reasonable

**Storage**:
- Format: Parquet (columnar storage)
- Compression: Snappy
- Location: `datalake/gold/brewery_summary.parquet`
- Purpose: Business-ready aggregated metrics for reporting

**Implementation**: `src/transformations.py` - `process_silver_to_gold()`

## Technology Stack

- **Orchestration**: Apache Airflow 2.10.4
- **Processing**: PySpark (distributed data processing)
- **Storage Formats**: JSON (Bronze), Parquet (Silver/Gold)
- **Compression**: Snappy (for Parquet files)
- **Containerization**: Docker & Docker Compose
- **Language**: Python 3.8+
- **Database**: PostgreSQL (Airflow metadata)

## Prerequisites

- Docker & Docker Compose
- Java 17 (included in Docker image for PySpark)

## Project Structure

```
.
├── dags/                      # Airflow DAG definitions
│   └── brewery_pipeline.py   # Main pipeline DAG
├── src/                       # Source code modules
│   ├── api_client.py          # Open Brewery DB API client
│   ├── ingestion.py           # Bronze layer ingestion
│   └── transformations.py     # Silver/Gold transformations
├── tests/                     # Unit tests
│   └── test_transformations.py
├── datalake/                  # Data storage (created at runtime)
│   ├── bronze/                # Raw JSON data
│   ├── silver/                # Curated Parquet data
│   └── gold/                  # Aggregated Parquet data
├── logs/                      # Airflow logs
├── docker-compose.yaml        # Docker services configuration
├── Dockerfile                 # Custom Airflow image with Java/PySpark
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

## How to Run

### Option 1: Docker (Recommended)

1. **Initialize Airflow**:
   ```bash
   # Linux/Mac
   echo "AIRFLOW_UID=$(id -u)" > .env
   
   # Windows (PowerShell)
   echo "AIRFLOW_UID=50000" > .env
   
   docker-compose up airflow-init
   ```

2. **Start Services**:
   ```bash
   docker-compose up -d
   ```

3. **Access Airflow UI**:
   - Open `http://localhost:8080` in your browser
   - Login with `airflow` / `airflow`

4. **Trigger Pipeline**:
   - Go to DAGs page and find `brewery_pipeline`
   - Click the play button to trigger manually

## Data Quality & Validation

The pipeline includes validation and quality checks:

- Schema validation before writing Parquet files
- Empty data checks to prevent processing invalid datasets
- Null value handling for critical columns (`state`, `brewery_type`)
- Partition name sanitization for filesystem compatibility
- Aggregation validation to verify results

Error handling includes retry mechanisms for API calls, cleanup on failures, and schema merging for Parquet files to handle inconsistencies.

## Monitoring & Alerting

The pipeline uses logging and the Airflow UI for monitoring. For production, I should setup

- Airflow alerts: email notifications or Slack callbacks for DAG failures
- Data quality checks: Great Expectations or SQLCheckOperators for validation
- Key metrics: execution time, record counts, error rates, API response times, Spark job performance

## Design Choices

**Medallion Architecture**: Clear separation between raw, curated, and aggregated data. Good for reprocessing and audit trails, but adds storage overhead.

**Apache Airflow**: Standard orchestration tool with good ecosystem. Handles retries and dependencies well, though there's a learning curve.

**PySpark**: Works well for distributed processing and large datasets. Optimized for Parquet, but requires Java and is more complex than Pandas for small data.

**Parquet Format**: Columnar storage with compression and good query performance. Not human-readable, but efficient for analytics.

**Partitioning by State**: Enables efficient location-based queries. Can create many small files, so partition names need sanitization.

**Snappy Compression**: Fast compression/decompression. Chosen over gzip for better analytics performance.

The pipeline uses atomic writes (temp + move), schema merging, and columnar storage for efficient reads.

## Testing

Run tests with pytest:

```bash
docker-compose run --rm --entrypoint /bin/bash airflow-scheduler -c "pytest /opt/airflow/tests"
```

Test coverage includes Silver layer transformations (partitioning, data cleaning), Gold layer aggregations, and edge cases like null values and missing columns.

## Future Enhancements

Potential improvements:

1. Incremental processing: track timestamps and only process new/updated records
2. Data quality framework: integrate Great Expectations for automated profiling
3. Schema evolution: handle schema changes gracefully with versioning
4. Monitoring dashboard: real-time metrics and data quality scores
5. Cloud integration: S3/ADLS storage, managed Airflow services, auto-scaling Spark clusters
6. Scalability: Refactor ingestion to use generators and NDJSON for lower memory footprint
