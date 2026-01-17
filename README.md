# Stock Market ETL Pipeline

ETL pipeline for S&P 500 stock data using Apache Airflow, AWS S3, and Snowflake.

## Architecture

```
Yahoo Finance API -> Airflow DAG -> S3 (Parquet) -> Snowpipe -> Snowflake
```

**Pipeline Stages:**
1. **Extract**: Fetch daily OHLCV data for 500 S&P 500 tickers
2. **Transform**: Create dimension/fact tables with 12 data quality checks
3. **Load**: Upload parquet files to S3 staging bucket
4. **Ingest**: Auto-ingest into Snowflake via Snowpipe (5-10s latency)

## Data Model

- `dim_sector`: Sector reference data
- `dim_company`: Company master data with sector FK
- `fact_daily_prices`: Daily OHLCV price records

## Setup

### 1. Environment Variables

```bash
cp .env.example .env
# Edit .env with your credentials
```

### 2. Snowflake Setup

```sql
-- Run in Snowflake
source sql/snowflake_setup.sql
source sql/snowpipe_setup.sql
```

### 3. Run with Docker

```bash
cd docker
docker-compose up -d
```

Access Airflow UI at `http://localhost:8080` (admin/admin)

## Project Structure

```
├── dags/                 # Airflow DAGs
├── src/
│   ├── ingestion/        # Data extraction
│   ├── transformation/   # Data transformation & validation
│   └── loading/          # S3 and Snowflake loaders
├── config/               # Configuration
├── sql/                  # Snowflake DDL scripts
└── docker/               # Docker setup
```

## Tech Stack

Python, Apache Airflow, AWS S3, Snowflake, Snowpipe, Docker, yfinance
