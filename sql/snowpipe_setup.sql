USE DATABASE STOCK_MARKET;
USE SCHEMA PUBLIC;

-- Snowpipe for dim_sector
CREATE OR REPLACE PIPE dim_sector_pipe
    AUTO_INGEST = TRUE
AS
COPY INTO dim_sector
FROM (
    SELECT
        $1:sector_id::INTEGER,
        $1:sector::VARCHAR,
        $1:created_at::TIMESTAMP_NTZ
    FROM @stock_etl_stage/dim_sector/
)
FILE_FORMAT = parquet_format
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Snowpipe for dim_company
CREATE OR REPLACE PIPE dim_company_pipe
    AUTO_INGEST = TRUE
AS
COPY INTO dim_company
FROM (
    SELECT
        $1:company_id::INTEGER,
        $1:symbol::VARCHAR,
        $1:company_name::VARCHAR,
        $1:sector_id::INTEGER,
        $1:sub_industry::VARCHAR,
        $1:headquarters::VARCHAR,
        $1:date_added::DATE,
        $1:cik::VARCHAR,
        $1:created_at::TIMESTAMP_NTZ,
        $1:updated_at::TIMESTAMP_NTZ
    FROM @stock_etl_stage/dim_company/
)
FILE_FORMAT = parquet_format
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Snowpipe for fact_daily_prices
CREATE OR REPLACE PIPE fact_daily_prices_pipe
    AUTO_INGEST = TRUE
AS
COPY INTO fact_daily_prices
FROM (
    SELECT
        $1:price_id::INTEGER,
        $1:company_id::INTEGER,
        $1:date::DATE,
        $1:open::DECIMAL(18,4),
        $1:high::DECIMAL(18,4),
        $1:low::DECIMAL(18,4),
        $1:close::DECIMAL(18,4),
        $1:volume::BIGINT,
        $1:extracted_at::TIMESTAMP_NTZ,
        $1:loaded_at::TIMESTAMP_NTZ
    FROM @stock_etl_stage/fact_daily_prices/
)
FILE_FORMAT = parquet_format
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Get notification channel ARN for S3 event setup
SHOW PIPES;
SELECT SYSTEM$PIPE_STATUS('dim_sector_pipe');
SELECT SYSTEM$PIPE_STATUS('dim_company_pipe');
SELECT SYSTEM$PIPE_STATUS('fact_daily_prices_pipe');
