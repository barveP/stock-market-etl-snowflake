-- Database and Schema
CREATE DATABASE IF NOT EXISTS STOCK_MARKET;
USE DATABASE STOCK_MARKET;
CREATE SCHEMA IF NOT EXISTS PUBLIC;

-- Dimension: Sector
CREATE OR REPLACE TABLE dim_sector (
    sector_id INTEGER PRIMARY KEY,
    sector VARCHAR(100) NOT NULL,
    created_at TIMESTAMP_NTZ
);

-- Dimension: Company
CREATE OR REPLACE TABLE dim_company (
    company_id INTEGER PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL UNIQUE,
    company_name VARCHAR(200),
    sector_id INTEGER REFERENCES dim_sector(sector_id),
    sub_industry VARCHAR(200),
    headquarters VARCHAR(200),
    date_added DATE,
    cik VARCHAR(20),
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

-- Fact: Daily Prices
CREATE OR REPLACE TABLE fact_daily_prices (
    price_id INTEGER PRIMARY KEY,
    company_id INTEGER REFERENCES dim_company(company_id),
    date DATE NOT NULL,
    open DECIMAL(18,4),
    high DECIMAL(18,4),
    low DECIMAL(18,4),
    close DECIMAL(18,4),
    volume BIGINT,
    extracted_at TIMESTAMP_NTZ,
    loaded_at TIMESTAMP_NTZ
);

-- Indexes
CREATE OR REPLACE INDEX idx_prices_date ON fact_daily_prices(date);
CREATE OR REPLACE INDEX idx_prices_company ON fact_daily_prices(company_id);
CREATE OR REPLACE INDEX idx_company_symbol ON dim_company(symbol);

-- File Format for Parquet
CREATE OR REPLACE FILE FORMAT parquet_format
    TYPE = 'PARQUET'
    COMPRESSION = 'SNAPPY';

-- External Stage for S3
CREATE OR REPLACE STAGE stock_etl_stage
    URL = 's3://stock-market-etl-bucket/staging/'
    CREDENTIALS = (AWS_KEY_ID = '' AWS_SECRET_KEY = '')
    FILE_FORMAT = parquet_format;
