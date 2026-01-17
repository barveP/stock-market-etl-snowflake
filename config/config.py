import os
from dotenv import load_dotenv

load_dotenv()


class AWSConfig:
    ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    REGION = os.getenv("AWS_REGION", "us-east-1")
    S3_BUCKET = os.getenv("S3_BUCKET")
    S3_STAGING_PREFIX = os.getenv("S3_STAGING_PREFIX", "staging/")


class SnowflakeConfig:
    ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
    USER = os.getenv("SNOWFLAKE_USER")
    PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
    DATABASE = os.getenv("SNOWFLAKE_DATABASE", "STOCK_MARKET")
    SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
    WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    ROLE = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")


class ETLConfig:
    BATCH_SIZE = 50
    MAX_RETRIES = 3
    RETRY_DELAY = 5
    DATA_PERIOD = "1d"
    DATA_INTERVAL = "1d"
