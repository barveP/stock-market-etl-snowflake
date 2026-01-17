import logging
from typing import Optional

import snowflake.connector

from config.config import SnowflakeConfig, AWSConfig

logger = logging.getLogger(__name__)


class SnowflakeLoader:
    def __init__(self):
        self.conn = None

    def connect(self):
        self.conn = snowflake.connector.connect(
            account=SnowflakeConfig.ACCOUNT,
            user=SnowflakeConfig.USER,
            password=SnowflakeConfig.PASSWORD,
            database=SnowflakeConfig.DATABASE,
            schema=SnowflakeConfig.SCHEMA,
            warehouse=SnowflakeConfig.WAREHOUSE,
            role=SnowflakeConfig.ROLE
        )
        logger.info("Connected to Snowflake")

    def disconnect(self):
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from Snowflake")

    def execute_query(self, query: str) -> Optional[list]:
        cursor = self.conn.cursor()
        try:
            cursor.execute(query)
            return cursor.fetchall()
        finally:
            cursor.close()

    def refresh_snowpipe(self, pipe_name: str, s3_path: str):
        query = f"ALTER PIPE {pipe_name} REFRESH"
        self.execute_query(query)
        logger.info(f"Refreshed Snowpipe {pipe_name}")

    def check_pipe_status(self, pipe_name: str) -> dict:
        query = f"SELECT SYSTEM$PIPE_STATUS('{pipe_name}')"
        result = self.execute_query(query)
        logger.info(f"Pipe status for {pipe_name}: {result}")
        return result

    def get_copy_history(self, table_name: str, hours: int = 24) -> list:
        query = f"""
            SELECT *
            FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
                TABLE_NAME => '{table_name}',
                START_TIME => DATEADD(HOURS, -{hours}, CURRENT_TIMESTAMP())
            ))
            ORDER BY LAST_LOAD_TIME DESC
        """
        return self.execute_query(query)

    def validate_load(self, table_name: str, expected_count: int) -> bool:
        query = f"SELECT COUNT(*) FROM {table_name}"
        result = self.execute_query(query)
        actual_count = result[0][0] if result else 0
        is_valid = actual_count >= expected_count
        logger.info(f"Load validation for {table_name}: expected={expected_count}, actual={actual_count}")
        return is_valid
