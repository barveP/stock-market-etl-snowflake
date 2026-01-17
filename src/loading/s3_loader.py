import io
import logging
from datetime import datetime
from typing import List

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config.config import AWSConfig

logger = logging.getLogger(__name__)


class S3Loader:
    def __init__(self):
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWSConfig.ACCESS_KEY_ID,
            aws_secret_access_key=AWSConfig.SECRET_ACCESS_KEY,
            region_name=AWSConfig.REGION
        )
        self.bucket = AWSConfig.S3_BUCKET
        self.staging_prefix = AWSConfig.S3_STAGING_PREFIX

    def _generate_key(self, table_name: str, partition_date: str) -> str:
        return f"{self.staging_prefix}{table_name}/date={partition_date}/{table_name}_{partition_date}.parquet"

    def upload_dataframe(self, df: pd.DataFrame, table_name: str) -> str:
        partition_date = datetime.utcnow().strftime("%Y-%m-%d")
        key = self._generate_key(table_name, partition_date)

        buffer = io.BytesIO()
        table = pa.Table.from_pandas(df)
        pq.write_table(table, buffer)
        buffer.seek(0)

        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=buffer.getvalue()
        )

        s3_path = f"s3://{self.bucket}/{key}"
        logger.info(f"Uploaded {table_name} to {s3_path}")
        return s3_path

    def upload_all_tables(
        self,
        dim_sector: pd.DataFrame,
        dim_company: pd.DataFrame,
        fact_prices: pd.DataFrame
    ) -> List[str]:
        paths = []

        paths.append(self.upload_dataframe(dim_sector, "dim_sector"))
        paths.append(self.upload_dataframe(dim_company, "dim_company"))
        paths.append(self.upload_dataframe(fact_prices, "fact_daily_prices"))

        logger.info(f"Uploaded {len(paths)} tables to S3")
        return paths

    def list_staging_files(self, table_name: str = None) -> List[str]:
        prefix = self.staging_prefix
        if table_name:
            prefix = f"{self.staging_prefix}{table_name}/"

        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=prefix
        )

        files = []
        if "Contents" in response:
            files = [obj["Key"] for obj in response["Contents"]]

        return files
