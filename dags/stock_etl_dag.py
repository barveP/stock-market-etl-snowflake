import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.ingestion.sp500_tickers import get_sp500_tickers
from src.ingestion.yahoo_finance import YahooFinanceExtractor
from src.transformation.transformers import StockDataTransformer
from src.loading.s3_loader import S3Loader
from src.loading.snowflake_loader import SnowflakeLoader

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30)
}


def extract_data(**context):
    logger.info("Starting data extraction")

    company_data = get_sp500_tickers()
    logger.info(f"Fetched {len(company_data)} S&P 500 companies")

    tickers = [c["symbol"] for c in company_data]

    extractor = YahooFinanceExtractor()
    price_data = extractor.extract_for_date_range(tickers, days_back=1)
    logger.info(f"Extracted {len(price_data)} price records")

    context["ti"].xcom_push(key="company_data", value=company_data)
    context["ti"].xcom_push(key="price_data", value=price_data)

    return {"companies": len(company_data), "prices": len(price_data)}


def transform_data(**context):
    logger.info("Starting data transformation")

    company_data = context["ti"].xcom_pull(key="company_data", task_ids="extract")
    price_data = context["ti"].xcom_pull(key="price_data", task_ids="extract")

    transformer = StockDataTransformer()
    dim_sector, dim_company, fact_prices, validations = transformer.transform(
        company_data, price_data
    )

    context["ti"].xcom_push(key="dim_sector", value=dim_sector.to_dict("records"))
    context["ti"].xcom_push(key="dim_company", value=dim_company.to_dict("records"))
    context["ti"].xcom_push(key="fact_prices", value=fact_prices.to_dict("records"))
    context["ti"].xcom_push(key="validations", value=validations)

    failed_checks = [v for v in validations if not v["passed"]]
    if failed_checks:
        logger.warning(f"{len(failed_checks)} validation checks failed")

    return {
        "dim_sector": len(dim_sector),
        "dim_company": len(dim_company),
        "fact_prices": len(fact_prices),
        "validations_passed": len(validations) - len(failed_checks)
    }


def load_to_s3(**context):
    logger.info("Starting S3 load")

    import pandas as pd

    dim_sector = pd.DataFrame(context["ti"].xcom_pull(key="dim_sector", task_ids="transform"))
    dim_company = pd.DataFrame(context["ti"].xcom_pull(key="dim_company", task_ids="transform"))
    fact_prices = pd.DataFrame(context["ti"].xcom_pull(key="fact_prices", task_ids="transform"))

    loader = S3Loader()
    paths = loader.upload_all_tables(dim_sector, dim_company, fact_prices)

    context["ti"].xcom_push(key="s3_paths", value=paths)
    logger.info(f"Loaded {len(paths)} tables to S3")

    return {"s3_paths": paths}


def notify_snowpipe(**context):
    logger.info("Starting Snowpipe notification")

    s3_paths = context["ti"].xcom_pull(key="s3_paths", task_ids="load_s3")

    loader = SnowflakeLoader()
    loader.connect()

    try:
        pipes = {
            "dim_sector": "STOCK_MARKET.PUBLIC.DIM_SECTOR_PIPE",
            "dim_company": "STOCK_MARKET.PUBLIC.DIM_COMPANY_PIPE",
            "fact_daily_prices": "STOCK_MARKET.PUBLIC.FACT_DAILY_PRICES_PIPE"
        }

        for table_name, pipe_name in pipes.items():
            matching_path = next((p for p in s3_paths if table_name in p), None)
            if matching_path:
                loader.refresh_snowpipe(pipe_name, matching_path)
                status = loader.check_pipe_status(pipe_name)
                logger.info(f"Pipe {pipe_name} status: {status}")
    finally:
        loader.disconnect()

    return {"pipes_refreshed": list(pipes.keys())}


with DAG(
    dag_id="stock_market_etl",
    default_args=default_args,
    description="Daily S&P 500 stock data ETL pipeline",
    schedule_interval="0 18 * * 1-5",
    catchup=False,
    tags=["etl", "stocks", "s3", "snowflake"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_data,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
        provide_context=True
    )

    load_s3_task = PythonOperator(
        task_id="load_s3",
        python_callable=load_to_s3,
        provide_context=True
    )

    snowpipe_task = PythonOperator(
        task_id="notify_snowpipe",
        python_callable=notify_snowpipe,
        provide_context=True
    )

    extract_task >> transform_task >> load_s3_task >> snowpipe_task
