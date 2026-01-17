import logging
from datetime import datetime
from typing import List, Dict, Tuple

import pandas as pd

from src.transformation.validators import DataValidator

logger = logging.getLogger(__name__)


class StockDataTransformer:
    def __init__(self):
        self.validator = DataValidator()

    def create_dim_sector(self, company_data: List[Dict]) -> pd.DataFrame:
        df = pd.DataFrame(company_data)
        sectors = df[["sector"]].drop_duplicates()
        sectors = sectors.reset_index(drop=True)
        sectors["sector_id"] = range(1, len(sectors) + 1)
        sectors["created_at"] = datetime.utcnow().isoformat()

        return sectors[["sector_id", "sector", "created_at"]]

    def create_dim_company(self, company_data: List[Dict], dim_sector: pd.DataFrame) -> pd.DataFrame:
        df = pd.DataFrame(company_data)
        df = df.merge(dim_sector[["sector_id", "sector"]], on="sector", how="left")

        df["company_id"] = range(1, len(df) + 1)
        df["created_at"] = datetime.utcnow().isoformat()
        df["updated_at"] = datetime.utcnow().isoformat()

        return df[[
            "company_id", "symbol", "company_name", "sector_id",
            "sub_industry", "headquarters", "date_added", "cik",
            "created_at", "updated_at"
        ]]

    def create_fact_daily_prices(self, price_data: List[Dict], dim_company: pd.DataFrame) -> pd.DataFrame:
        df = pd.DataFrame(price_data)

        company_lookup = dim_company[["company_id", "symbol"]].copy()
        df = df.merge(company_lookup, on="symbol", how="left")

        df["price_id"] = range(1, len(df) + 1)
        df["date"] = pd.to_datetime(df["date"])
        df["loaded_at"] = datetime.utcnow().isoformat()

        df["open"] = df["open"].round(4)
        df["high"] = df["high"].round(4)
        df["low"] = df["low"].round(4)
        df["close"] = df["close"].round(4)

        return df[[
            "price_id", "company_id", "date", "open", "high",
            "low", "close", "volume", "extracted_at", "loaded_at"
        ]]

    def transform(
        self,
        company_data: List[Dict],
        price_data: List[Dict]
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, List[Dict]]:
        logger.info("Starting transformation")

        price_df = pd.DataFrame(price_data)
        is_valid, validation_results = self.validator.validate_price_data(price_df)

        if not is_valid:
            logger.warning("Validation failed, proceeding with valid records only")
            price_df = self._clean_invalid_records(price_df)

        dim_sector = self.create_dim_sector(company_data)
        logger.info(f"Created dim_sector with {len(dim_sector)} sectors")

        dim_company = self.create_dim_company(company_data, dim_sector)
        logger.info(f"Created dim_company with {len(dim_company)} companies")

        fact_prices = self.create_fact_daily_prices(price_data, dim_company)
        logger.info(f"Created fact_daily_prices with {len(fact_prices)} records")

        return dim_sector, dim_company, fact_prices, validation_results

    def _clean_invalid_records(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.dropna(subset=["symbol", "date"])
        df = df.drop_duplicates(subset=["symbol", "date"])

        price_cols = ["open", "high", "low", "close"]
        for col in price_cols:
            df = df[df[col] > 0]

        df = df[df["volume"] >= 0]
        return df
