import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import yfinance as yf
import pandas as pd

from config.config import ETLConfig

logger = logging.getLogger(__name__)


class YahooFinanceExtractor:
    def __init__(self):
        self.max_retries = ETLConfig.MAX_RETRIES
        self.retry_delay = ETLConfig.RETRY_DELAY
        self.batch_size = ETLConfig.BATCH_SIZE

    def _fetch_with_retry(self, ticker: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        for attempt in range(self.max_retries):
            try:
                stock = yf.Ticker(ticker)
                df = stock.history(start=start_date, end=end_date, interval="1d")

                if df.empty:
                    logger.warning(f"No data returned for {ticker}")
                    return None

                df = df.reset_index()
                df["symbol"] = ticker
                return df

            except Exception as e:
                wait_time = self.retry_delay * (2 ** attempt)
                logger.warning(f"Attempt {attempt + 1} failed for {ticker}: {e}. Retrying in {wait_time}s")
                time.sleep(wait_time)

        logger.error(f"All retries exhausted for {ticker}")
        return None

    def extract_daily_prices(self, tickers: List[str], start_date: str, end_date: str) -> List[Dict]:
        all_records = []

        for i in range(0, len(tickers), self.batch_size):
            batch = tickers[i:i + self.batch_size]
            logger.info(f"Processing batch {i // self.batch_size + 1}: {len(batch)} tickers")

            for ticker in batch:
                df = self._fetch_with_retry(ticker, start_date, end_date)

                if df is not None:
                    for _, row in df.iterrows():
                        all_records.append({
                            "symbol": ticker,
                            "date": row["Date"].strftime("%Y-%m-%d"),
                            "open": float(row["Open"]),
                            "high": float(row["High"]),
                            "low": float(row["Low"]),
                            "close": float(row["Close"]),
                            "volume": int(row["Volume"]),
                            "extracted_at": datetime.utcnow().isoformat()
                        })

            time.sleep(1)

        logger.info(f"Extracted {len(all_records)} records for {len(tickers)} tickers")
        return all_records

    def extract_for_date_range(self, tickers: List[str], days_back: int = 1) -> List[Dict]:
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        return self.extract_daily_prices(tickers, start_date, end_date)
