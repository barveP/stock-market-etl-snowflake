import logging
from typing import List, Dict, Tuple
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


class DataValidator:
    def __init__(self):
        self.validation_results = []

    def _log_result(self, check_name: str, passed: bool, message: str):
        self.validation_results.append({
            "check": check_name,
            "passed": passed,
            "message": message,
            "timestamp": datetime.utcnow().isoformat()
        })
        if passed:
            logger.info(f"PASSED: {check_name} - {message}")
        else:
            logger.error(f"FAILED: {check_name} - {message}")

    def check_not_empty(self, df: pd.DataFrame, name: str) -> bool:
        passed = len(df) > 0
        self._log_result("not_empty", passed, f"{name} has {len(df)} records")
        return passed

    def check_no_null_symbols(self, df: pd.DataFrame) -> bool:
        null_count = df["symbol"].isnull().sum()
        passed = null_count == 0
        self._log_result("no_null_symbols", passed, f"Found {null_count} null symbols")
        return passed

    def check_no_null_dates(self, df: pd.DataFrame) -> bool:
        null_count = df["date"].isnull().sum()
        passed = null_count == 0
        self._log_result("no_null_dates", passed, f"Found {null_count} null dates")
        return passed

    def check_positive_prices(self, df: pd.DataFrame) -> bool:
        price_cols = ["open", "high", "low", "close"]
        negative_count = (df[price_cols] < 0).sum().sum()
        passed = negative_count == 0
        self._log_result("positive_prices", passed, f"Found {negative_count} negative prices")
        return passed

    def check_positive_volume(self, df: pd.DataFrame) -> bool:
        negative_count = (df["volume"] < 0).sum()
        passed = negative_count == 0
        self._log_result("positive_volume", passed, f"Found {negative_count} negative volumes")
        return passed

    def check_ohlc_relationship(self, df: pd.DataFrame) -> bool:
        invalid = ((df["high"] < df["low"]) |
                   (df["high"] < df["open"]) |
                   (df["high"] < df["close"]) |
                   (df["low"] > df["open"]) |
                   (df["low"] > df["close"])).sum()
        passed = invalid == 0
        self._log_result("ohlc_relationship", passed, f"Found {invalid} invalid OHLC relationships")
        return passed

    def check_date_format(self, df: pd.DataFrame) -> bool:
        try:
            pd.to_datetime(df["date"])
            passed = True
            message = "All dates are valid"
        except Exception as e:
            passed = False
            message = f"Invalid date format: {e}"
        self._log_result("date_format", passed, message)
        return passed

    def check_no_duplicates(self, df: pd.DataFrame) -> bool:
        duplicates = df.duplicated(subset=["symbol", "date"]).sum()
        passed = duplicates == 0
        self._log_result("no_duplicates", passed, f"Found {duplicates} duplicate records")
        return passed

    def check_symbol_format(self, df: pd.DataFrame) -> bool:
        invalid = df["symbol"].str.contains(r"[^A-Z0-9\-\.]", regex=True, na=True).sum()
        passed = invalid == 0
        self._log_result("symbol_format", passed, f"Found {invalid} invalid symbol formats")
        return passed

    def check_price_precision(self, df: pd.DataFrame) -> bool:
        price_cols = ["open", "high", "low", "close"]
        max_decimals = df[price_cols].applymap(
            lambda x: len(str(x).split(".")[-1]) if "." in str(x) else 0
        ).max().max()
        passed = max_decimals <= 6
        self._log_result("price_precision", passed, f"Max decimal places: {max_decimals}")
        return passed

    def check_reasonable_prices(self, df: pd.DataFrame) -> bool:
        price_cols = ["open", "high", "low", "close"]
        extreme = ((df[price_cols] > 100000) | (df[price_cols] < 0.01)).sum().sum()
        passed = extreme == 0
        self._log_result("reasonable_prices", passed, f"Found {extreme} extreme prices")
        return passed

    def check_volume_range(self, df: pd.DataFrame) -> bool:
        extreme = (df["volume"] > 1e12).sum()
        passed = extreme == 0
        self._log_result("volume_range", passed, f"Found {extreme} extreme volumes")
        return passed

    def validate_price_data(self, df: pd.DataFrame) -> Tuple[bool, List[Dict]]:
        self.validation_results = []

        checks = [
            self.check_not_empty(df, "price_data"),
            self.check_no_null_symbols(df),
            self.check_no_null_dates(df),
            self.check_positive_prices(df),
            self.check_positive_volume(df),
            self.check_ohlc_relationship(df),
            self.check_date_format(df),
            self.check_no_duplicates(df),
            self.check_symbol_format(df),
            self.check_price_precision(df),
            self.check_reasonable_prices(df),
            self.check_volume_range(df)
        ]

        all_passed = all(checks)
        return all_passed, self.validation_results
