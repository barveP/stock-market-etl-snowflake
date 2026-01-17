import requests
import pandas as pd
from typing import List, Dict


def get_sp500_tickers() -> List[Dict[str, str]]:
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tables = pd.read_html(url)
    df = tables[0]

    tickers = []
    for _, row in df.iterrows():
        tickers.append({
            "symbol": row["Symbol"].replace(".", "-"),
            "company_name": row["Security"],
            "sector": row["GICS Sector"],
            "sub_industry": row["GICS Sub-Industry"],
            "headquarters": row["Headquarters Location"],
            "date_added": str(row["Date added"]) if pd.notna(row["Date added"]) else None,
            "cik": str(row["CIK"]) if pd.notna(row["CIK"]) else None
        })

    return tickers
