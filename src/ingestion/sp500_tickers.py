import requests
import pandas as pd
from typing import List, Dict
from io import StringIO


def get_sp500_tickers() -> List[Dict[str, str]]:
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "stock-market-etl/1.0"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    tables = pd.read_html(StringIO(response.text))
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
