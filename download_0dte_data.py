import os
import sys
import time
import logging
from datetime import datetime, timedelta
from typing import List, Optional

import requests
import pandas as pd
# --20240229

BASE_URL = "http://localhost:25503/v3/option/history/greeks/first_order"
SYMBOLS: List[str] = ["SPY"]
# ["SPXW", "SPY", "QQQ"]
START_DATE = datetime(2025,1,1)
END_DATE = datetime(2025, 12, 5)  # inclusive
INTERVAL = "1m"
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data")

# Simple rate limiting / retry settings
REQUESTS_PER_SECOND = 5
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def ensure_output_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def build_params(symbol: str, date_obj: datetime) -> dict:
    date_str = date_obj.strftime("%Y%m%d")
    return {
        "symbol": symbol,
        "expiration": date_str,  # 0DTE: expiration == trade date
        "date": date_str,
        "interval": INTERVAL,
        "format": "json",
    }


def fetch_day(symbol: str, date_obj: datetime) -> Optional[pd.DataFrame]:
    params = build_params(symbol, date_obj)
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            resp = requests.get(BASE_URL, params=params, timeout=30)
            if resp.status_code == 429:
                # rate limited
                logging.warning("Rate limited, sleeping before retry...")
                time.sleep(RETRY_BACKOFF_SECONDS)
                attempt += 1
                continue

            if not resp.ok:
                logging.error(
                    "Request failed for %s %s (status %s): %s",
                    symbol,
                    params["date"],
                    resp.status_code,
                    resp.text[:500],
                )
                return None

            data = resp.json()
            if not data:
                logging.info("No data for %s %s", symbol, params["date"])
                return None

            # Expecting list of records; if dict, try to unwrap
            if isinstance(data, dict):
                # try common key names, else wrap in list
                for key in ("data", "results", "items"):
                    if key in data and isinstance(data[key], list):
                        data = data[key]
                        break
                else:
                    data = [data]

            df = pd.DataFrame(data)
            if df.empty:
                logging.info("Empty dataframe for %s %s", symbol, params["date"])
                return None

            df["symbol"] = symbol
            df["trade_date"] = date_obj.date()
            return df
        except requests.RequestException as e:
            logging.warning(
                "Network error on attempt %d for %s %s: %s",
                attempt + 1,
                symbol,
                params["date"],
                e,
            )
            time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))
            attempt += 1

    logging.error("Failed to fetch after %d retries for %s %s", MAX_RETRIES, symbol, params["date"])
    return None


def save_parquet(df: pd.DataFrame, symbol: str, date_obj: datetime, out_dir: str) -> None:
    date_str = date_obj.strftime("%Y%m%d")
    filename = f"{symbol}_{date_str}.parquet"
    out_path = os.path.join(out_dir, filename)
    df.to_parquet(out_path, index=False)
    logging.info("Saved %s rows to %s", len(df), out_path)


def daterange(start: datetime, end_inclusive: datetime):
    cur = start
    while cur <= end_inclusive:
        yield cur
        cur += timedelta(days=1)


def main() -> None:
    setup_logging()
    ensure_output_dir(OUTPUT_DIR)

    logging.info(
        "Starting 0DTE download for symbols=%s from %s to %s (interval=%s)",
        SYMBOLS,
        START_DATE.date(),
        END_DATE.date(),
        INTERVAL,
    )

    delay_between_requests = 1.0 / REQUESTS_PER_SECOND if REQUESTS_PER_SECOND > 0 else 0.0

    for date_obj in daterange(START_DATE, END_DATE):
        for symbol in SYMBOLS:
            logging.info("Fetching %s %s", symbol, date_obj.date())
            df = fetch_day(symbol, date_obj)
            if df is not None and not df.empty:
                save_parquet(df, symbol, date_obj, OUTPUT_DIR)
            time.sleep(delay_between_requests)

    logging.info("Download completed.")


if __name__ == "__main__":
    main()
