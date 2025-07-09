
"""
Fetch 5-min OHLCV data for NIFTY, BANKNIFTY, FINNIFTY using Upstox API.
Save as CSV for further processing.

Requirements:
- pip install upstox-python pandas
- Upstox API credentials (api_key, api_secret, redirect_uri, access_token)
"""
import csv
import datetime
import time
from upstox_client import ApiClient, Configuration
from upstox_client.api.history_v3_api import HistoryV3Api
import requests


# ---- USER: Fill your credentials here ----
API_KEY = "906a291c-a069-4932-8ba9-e4d48deee192"
API_SECRET = "x9zfab4ah9"
REDIRECT_URI = "https://www.google.com/"
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI0RUFRRFciLCJqdGkiOiI2ODZlNTg4YThlYzdhMTUzOWY1NWI3MDEiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzUyMDYyMDkwLCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NTIwOTg0MDB9.SEc1a2MJVUN18HX7takY5Ayw-kGA4P_-jLEXB37HzF8"  # Generate using OAuth2 flow

# ---- SYMBOL TOKENS (NSE Index Futures) ----
SYMBOLS = {
    "NIFTY": {"instrument_key": "NSE_INDEX|Nifty 50"}
}

# ---- Date Range ----
START_DATE = "2025-06-08"  # yyyy-mm-dd
END_DATE = "2025-06-09"    # yyyy-mm-dd

# ---- Helper: Fetch 5-min candles ----
def fetch_5min_candles(api, instrument_key, start_date, end_date):
    unit = "minutes"
    interval = 15
    all_candles = []
    from_date = start_date  # e.g., "2025-06-01"
    to_date = end_date      # e.g., "2025-06-30"
    try:
        resp = api.get_historical_candle_data1(
            instrument_key=instrument_key,
            unit=unit,
            interval=interval,
            to_date=to_date,
            from_date=from_date
        )
        # Always extract candles from resp.data["candles"] if present
        candles = []
        if hasattr(resp, "data") and resp.data and isinstance(resp.data, dict) and "candles" in resp.data:
            candles = resp.data["candles"]
        # fallback for dict response (rare)
        elif isinstance(resp, dict) and "data" in resp and isinstance(resp["data"], dict) and "candles" in resp["data"]:
            candles = resp["data"]["candles"]
        if candles:
            all_candles.extend(candles)
    except Exception as e:
        print(f"Error fetching {instrument_key}: {e}")
    return all_candles

# ---- Main ----

def main():
    # Configure Upstox API client
    config = Configuration()
    config.api_key["apiKey"] = API_KEY
    config.access_token = ACCESS_TOKEN
    api_client = ApiClient(config)
    api = HistoryV3Api(api_client)

    for symbol, info in SYMBOLS.items():
        print(f"Fetching {symbol}...")
        candles = fetch_5min_candles(api, info["instrument_key"], START_DATE, END_DATE)
        out_path = f"{symbol}_5min_{START_DATE}_to_{END_DATE}.csv"
        columns = ["datetime", "open", "high", "low", "close"]
        with open(out_path, mode="w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)
            if candles:
                for candle in candles:
                    # Only write first 5 fields (ignore volume/oi if present)
                    writer.writerow(candle[:5])
                print(f"Saved {symbol} data: {len(candles)} rows to {out_path}.")
            else:
                print(f"No data for {symbol}. Empty file saved: {out_path}")

if __name__ == "__main__":
    main()
