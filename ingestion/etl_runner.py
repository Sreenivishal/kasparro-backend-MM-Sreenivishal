import sys
import os

# --- CRITICAL FIX: Add project root to system path ---
# This ensures Python can find 'services' even if running from inside 'ingestion'
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import requests
import csv
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential, before_log

# Import storage functions and checkpoint updater
from services.db import store_raw, store_normalized
from services.checkpoints import update_checkpoint

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"
COINPAPRIKA_URL = "https://api.coinpaprika.com/v1/tickers"
CSV_PATH = "/app/data/manual_rates.csv"

# --- P2.3: RETRY LOGIC DECORATOR ---
# Retries 5 times with exponential backoff for network calls
robust_request = retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    before=before_log(logger, logging.INFO)
)

# --- 1. COINGECKO SOURCE ---
@robust_request
def fetch_coingecko():
    headers = {"accept": "application/json"}
    if COINGECKO_API_KEY and "xxxxx" not in COINGECKO_API_KEY:
         headers["x-cg-demo-api-key"] = COINGECKO_API_KEY

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 50,
        "page": 1,
        "sparkline": "false",
    }

    r = requests.get(COINGECKO_URL, headers=headers, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def normalize_coingecko(item):
    return {
        "asset_id": item["id"],
        "symbol": item["symbol"],
        "name": item["name"],
        "price_usd": item["current_price"],
        "market_cap": item["market_cap"],
        "volume_24h": item["total_volume"],
    }

# --- 2. COINPAPRIKA SOURCE ---
@robust_request
def fetch_coinpaprika():
    # Fetch top 50 only
    r = requests.get(COINPAPRIKA_URL, timeout=10)
    r.raise_for_status()
    data = r.json()
    return data[:50] 

def normalize_coinpaprika(item):
    usd_data = item.get("quotes", {}).get("USD", {})
    return {
        "asset_id": item["id"],
        "symbol": item["symbol"],
        "name": item["name"],
        "price_usd": usd_data.get("price"),
        "market_cap": usd_data.get("market_cap"),
        "volume_24h": usd_data.get("volume_24h"),
    }

# --- 3. CSV SOURCE ---
def fetch_csv_data():
    if not os.path.exists(CSV_PATH):
        logger.warning(f"CSV file not found at {CSV_PATH}. Skipping.")
        return []
    
    records = []
    try:
        with open(CSV_PATH, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                records.append(dict(row))
        return records
    except Exception as e:
        logger.error(f"Failed to read CSV: {e}")
        return []

def normalize_csv(item):
    # Safe float conversion
    def safe_float(val):
        try:
            return float(val)
        except (ValueError, TypeError):
            return 0.0

    return {
        "asset_id": item.get("id"),
        "symbol": item.get("symbol"),
        "name": item.get("name"),
        "price_usd": safe_float(item.get("price")),
        "market_cap": safe_float(item.get("cap")),
        "volume_24h": safe_float(item.get("vol")),
    }

# --- GENERIC RUNNER ---
def run_source(source, fetch_fn, norm_fn, raw_table):
    logger.info(f"Starting source: {source}")

    try:
        data = fetch_fn()
    except Exception as e:
        logger.error(f"{source} failed after max retries: {e}")
        return

    if not data:
        logger.warning(f"{source}: No data fetched")
        return

    logger.info(f"{source}: fetched {len(data)} records")
    store_raw(raw_table, data)

    normalized = []
    for item in data:
        try:
            norm_item = norm_fn(item)
            if norm_item["asset_id"] and norm_item["price_usd"] is not None:
                normalized.append(norm_item)
        except Exception as e:
            logger.error(f"{source} validation failed: {e}")

    logger.info(f"{source}: normalized {len(normalized)} records")

    if normalized:
        store_normalized(source, normalized)
        update_checkpoint(source)
        logger.info(f"{source}: complete")

if __name__ == "__main__":
    run_source("coingecko", fetch_coingecko, normalize_coingecko, "raw_coingecko")
    run_source("coinpaprika", fetch_coinpaprika, normalize_coinpaprika, "raw_coinpaprika")
    run_source("manual_csv", fetch_csv_data, normalize_csv, "raw_csv_assets")