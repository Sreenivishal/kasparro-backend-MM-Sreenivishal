import os
import json
import psycopg2
from datetime import datetime
from psycopg2.extras import RealDictCursor, execute_values

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@db:5432/crypto_db")

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def fetch_all(query, params=None):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()
    except Exception as e:
        print(f"DB Error in fetch_all: {e}")
        return []
    finally:
        conn.close()

def fetch_one(query, params=None):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchone()
    except Exception as e:
        print(f"DB Error in fetch_one: {e}")
        return None
    finally:
        conn.close()

def execute(query, params=None):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
    except Exception as e:
        print(f"DB Error in execute: {e}")
    finally:
        conn.close()

def ping():
    try:
        conn = get_conn()
        conn.close()
        return True
    except Exception:
        return False

# --- STORAGE FUNCTIONS ---

def store_raw(table_name: str, data: list):
    """Inserts raw JSON data into the specified raw table."""
    conn = get_conn()
    try:
        query = f"INSERT INTO {table_name} (fetched_at, payload) VALUES (NOW(), %s)"
        with conn:
            with conn.cursor() as cur:
                cur.execute(query, (json.dumps(data),))
    except Exception as e:
        print(f"DB Error in store_raw: {e}")
    finally:
        conn.close()

def store_normalized(source: str, data: list):
    """Upserts assets and inserts prices efficiently using execute_values."""
    if not data:
        return

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                # 1. Upsert Assets
                asset_query = """
                    INSERT INTO assets (asset_id, symbol, name, source)
                    VALUES %s
                    ON CONFLICT (asset_id) DO NOTHING
                """
                asset_values = [(d['asset_id'], d['symbol'], d['name'], source) for d in data]
                execute_values(cur, asset_query, asset_values)

                # 2. Insert Prices
                # We calculate timestamp here to ensure consistency across the batch
                now = datetime.now()
                price_query = """
                    INSERT INTO prices (asset_id, price_usd, market_cap, volume_24h, timestamp)
                    VALUES %s
                    ON CONFLICT (asset_id, timestamp) DO NOTHING
                """
                price_values = [
                    (d['asset_id'], d['price_usd'], d['market_cap'], d['volume_24h'], now)
                    for d in data
                ]
                execute_values(cur, price_query, price_values)
                
    except Exception as e:
        print(f"DB Error in store_normalized: {e}")
    finally:
        conn.close()