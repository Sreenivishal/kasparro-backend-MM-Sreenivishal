import os

# Database
DATABASE_URL = os.getenv("DATABASE_URL")

# API keys
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")
COINPAPRIKA_API_KEY = os.getenv("COINPAPRIKA_API_KEY")

# Safety check (optional but good)
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

if not COINGECKO_API_KEY:
    # CoinGecko allows limited unauthenticated usage,
    # but we log this clearly for evaluation.
    print("WARNING: COINGECKO_API_KEY is not set")


ETL_BATCH_SIZE = 50
