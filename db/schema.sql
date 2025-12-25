-- 1. Raw Data Tables
CREATE TABLE IF NOT EXISTS raw_coingecko (
    id SERIAL PRIMARY KEY,
    fetched_at TIMESTAMP NOT NULL,
    payload JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_coinpaprika (
    id SERIAL PRIMARY KEY,
    fetched_at TIMESTAMP NOT NULL,
    payload JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_csv_assets (
    id SERIAL PRIMARY KEY,
    fetched_at TIMESTAMP NOT NULL,
    payload JSONB NOT NULL
);

-- 2. Unified Assets Table
CREATE TABLE IF NOT EXISTS assets (
    asset_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    source TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- 3. Prices Table (Linked to Assets)
CREATE TABLE IF NOT EXISTS prices (
    asset_id TEXT NOT NULL,
    price_usd NUMERIC NOT NULL,
    market_cap NUMERIC,
    volume_24h NUMERIC,
    timestamp TIMESTAMP NOT NULL,
    PRIMARY KEY (asset_id, timestamp),
    CONSTRAINT fk_asset
        FOREIGN KEY (asset_id)
        REFERENCES assets(asset_id)
        ON DELETE CASCADE
);

-- 4. Operational Tables
CREATE TABLE IF NOT EXISTS checkpoints (
    source TEXT PRIMARY KEY,
    last_run TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS etl_runs (
    run_id UUID PRIMARY KEY,
    source TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    status TEXT CHECK (status IN ('success','failure')),
    records_processed INTEGER,
    error_message TEXT
);

-- 5. Indexes for Performance
CREATE INDEX IF NOT EXISTS idx_prices_timestamp ON prices(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_assets_symbol ON assets(symbol);
CREATE INDEX IF NOT EXISTS idx_raw_coingecko_time ON raw_coingecko(fetched_at);
CREATE INDEX IF NOT EXISTS idx_raw_coinpaprika_time ON raw_coinpaprika(fetched_at);