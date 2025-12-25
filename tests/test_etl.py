import pytest
from fastapi.testclient import TestClient
from api.main import app
from ingestion.etl_runner import normalize_coingecko, normalize_coinpaprika, normalize_csv

# Initialize API Test Client
client = TestClient(app)

# --- 1. UNIT TESTS: ETL TRANSFORMATION LOGIC ---

def test_normalize_coingecko():
    """Test that Coingecko data is correctly mapped to our schema."""
    raw_input = {
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": 50000.0,
        "market_cap": 1000000000,
        "total_volume": 5000000
    }
    
    expected_output = {
        "asset_id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "price_usd": 50000.0,
        "market_cap": 1000000000,
        "volume_24h": 5000000
    }
    
    assert normalize_coingecko(raw_input) == expected_output

def test_normalize_coinpaprika():
    """Test that Coinpaprika's nested structure is flattened correctly."""
    raw_input = {
        "id": "ethereum",
        "symbol": "eth",
        "name": "Ethereum",
        "quotes": {
            "USD": {
                "price": 3000.0,
                "market_cap": 500000000,
                "volume_24h": 2000000
            }
        }
    }
    
    expected_output = {
        "asset_id": "ethereum",
        "symbol": "eth",
        "name": "Ethereum",
        "price_usd": 3000.0,
        "market_cap": 500000000,
        "volume_24h": 2000000
    }
    
    assert normalize_coinpaprika(raw_input) == expected_output

def test_normalize_csv():
    """Test that CSV string data is converted to floats safely."""
    raw_input = {
        "id": "kaspa",
        "symbol": "KAS",
        "name": "Kaspa",
        "price": "0.12",   # Input is string from CSV
        "cap": "300000",
        "vol": "500"
    }
    
    result = normalize_csv(raw_input)
    
    assert result["price_usd"] == 0.12
    assert isinstance(result["price_usd"], float)

# --- 2. INTEGRATION TESTS: API ENDPOINTS ---

def test_health_endpoint():
    """Check if /health returns 200 and DB status."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert "db_connected" in data
    # We expect db_connected to be a boolean (True/False)
    assert isinstance(data["db_connected"], bool)

def test_metrics_endpoint():
    """P2 Requirement: Check if Prometheus metrics are exposed."""
    response = client.get("/metrics")
    assert response.status_code == 200
    # Prometheus metrics usually start with comments or type definitions
    assert "# HELP" in response.text or "# TYPE" in response.text

def test_home_not_found():
    """Ensure root path exists (if you added it) or returns 404/Home."""
    # If you added the Home endpoint I suggested earlier, this should be 200.
    # If not, it will be 404. Let's strictly test that the server responds.
    response = client.get("/")
    assert response.status_code in [200, 404]