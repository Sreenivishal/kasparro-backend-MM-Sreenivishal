import pytest
from fastapi.testclient import TestClient
from api.main import app

# Create a test client using the FastAPI app
client = TestClient(app)

def test_health_endpoint():
    """
    Verifies that the /health endpoint returns status 200
    and correctly reports database connection status.
    """
    response = client.get("/health")
    assert response.status_code == 200
    
    data = response.json()
    # Check for required keys defined in api/routes.py
    assert "db_connected" in data
    assert "coingecko_last_run" in data
    assert "coinpaprika_last_run" in data
    
    # db_connected should be a boolean
    assert isinstance(data["db_connected"], bool)

def test_data_endpoint_structure():
    """
    Verifies that /data returns the correct JSON structure:
    request_id, count, and a list of data items.
    """
    response = client.get("/data?limit=10")
    assert response.status_code == 200
    
    data = response.json()
    assert "request_id" in data
    assert "count" in data
    assert "data" in data
    
    # Check if data is actually a list
    assert isinstance(data["data"], list)

def test_data_pagination():
    """
    Verifies that pagination parameters (limit) work.
    """
    # Request only 5 records
    limit = 5
    response = client.get(f"/data?limit={limit}")
    assert response.status_code == 200
    
    data = response.json()
    # The count should be less than or equal to the limit
    assert len(data["data"]) <= limit

def test_stats_endpoint():
    """
    Verifies that /stats returns a list of source checkpoints.
    """
    response = client.get("/stats")
    assert response.status_code == 200
    
    data = response.json()
    assert isinstance(data, list)
    
    # If ETL has run, we expect at least one source (e.g., coingecko)
    if len(data) > 0:
        first_item = data[0]
        assert "source" in first_item
        assert "last_run" in first_item

def test_metrics_endpoint():
    """
    P2 Requirement: Verifies that the Prometheus /metrics endpoint is active.
    """
    response = client.get("/metrics")
    assert response.status_code == 200
    
    # Prometheus metrics return plain text, not JSON
    assert "text/plain" in response.headers["content-type"]
    # Check for standard Prometheus comments
    assert "# HELP" in response.text

def test_404_on_invalid_route():
    """
    Verifies that the API handles unknown routes correctly.
    """
    response = client.get("/this-route-does-not-exist")
    assert response.status_code == 404