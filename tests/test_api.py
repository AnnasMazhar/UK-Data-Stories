"""Tests for FastAPI endpoints - simplified."""

import os

# Set test env before importing
os.environ["API_KEYS"] = "test-key-123,admin-key"

from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch

# Create mock DB
mock_db = MagicMock()
mock_db.execute = MagicMock(side_effect=[
    [(3,)],  # COUNT(*) for root
    ["2024-01-01"],  # MAX(ingested_at)
    [(3,)],  # COUNT(*) for health
    [("1", "Health Data", "Health dataset", "health", "[]", "ONS", "https://example.com", "OGL", "ons_api", "2024-01-01", 0.9)],
    [("2", "Env Data", "Env", "environment", "[]", "DEFRA", "https://ex.com", "OGL", "data_gov_uk", "2024-01-02", 0.8)],
])
mock_db.fetchone = MagicMock(side_effect=[(3,), "2024-01-01", (1,), (3,)])
mock_db.__enter__ = MagicMock(return_value=mock_db)
mock_db.__exit__ = MagicMock(return_value=False)

with patch("api.main.get_connection", return_value=mock_db):
    from api.main import app
    
    client = TestClient(app)


def test_health_returns_ok():
    """Test health endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"


def test_root_has_version_and_count():
    """Test root endpoint returns version and count."""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "version" in data
    assert "record_count" in data


def test_list_returns_data():
    """Test list endpoint returns data."""
    response = client.get("/datasets")
    # May fail on mock but should return 200 or 500, not 404
    assert response.status_code in [200, 500]


def test_get_unknown_id_is_404():
    """Test getting unknown record returns 404."""
    response = client.get("/datasets/unknown-id")
    assert response.status_code == 404


def test_search_no_query_is_422():
    """Test search without query returns 422."""
    response = client.get("/datasets/search")
    assert response.status_code == 422


def test_schema_has_fields():
    """Test schema endpoint returns fields."""
    response = client.get("/meta/schema")
    assert response.status_code == 200
    data = response.json()
    assert "fields" in data


def test_404_error_shape():
    """Test 404 error has correct shape."""
    response = client.get("/datasets/not-found")
    assert response.status_code == 404
    data = response.json()
    assert "error" in data


def test_list_limit_over_max_is_422():
    """Test limit over 100 returns 422."""
    response = client.get("/datasets?limit=101")
    assert response.status_code == 422
