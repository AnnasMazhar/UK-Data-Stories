"""Test fixtures for GovDataStory."""

import os
import pytest
import duckdb

# Clear API keys for most tests (auth not required)
os.environ.pop("API_KEYS", None)


@pytest.fixture
def db():
    """Create in-memory database with schema."""
    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE IF NOT EXISTS records (
            id VARCHAR PRIMARY KEY,
            title VARCHAR,
            description VARCHAR,
            topic VARCHAR,
            keywords VARCHAR[],
            organization VARCHAR,
            url VARCHAR,
            license VARCHAR,
            source VARCHAR NOT NULL,
            ingested_at TIMESTAMP NOT NULL,
            quality_score DOUBLE DEFAULT 0.0
        );
        
        CREATE TABLE IF NOT EXISTS ingest_runs (
            run_id VARCHAR PRIMARY KEY,
            source VARCHAR NOT NULL,
            started_at TIMESTAMP NOT NULL,
            finished_at TIMESTAMP,
            inserted INTEGER DEFAULT 0,
            updated INTEGER DEFAULT 0,
            errors INTEGER DEFAULT 0
        );
    """)
    
    # Insert test data
    con.execute("""
        INSERT INTO records VALUES 
        ('1', 'Health Data', 'Health dataset', 'health', '[]', 'ONS', 'https://example.com', 'OGL', 'ons_api', '2024-01-01', 0.9),
        ('2', 'Env Data', 'Environment dataset', 'environment', '[]', 'DEFRA', 'https://example2.com', 'OGL', 'data_gov_uk', '2024-01-02', 0.8),
        ('3', 'Transport Data', 'Transport dataset', 'transport', '[]', 'DfT', 'https://example3.com', 'OGL', 'data_gov_uk', '2024-01-03', 0.7)
    """)
    
    yield con
    con.close()


@pytest.fixture
def client(db):
    """Create test client with in-memory DB."""
    from fastapi.testclient import TestClient
    from unittest.mock import patch
    
    with patch("api.main.get_connection", return_value=db):
        from api.main import app
        yield TestClient(app)


@pytest.fixture
def client_with_auth(db):
    """Create test client with API keys enabled."""
    os.environ["API_KEYS"] = "test-key-123,admin-key"
    from fastapi.testclient import TestClient
    from unittest.mock import patch
    
    with patch("api.main.get_connection", return_value=db):
        from api.main import app
        yield TestClient(app)
    
    # Cleanup
    os.environ.pop("API_KEYS", None)
