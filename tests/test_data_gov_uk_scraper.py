"""Tests for data.gov.uk scraper."""

import json
from unittest.mock import MagicMock, patch

import pytest
from scrapers.data_gov_uk import DataGovUkScraper


@pytest.fixture
def scraper(tmp_path):
    """Create scraper with temp output directory."""
    return DataGovUkScraper(output_dir=tmp_path)


@pytest.fixture
def mock_search_response():
    """Mock CKAN search response."""
    return {
        "result": {
            "results": [
                {
                    "id": "pkg1",
                    "title": "Air Quality Data",
                    "notes": "Air quality measurements",
                    "organization": {"title": "DEFRA"},
                    "tags": [{"name": "environment"}, {"name": "air"}],
                    "resources": [{"url": "https://example.com/data.csv"}],
                    "license_title": "OGL",
                    "metadata_modified": "2024-01-01"
                }
            ]
        }
    }


def test_transform_record(scraper, mock_search_response):
    """Test transformation of CKAN package to canonical schema."""
    package = mock_search_response["result"]["results"][0]
    result = scraper.transform_record(package, "environment")
    
    assert result["title"] == "Air Quality Data"
    assert result["description"] == "Air quality measurements"
    assert result["organization"] == "DEFRA"
    assert result["source"] == "data_gov_uk"
    assert result["topic"] == "environment"


def test_transform_record_transport_topic(scraper):
    """Test topic inference for transport."""
    package = {
        "id": "pkg2",
        "title": "Bus Routes",
        "notes": "Bus route data",
        "tags": [{"name": "transport"}],
        "organization": {"title": "DfT"},
        "resources": [],
        "license_title": "OGL"
    }
    
    result = scraper.transform_record(package, "transport")
    assert result["topic"] == "transport"


@patch("scrapers.base.httpx.Client.get")
def test_search_by_topic(mock_get, scraper, mock_search_response):
    """Test searching packages by topic."""
    mock_response = MagicMock()
    mock_response.json.return_value = mock_search_response
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response
    
    results = scraper.search_by_topic("environment", limit=10)
    
    assert len(results) >= 1


@patch("scrapers.base.httpx.Client.get")
def test_run_writes_jsonl(mock_get, scraper, mock_search_response):
    """Test that run() writes to JSONL file."""
    mock_response = MagicMock()
    mock_response.json.return_value = mock_search_response
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response
    
    scraper.run(max_per_topic=10)
    
    output_file = scraper.output_dir / "data_gov_uk.jsonl"
    assert output_file.exists()
    
    with open(output_file) as f:
        lines = f.readlines()
        assert len(lines) >= 1
        record = json.loads(lines[0])
        assert "title" in record


def test_derive_record_id(scraper):
    """Test record ID derivation."""
    record = {"id": "test-id-123"}
    record_id = scraper._derive_record_id(record)
    assert record_id == "test-id-123"
