"""Tests for data.gov.uk scraper."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from scrapers.data_gov_uk import DataGovUkScraper


@pytest.fixture
def scraper(tmp_path):
    """Create scraper with temp output directory."""
    return DataGovUkScraper(output_dir=tmp_path)


@pytest.fixture
def mock_ckan_response():
    """Mock CKAN package_list response."""
    return [
        {"id": "pkg1", "name": "air-quality-data", "title": "Air Quality Data"},
        {"id": "pkg2", "name": "bus-routes", "title": "Bus Routes"},
    ]


def test_transform_record(scraper, mock_ckan_response):
    """Test transformation of CKAN package to canonical schema."""
    package = {
        "id": "pkg1",
        "name": "air-quality-data",
        "title": "Air Quality Data",
        "notes": "Air quality measurements from monitoring stations.",
        "tags": [{"name": "environment"}, {"name": "air"}],
        "organization": {"title": "DEFRA"},
        "license_title": "Open Government Licence"
    }
    
    result = scraper.transform_record(package)
    
    assert result["id"] == "pkg1"
    assert result["title"] == "Air Quality Data"
    assert result["description"] == "Air quality measurements from monitoring stations."
    assert result["organization"] == "DEFRA"
    assert result["tags"] == ["environment", "air"]
    assert result["source"] == "data_gov_uk"
    assert result["topic"] == "environment"
    assert result["ingested_at"] is not None


def test_transform_record_transport_topic(scraper):
    """Test topic inference for transport."""
    package = {
        "id": "pkg2",
        "name": "bus-routes",
        "title": "Bus Routes",
        "notes": "Bus route data",
        "tags": [{"name": "transport"}, {"name": "bus"}]
    }
    
    result = scraper.transform_record(package)
    assert result["topic"] == "transport"


def test_infer_topic_from_title(scraper):
    """Test topic inference from title only."""
    package = {
        "id": "pkg3",
        "name": "crime-stats",
        "title": "Crime Statistics",
        "notes": "Crime data",
        "tags": []
    }
    
    result = scraper.transform_record(package)
    assert result["topic"] == "crime"


@patch("scrapers.base.httpx.Client.get")
def test_list_packages(mock_get, scraper, mock_ckan_response):
    """Test fetching packages."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"result": mock_ckan_response}
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response
    
    packages = scraper.list_packages(limit=10)
    
    assert len(packages) == 2
    assert packages[0]["id"] == "pkg1"


@patch("scrapers.base.httpx.Client.get")
def test_search_packages(mock_get, scraper):
    """Test searching packages."""
    search_result = {
        "result": {
            "count": 1,
            "results": [
                {"id": "search1", "name": "found-pkg", "title": "Found Package"}
            ]
        }
    }
    
    mock_response = MagicMock()
    mock_response.json.return_value = search_result
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response
    
    results = scraper.search_packages("air quality")
    
    assert len(results) == 1
    assert results[0]["id"] == "search1"


@patch("scrapers.base.httpx.Client.get")
def test_run_writes_jsonl(mock_get, scraper, mock_ckan_response):
    """Test that run() writes to JSONL file."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"result": mock_ckan_response}
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response
    
    scraper.run(max_packages=10)
    
    output_file = scraper.output_dir / "data_gov_uk.jsonl"
    assert output_file.exists()
    
    with open(output_file) as f:
        lines = f.readlines()
        assert len(lines) == 2
        record = json.loads(lines[0])
        assert record["id"] == "pkg1"
