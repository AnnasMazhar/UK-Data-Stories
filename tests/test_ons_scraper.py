"""Tests for ONS scraper."""

import json
from unittest.mock import MagicMock, patch

import pytest
from scrapers.ons_api import ONSScraper


@pytest.fixture
def scraper(tmp_path):
    """Create scraper with temp output directory."""
    return ONSScraper(output_dir=tmp_path)


@pytest.fixture
def mock_ons_response():
    """Mock ONS API response."""
    return {
        "items": [
            {
                "id": "wellbeing-quarterly",
                "title": "Personal Well-being Quarterly",
                "description": "Seasonally and non seasonally-adjusted quarterly estimates of life satisfaction.",
                "keywords": ["well-being", "happiness", "anxiety"],
                "last_updated": "2023-12-13T09:40:24.204Z",
                "links": {}
            },
            {
                "id": "population-estimates",
                "title": "Population Estimates",
                "description": "Mid-year population estimates for the UK.",
                "keywords": ["population", "census"],
                "last_updated": "2024-01-01T00:00:00Z",
                "links": {}
            }
        ]
    }


def test_transform_record(scraper, mock_ons_response):
    """Test transformation of ONS dataset to canonical schema."""
    dataset = mock_ons_response["items"][0]
    result = scraper.transform_record(dataset)
    
    assert result["id"] == "wellbeing-quarterly"
    assert result["title"] == "Personal Well-being Quarterly"
    assert result["keywords"] == ["well-being", "happiness", "anxiety"]
    assert result["source"] == "ons_api"
    assert result["topic"] == "other"  # well-being not in health keywords
    assert result["ingested_at"] is not None


def test_transform_record_economy_topic(scraper):
    """Test topic inference for economy."""
    dataset = {
        "id": "gdp",
        "title": "GDP Estimates",
        "description": "Gross Domestic Product",
        "keywords": ["gdp", "economy"],
        "last_updated": "2024-01-01T00:00:00Z"
    }
    
    result = scraper.transform_record(dataset)
    assert result["topic"] == "economy"


def test_infer_topic_from_title(scraper):
    """Test topic inference from title."""
    dataset = {
        "id": "test",
        "title": "Employment Statistics",
        "description": "Employment data",
        "keywords": [],
        "last_updated": "2024-01-01T00:00:00Z"
    }
    
    result = scraper.transform_record(dataset)
    assert result["topic"] == "economy"


@patch("scrapers.base.httpx.Client.get")
def test_list_datasets(mock_get, scraper, mock_ons_response):
    """Test fetching datasets."""
    mock_response = MagicMock()
    mock_response.json.return_value = mock_ons_response
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response
    
    datasets = scraper.list_datasets(limit=10)
    
    assert len(datasets) == 2
    assert datasets[0]["id"] == "wellbeing-quarterly"


@patch("scrapers.base.httpx.Client.get")
def test_run_writes_jsonl(mock_get, scraper, mock_ons_response):
    """Test that run() writes to JSONL file."""
    mock_response = MagicMock()
    mock_response.json.return_value = mock_ons_response
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response
    
    scraper.run(max_datasets=10)
    
    output_file = scraper.output_dir / "ons_api.jsonl"
    assert output_file.exists()
    
    with open(output_file) as f:
        lines = f.readlines()
        assert len(lines) == 2
        record = json.loads(lines[0])
        assert record["id"] == "wellbeing-quarterly"
