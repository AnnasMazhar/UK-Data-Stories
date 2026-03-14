"""Tests for ETL transform module."""

import json
from pathlib import Path

import pytest
from etl.transform import (
    calculate_quality_score,
    deduplicate,
    generate_id,
    group_by_topic,
    transform_ons_record,
    transform_data_gov_uk_record,
    transform_record,
)


def test_generate_id():
    """Test deterministic ID generation."""
    id1 = generate_id("test-dataset", "ons_api")
    id2 = generate_id("test-dataset", "ons_api")
    id3 = generate_id("test-dataset", "data_gov_uk")
    
    assert id1 == id2
    assert id1 != id3  # Different sources = different IDs


def test_transform_ons_record():
    """Test ONS record transformation."""
    record = {
        "id": "wellbeing-quarterly",
        "title": "Personal Well-being",
        "description": "Well-being data",
        "keywords": ["well-being"],
        "topic": "health",
        "url": "https://ons.gov.uk/wellbeing",
        "source": "ons_api",
        "ingested_at": "2024-06-15T00:00:00Z"
    }
    
    result = transform_ons_record(record)
    
    assert result["title"] == "Personal Well-being"
    assert result["organization"] == "Office for National Statistics"
    assert result["license"] == "Open Government Licence"
    assert result["source"] == "ons_api"


def test_transform_data_gov_uk_record():
    """Test data.gov.uk record transformation."""
    record = {
        "name": "air-quality",
        "title": "Air Quality Data",
        "description": "Air quality measurements",
        "tags": ["environment", "air"],
        "topic": "environment",
        "organization": "DEFRA",
        "url": "https://data.gov.uk/air-quality",
        "license": "Open Government Licence",
        "source": "data_gov_uk",
        "ingested_at": "2024-06-15T00:00:00Z"
    }
    
    result = transform_data_gov_uk_record(record)
    
    assert result["title"] == "Air Quality Data"
    assert result["organization"] == "DEFRA"
    assert result["source"] == "data_gov_uk"


def test_transform_record_routes_correctly():
    """Test transform_record routes to correct handler."""
    ons_record = {"id": "test", "source": "ons_api", "title": "Test"}
    uk_record = {"name": "test", "source": "data_gov_uk", "title": "Test"}
    
    result_ons = transform_record(ons_record)
    result_uk = transform_record(uk_record)
    
    assert result_ons["organization"] == "Office for National Statistics"
    assert result_uk["organization"] is None


def test_dedup_removes_exact_duplicates():
    """Test deduplication removes exact duplicates."""
    records = [
        {"id": "abc", "name": "tool1"},
        {"id": "def", "name": "tool2"},
        {"id": "abc", "name": "tool1-duplicate"},
    ]
    
    result = deduplicate(records)
    
    assert len(result) == 2
    assert result[0]["id"] == "abc"
    assert result[1]["id"] == "def"


def test_dedup_keeps_distinct_records():
    """Test deduplication keeps distinct records."""
    records = [
        {"id": "abc", "name": "tool1"},
        {"id": "def", "name": "tool2"},
        {"id": "ghi", "name": "tool3"},
    ]
    
    result = deduplicate(records)
    
    assert len(result) == 3


def test_group_by_topic():
    """Test grouping records by topic."""
    records = [
        {"topic": "health", "title": "Health Data 1"},
        {"topic": "health", "title": "Health Data 2"},
        {"topic": "environment", "title": "Env Data"},
    ]
    
    groups = group_by_topic(records)
    
    assert len(groups["health"]) == 2
    assert len(groups["environment"]) == 1


def test_quality_score_full_record():
    """Test quality score for complete record."""
    record = {
        "title": "Test Dataset",
        "description": "Description",
        "topic": "health",
        "source": "ons_api",
        "ingested_at": "2024-06-15T00:00:00Z"
    }
    
    score = calculate_quality_score(record)
    assert score == 1.0


def test_quality_score_sparse_record():
    """Test quality score for sparse record."""
    record = {
        "title": "Test",
        "description": None,
        "topic": None,
        "source": "ons_api",
        "ingested_at": None
    }
    
    score = calculate_quality_score(record)
    assert score == 0.4  # 2/5 fields


def test_quality_score_empty_record():
    """Test quality score for empty record."""
    record = {}
    
    score = calculate_quality_score(record)
    assert score == 0.0
