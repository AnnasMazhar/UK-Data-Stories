"""Tests for ETL transform module."""

from etl.transform import (
    calculate_quality_score,
    deduplicate,
    generate_id,
    group_by_topic,
    transform_ons_record,
    transform_ckan_gov_uk_record,
    transform_record,
)


def test_generate_id():
    """Test deterministic ID generation."""
    id1 = generate_id("test-dataset", "ons_api")
    id2 = generate_id("test-dataset", "ons_api")
    id3 = generate_id("test-dataset", "ckan_gov_uk")

    assert id1 == id2
    assert id1 != id3


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
        "ingested_at": "2024-06-15T00:00:00Z",
    }
    result = transform_ons_record(record)
    assert result["title"] == "Personal Well-being"
    assert result["organization"] == "Office for National Statistics"
    assert result["license"] == "Open Government Licence"
    assert result["source"] == "ons_api"


def test_transform_ckan_gov_uk_record():
    """Test CKAN record transformation."""
    record = {
        "record_id": "abc-123",
        "title": "Air Quality Data",
        "description": "Air quality measurements",
        "keywords": ["environment", "air"],
        "topic": "environment",
        "organization": "DEFRA",
        "url": "https://example.com",
        "license": "OGL",
        "source": "ckan_gov_uk",
        "ingested_at": "2024-06-15T00:00:00Z",
        "metadata_created": "2020-01-01",
        "metadata_modified": "2024-01-01",
        "theme": "environment",
        "num_resources": 3,
    }
    result = transform_ckan_gov_uk_record(record)
    assert result["title"] == "Air Quality Data"
    assert result["organization"] == "DEFRA"
    assert result["source"] == "ckan_gov_uk"
    assert result["metadata_modified"] == "2024-01-01"
    assert result["num_resources"] == 3


def test_transform_record_routes_correctly():
    """Test transform_record routes to correct handler."""
    ons = transform_record({"id": "test", "source": "ons_api", "title": "Test"})
    ckan = transform_record({"record_id": "test", "source": "ckan_gov_uk", "title": "Test"})

    assert ons["organization"] == "Office for National Statistics"
    assert ckan["source"] == "ckan_gov_uk"


def test_dedup_removes_exact_duplicates():
    """Test deduplication removes exact duplicates."""
    records = [
        {"id": "abc", "name": "tool1"},
        {"id": "def", "name": "tool2"},
        {"id": "abc", "name": "tool1-duplicate"},
    ]
    result = deduplicate(records)
    assert len(result) == 2


def test_dedup_keeps_distinct_records():
    """Test deduplication keeps distinct records."""
    records = [{"id": f"id{i}"} for i in range(3)]
    assert len(deduplicate(records)) == 3


def test_group_by_topic():
    """Test grouping records by topic."""
    records = [
        {"topic": "health", "title": "H1"},
        {"topic": "health", "title": "H2"},
        {"topic": "environment", "title": "E1"},
    ]
    groups = group_by_topic(records)
    assert len(groups["health"]) == 2
    assert len(groups["environment"]) == 1


def test_quality_score_full_record():
    """Test quality score for complete record."""
    record = {
        "title": "Test", "description": "Desc", "topic": "health",
        "source": "ons_api", "ingested_at": "2024-06-15T00:00:00Z",
    }
    assert calculate_quality_score(record) == 1.0


def test_quality_score_with_formats():
    """Test quality score bonus for machine-readable formats."""
    record = {
        "title": "Test", "description": "Desc", "topic": "health",
        "source": "ckan_gov_uk", "ingested_at": "2024-06-15T00:00:00Z",
        "formats": ["CSV", "PDF"],
    }
    score = calculate_quality_score(record)
    assert score > 1.0 - 0.01  # base 1.0 + 0.1 capped at 1.0


def test_quality_score_with_temporal_metadata():
    """Test quality score bonus for temporal metadata."""
    record = {
        "title": "Test", "description": None, "topic": "health",
        "source": "ckan_gov_uk", "ingested_at": "2024-06-15T00:00:00Z",
        "metadata_modified": "2024-01-01",
    }
    score = calculate_quality_score(record)
    assert score > 0.8  # 4/5 + 0.05


def test_quality_score_sparse_record():
    """Test quality score for sparse record."""
    record = {"title": "Test", "source": "ons_api"}
    assert calculate_quality_score(record) == 0.4


def test_quality_score_empty_record():
    """Test quality score for empty record."""
    assert calculate_quality_score({}) == 0.0
