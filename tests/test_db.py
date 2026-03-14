"""Tests for DuckDB storage."""

import json
import uuid
from datetime import datetime, timezone

import duckdb
import pytest
from db.schema import SCHEMA_SQL, init_db, init_fts


@pytest.fixture
def db():
    """Create in-memory database with schema."""
    con = duckdb.connect(":memory:")
    con.execute(SCHEMA_SQL)
    yield con
    con.close()


def test_schema_creates_tables(db):
    """Test that schema creates required tables."""
    tables = db.execute("""
        SELECT name FROM sqlite_master WHERE type='table'
    """).fetchall()
    
    table_names = [t[0] for t in tables]
    assert "records" in table_names
    assert "ingest_runs" in table_names


def test_insert_and_retrieve(db):
    """Test inserting and retrieving records."""
    record = {
        "id": "test-001",
        "title": "Test Dataset",
        "description": "A test dataset",
        "topic": "health",
        "keywords": json.dumps(["health", "test"]),
        "organization": "Test Org",
        "url": "https://example.com",
        "license": "OGL",
        "source": "ons_api",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "quality_score": 0.8
    }
    
    db.execute("""
        INSERT INTO records 
        (id, title, description, topic, keywords, organization, url, license, source, ingested_at, quality_score)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, list(record.values()))
    
    result = db.execute("SELECT * FROM records WHERE id = 'test-001'").fetchone()
    
    assert result is not None
    assert result[1] == "Test Dataset"  # title


def test_upsert_is_idempotent(db):
    """Test that upsert replaces existing record."""
    record = {
        "id": "test-001",
        "title": "Original Title",
        "description": "Original desc",
        "topic": "health",
        "keywords": "[]",
        "organization": "Org",
        "url": "https://example.com",
        "license": "OGL",
        "source": "ons_api",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "quality_score": 0.5
    }
    
    db.execute("""
        INSERT INTO records 
        (id, title, description, topic, keywords, organization, url, license, source, ingested_at, quality_score)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, list(record.values()))
    
    # Upsert
    record["title"] = "Updated Title"
    record["quality_score"] = 0.9
    
    db.execute("""
        INSERT OR REPLACE INTO records 
        (id, title, description, topic, keywords, organization, url, license, source, ingested_at, quality_score)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, list(record.values()))
    
    # Should have only one record
    count = db.execute("SELECT COUNT(*) FROM records").fetchone()[0]
    assert count == 1
    
    # Should have updated title
    title = db.execute("SELECT title FROM records WHERE id = 'test-001'").fetchone()[0]
    assert title == "Updated Title"


def test_category_index_filters_correctly(db):
    """Test that topic index enables filtering."""
    records = [
        {"id": "1", "title": "Health Data", "topic": "health"},
        {"id": "2", "title": "Env Data", "topic": "environment"},
        {"id": "3", "title": "More Health", "topic": "health"},
    ]
    
    for r in records:
        db.execute("""
            INSERT INTO records (id, title, topic, source, ingested_at)
            VALUES (?, ?, ?, ?, ?)
        """, [r["id"], r["title"], r["topic"], "test", datetime.now(timezone.utc).isoformat()])
    
    health_records = db.execute("SELECT COUNT(*) FROM records WHERE topic = 'health'").fetchone()[0]
    assert health_records == 2


def test_quality_score_filter(db):
    """Test filtering by quality score."""
    records = [
        {"id": "1", "title": "High Quality", "quality": 0.9},
        {"id": "2", "title": "Low Quality", "quality": 0.3},
    ]
    
    for r in records:
        db.execute("""
            INSERT INTO records (id, title, quality_score, source, ingested_at)
            VALUES (?, ?, ?, ?, ?)
        """, [r["id"], r["title"], r["quality"], "test", datetime.now(timezone.utc).isoformat()])
    
    high_quality = db.execute("SELECT COUNT(*) FROM records WHERE quality_score >= 0.7").fetchone()[0]
    assert high_quality == 1


def test_ingest_run_recorded(db):
    """Test that ingest runs are recorded."""
    run_id = str(uuid.uuid4())
    started = datetime.now(timezone.utc)
    
    db.execute("""
        INSERT INTO ingest_runs (run_id, source, started_at, inserted, updated, errors)
        VALUES (?, ?, ?, ?, ?, ?)
    """, [run_id, "ons_api", started.isoformat(), 10, 0, 0])
    
    result = db.execute("SELECT * FROM ingest_runs WHERE run_id = ?", [run_id]).fetchone()
    
    assert result is not None
    assert result[1] == "ons_api"  # source
    assert result[4] == 10  # inserted


def test_multiple_sources(db):
    """Test storing records from multiple sources."""
    sources = ["ons_api", "data_gov_uk"]
    
    for i, source in enumerate(sources):
        db.execute("""
            INSERT INTO records (id, title, source, ingested_at)
            VALUES (?, ?, ?, ?)
        """, [f"rec-{i}", f"Dataset from {source}", source, datetime.now(timezone.utc).isoformat()])
    
    ons_count = db.execute("SELECT COUNT(*) FROM records WHERE source = 'ons_api'").fetchone()[0]
    uk_count = db.execute("SELECT COUNT(*) FROM records WHERE source = 'data_gov_uk'").fetchone()[0]
    
    assert ons_count == 1
    assert uk_count == 1
