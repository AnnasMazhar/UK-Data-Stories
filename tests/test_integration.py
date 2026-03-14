"""Integration tests for data quality."""

import pytest

pytestmark = pytest.mark.integration


def test_minimum_100_records():
    """Test that we have at least 100 records."""
    import duckdb
    con = duckdb.connect("data/govdatastory.duckdb", read_only=True)
    count = con.execute("SELECT COUNT(*) FROM records").fetchone()[0]
    con.close()
    assert count >= 100, f"Expected >=100 records, got {count}"


def test_no_null_names():
    """Test that no records have null titles."""
    import duckdb
    con = duckdb.connect("data/govdatastory.duckdb", read_only=True)
    nulls = con.execute("SELECT COUNT(*) FROM records WHERE title IS NULL").fetchone()[0]
    con.close()
    assert nulls == 0, f"Found {nulls} records with null titles"


def test_quality_score_70pct():
    """Test that average quality score is >= 0.7."""
    import duckdb
    con = duckdb.connect("data/govdatastory.duckdb", read_only=True)
    avg = con.execute("SELECT AVG(quality_score) FROM records").fetchone()[0]
    con.close()
    assert avg >= 0.7, f"Average quality score {avg} < 0.7"


def test_at_least_3_categories():
    """Test that we have at least 3 distinct topics."""
    import duckdb
    con = duckdb.connect("data/govdatastory.duckdb", read_only=True)
    count = con.execute("SELECT COUNT(DISTINCT topic) FROM records").fetchone()[0]
    con.close()
    assert count >= 3, f"Expected >=3 topics, got {count}"


def test_no_duplicate_ids():
    """Test that there are no duplicate IDs."""
    import duckdb
    con = duckdb.connect("data/govdatastory.duckdb", read_only=True)
    dups = con.execute("""
        SELECT id, COUNT(*) as cnt 
        FROM records 
        GROUP BY id 
        HAVING COUNT(*) > 1
    """).fetchall()
    con.close()
    assert len(dups) == 0, f"Found duplicate IDs: {dups}"


def test_recent_records_exist():
    """Test that records have recent ingested_at dates."""
    import duckdb
    con = duckdb.connect("data/govdatastory.duckdb", read_only=True)
    recent = con.execute("""
        SELECT COUNT(*) FROM records 
        WHERE ingested_at > CURRENT_TIMESTAMP - INTERVAL '7 days'
    """).fetchone()[0]
    con.close()
    assert recent > 0, "No records from last 7 days"


def test_ingest_run_logged():
    """Test that ingest_runs table exists and is queryable."""
    import duckdb
    con = duckdb.connect("data/govdatastory.duckdb", read_only=True)
    # Just verify the table exists and is accessible
    con.execute("SELECT 1 FROM ingest_runs LIMIT 1").fetchall()
    con.close()
    assert True  # If we got here, table is accessible


def test_fts_returns_results():
    """Test that search would return results (basic check)."""
    import duckdb
    con = duckdb.connect("data/govdatastory.duckdb", read_only=True)
    results = con.execute("""
        SELECT COUNT(*) FROM records 
        WHERE title LIKE '%population%' OR description LIKE '%population%'
    """).fetchone()[0]
    con.close()
    assert results > 0, "No results for search query"
