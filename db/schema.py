"""DuckDB schema and connection management."""

import threading
from pathlib import Path

import duckdb

# Schema definition
SCHEMA_SQL = """
-- Main records table
CREATE TABLE IF NOT EXISTS records (
    id VARCHAR PRIMARY KEY,
    title VARCHAR,
    description VARCHAR,
    topic VARCHAR,
    keywords VARCHAR[],  -- Stored as JSON array
    organization VARCHAR,
    url VARCHAR,
    license VARCHAR,
    source VARCHAR NOT NULL,
    ingested_at TIMESTAMP NOT NULL,
    quality_score DOUBLE DEFAULT 0.0,
    -- Full-text search content (computed)
    fts_content VARCHAR
);

-- Ingest runs tracking
CREATE TABLE IF NOT EXISTS ingest_runs (
    run_id VARCHAR PRIMARY KEY,
    source VARCHAR NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    inserted INTEGER DEFAULT 0,
    updated INTEGER DEFAULT 0,
    errors INTEGER DEFAULT 0
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_records_topic ON records(topic);
CREATE INDEX IF NOT EXISTS idx_records_source ON records(source);
CREATE INDEX IF NOT EXISTS idx_records_ingested_at ON records(ingested_at);
"""

# FTS setup (run after schema)
FTS_SQL = """
-- Install FTS extension
INSTALL fts;
LOAD fts;

-- Create FTS index on records (if table exists and not empty)
-- This is done dynamically in init_fts()
"""


def init_db(db_path: str | Path = "data/govdatastory.duckdb") -> duckdb.DuckDBPyConnection:
    """Initialize database with schema."""
    conn = duckdb.connect(str(db_path))
    conn.execute(SCHEMA_SQL)
    
    # Try to setup FTS (may fail if no data yet)
    try:
        conn.execute("INSTALL fts")
        conn.execute("LOAD fts")
    except Exception:
        pass  # FTS setup deferred until data exists
    
    return conn


def init_fts(conn: duckdb.DuckDBPyConnection) -> None:
    """Initialize full-text search index."""
    try:
        # Check if table has data
        count = conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]
        if count > 0:
            # Create FTS content column if not exists
            conn.execute("""
                ALTER TABLE records ADD COLUMN IF NOT EXISTS fts_content VARCHAR
            """)
            
            # Populate FTS content
            conn.execute("""
                UPDATE records 
                SET fts_content = COALESCE(title, '') || ' ' || COALESCE(description, '') || ' ' || COALESCE(topic, '')
            """)
            
            # Create FTS index (DuckDB 0.10+)
            conn.execute("""
                PRAGMA create_fts_index('records', 'id', 'fts_content', 'id')
            """)
    except Exception as e:
        print(f"Warning: FTS setup deferred: {e}")


# Thread-local connection storage
_local = threading.local()


def get_connection(db_path: str | Path = "data/govdatastory.duckdb") -> duckdb.DuckDBPyConnection:
    """Get thread-local read-only connection."""
    if not hasattr(_local, 'connection') or _local.connection is None:
        _local.connection = duckdb.connect(str(db_path), read_only=True)
    return _local.connection


def get_write_connection(db_path: str | Path = "data/govdatastory.duckdb") -> duckdb.DuckDBPyConnection:
    """Get write connection (for ingestion)."""
    return duckdb.connect(str(db_path), read_only=False)


def close_connection() -> None:
    """Close thread-local connection."""
    if hasattr(_local, 'connection') and _local.connection:
        _local.connection.close()
        _local.connection = None
