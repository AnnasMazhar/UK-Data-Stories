"""ETL transform module for GovDataStory."""

import hashlib
import json
import logging
from pathlib import Path
from typing import Any

import duckdb

logger = logging.getLogger(__name__)

# Formats that indicate machine-readable data
MACHINE_READABLE_FORMATS = {"CSV", "JSON", "XML", "XLSX", "XLS", "GEOJSON", "API", "WFS"}


def generate_id(name: str, source: str) -> str:
    """Generate deterministic ID from name + source."""
    key = f"{source}:{name.lower().strip()}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def deduplicate(records: list[dict]) -> list[dict]:
    """Deduplicate records by record_id."""
    seen = set()
    unique = []
    for record in records:
        record_id = record.get("record_id", record.get("id", ""))
        if record_id and record_id not in seen:
            seen.add(record_id)
            unique.append(record)
    return unique


def group_by_topic(records: list[dict]) -> dict[str, list[dict]]:
    """Group records by topic."""
    groups: dict[str, list[dict]] = {}
    for record in records:
        topic = record.get("topic", "other")
        groups.setdefault(topic, []).append(record)
    return groups


def calculate_quality_score(record: dict) -> float:
    """Calculate quality score based on field completeness and data format."""
    fields = ["title", "description", "topic", "source", "ingested_at"]
    filled = sum(1 for f in fields if record.get(f))
    base = filled / len(fields)

    # Bonus for machine-readable formats
    formats = record.get("formats", [])
    if isinstance(formats, list) and set(f.upper() for f in formats) & MACHINE_READABLE_FORMATS:
        base = min(base + 0.1, 1.0)

    # Bonus for having temporal metadata
    if record.get("metadata_modified") or record.get("metadata_created"):
        base = min(base + 0.05, 1.0)

    return round(base, 2)


def transform_ons_record(record: dict) -> dict[str, Any]:
    """Transform ONS record to canonical schema."""
    keywords = record.get("keywords", [])
    if isinstance(keywords, str):
        try:
            keywords = json.loads(keywords)
        except Exception:
            keywords = []

    return {
        "record_id": record.get("id", ""),
        "title": record.get("title"),
        "description": record.get("description"),
        "topic": record.get("topic"),
        "keywords": keywords,
        "organization": "Office for National Statistics",
        "url": record.get("url"),
        "license": "Open Government Licence",
        "source": "ons_api",
        "ingested_at": record.get("ingested_at"),
        "quality_score": calculate_quality_score(record),
    }


def transform_ckan_gov_uk_record(record: dict) -> dict[str, Any]:
    """Transform ckan.publishing.service.gov.uk record to canonical schema."""
    return {
        "record_id": record.get("record_id", ""),
        "title": record.get("title"),
        "description": record.get("description"),
        "topic": record.get("topic"),
        "keywords": record.get("keywords", []),
        "organization": record.get("organization"),
        "url": record.get("url"),
        "license": record.get("license"),
        "source": "ckan_gov_uk",
        "ingested_at": record.get("ingested_at"),
        "quality_score": calculate_quality_score(record),
        "metadata_created": record.get("metadata_created"),
        "metadata_modified": record.get("metadata_modified"),
        "theme": record.get("theme"),
        "num_resources": record.get("num_resources", 0),
    }


def transform_record(record: dict) -> dict[str, Any]:
    """Transform any record to canonical schema based on source."""
    source = record.get("source", "")
    if source == "ons_api":
        return transform_ons_record(record)
    if source == "ckan_gov_uk":
        return transform_ckan_gov_uk_record(record)
    logger.warning(f"Unknown source: {source}")
    return record


def load_to_duckdb(jsonl_path: Path, db_path: str = "data/govdatastory.duckdb"):
    """Load JSONL data to DuckDB with upsert."""
    if not jsonl_path.exists():
        logger.warning(f"File not found: {jsonl_path}")
        return 0

    conn = duckdb.connect(db_path)
    count = 0

    try:
        # Read raw JSONL into a temp table, then transform and upsert
        conn.execute(f"CREATE OR REPLACE TEMP TABLE raw_import AS SELECT * FROM read_json_auto('{jsonl_path}', sample_size=-1, union_by_name=true, auto_detect=true)")

        # Force record_id/id to VARCHAR to avoid type inference issues
        cols = {r[0] for r in conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name='raw_import'").fetchall()}

        # Build column expressions with fallbacks
        id_expr = "CAST(record_id AS VARCHAR)" if "record_id" in cols else ("CAST(id AS VARCHAR)" if "id" in cols else "NULL")
        meta_created = "metadata_created" if "metadata_created" in cols else "NULL"
        meta_modified = "metadata_modified" if "metadata_modified" in cols else "NULL"
        theme = "theme" if "theme" in cols else "NULL"
        num_res = "num_resources" if "num_resources" in cols else "0"
        org = "organization" if "organization" in cols else "NULL"
        kw = "keywords" if "keywords" in cols else ("tags" if "tags" in cols else "NULL")
        lic = "license" if "license" in cols else "NULL"

        conn.execute(f"""
            INSERT OR REPLACE INTO records
            SELECT
                {id_expr} as id,
                title,
                description,
                topic,
                {kw} as keywords,
                {org} as organization,
                url,
                {lic} as license,
                source,
                ingested_at,
                0.8 as quality_score,
                CAST(TRY_CAST({meta_created} AS TIMESTAMP) AS TIMESTAMP) as metadata_created,
                CAST(TRY_CAST({meta_modified} AS TIMESTAMP) AS TIMESTAMP) as metadata_modified,
                {theme} as theme,
                {num_res} as num_resources
            FROM raw_import
            WHERE {id_expr} IS NOT NULL
        """)

        count = conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]
        logger.info(f"Loaded {jsonl_path.name}, total records: {count}")
    except Exception as e:
        logger.error(f"Error loading {jsonl_path}: {e}")
    finally:
        conn.close()

    return count


def run_etl(raw_dir: Path = Path("raw"), db_path: str = "data/govdatastory.duckdb"):
    """Run full ETL pipeline."""
    total_loaded = 0
    for raw_file in sorted(raw_dir.glob("*.jsonl")):
        logger.info(f"Processing {raw_file}")
        count = load_to_duckdb(raw_file, db_path)
        total_loaded = count
    return total_loaded


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_etl()
