"""ETL transform module for GovDataStory."""

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

logger = logging.getLogger(__name__)


def generate_id(name: str, source: str) -> str:
    """Generate deterministic ID from name + source (legacy)."""
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
        elif record_id:
            logger.debug(f"Duplicate: {record_id}")
    
    return unique


def group_by_topic(records: list[dict]) -> dict[str, list[dict]]:
    """Group records by topic."""
    groups = {}
    for record in records:
        topic = record.get("topic", "other")
        if topic not in groups:
            groups[topic] = []
        groups[topic].append(record)
    return groups


def calculate_quality_score(record: dict) -> float:
    """Calculate quality score based on non-null fields."""
    important_fields = ["title", "description", "topic", "source", "ingested_at"]
    
    filled = sum(1 for field in important_fields if record.get(field))
    return round(filled / len(important_fields), 2)


def transform_ons_record(record: dict) -> dict[str, Any]:
    """Transform ONS record to canonical schema."""
    keywords = record.get("keywords", [])
    if isinstance(keywords, str):
        try:
            keywords = json.loads(keywords)
        except:
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
        "quality_score": calculate_quality_score(record)
    }


def transform_data_gov_uk_record(record: dict) -> dict[str, Any]:
    """Transform data.gov.uk record to canonical schema."""
    return {
        "record_id": record.get("record_id", ""),
        "title": record.get("title"),
        "description": record.get("description"),
        "topic": record.get("topic"),
        "keywords": record.get("tags", []),
        "organization": record.get("organization"),
        "url": record.get("url"),
        "license": record.get("license"),
        "source": "data_gov_uk",
        "ingested_at": record.get("ingested_at"),
        "quality_score": calculate_quality_score(record)
    }


def transform_police_uk_record(record: dict) -> dict[str, Any]:
    """Transform police.uk record to canonical schema."""
    return {
        "record_id": record.get("record_id", ""),
        "title": record.get("title"),
        "description": record.get("description"),
        "topic": "crime",
        "keywords": [],
        "organization": record.get("organization"),
        "url": record.get("url"),
        "license": "Open Government Licence",
        "source": "police_uk",
        "ingested_at": record.get("ingested_at"),
        "quality_score": calculate_quality_score(record)
    }


def transform_parliament_record(record: dict, source_type: str) -> dict[str, Any]:
    """Transform parliament record to canonical schema."""
    return {
        "record_id": record.get("record_id", ""),
        "title": record.get("title"),
        "description": record.get("description"),
        "topic": "parliament",
        "keywords": record.get("keywords", []),
        "organization": "UK Parliament",
        "url": record.get("url"),
        "license": "Open Parliament Licence",
        "source": source_type,
        "ingested_at": record.get("ingested_at"),
        "quality_score": calculate_quality_score(record)
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
    elif source == "data_gov_uk":
        return transform_data_gov_uk_record(record)
    elif source == "police_uk":
        return transform_police_uk_record(record)
    elif source == "parliament_members":
        return transform_parliament_record(record, "parliament_members")
    elif source == "parliament_bills":
        return transform_parliament_record(record, "parliament_bills")
    elif source == "ckan_gov_uk":
        return transform_ckan_gov_uk_record(record)
    else:
        logger.warning(f"Unknown source: {source}")
        return record


def load_to_duckdb(jsonl_path: Path, db_path: str = "data/govdatastory.duckdb"):
    """Load JSONL data to DuckDB with upsert (dedup)."""
    if not jsonl_path.exists():
        logger.warning(f"File not found: {jsonl_path}")
        return 0
    
    conn = duckdb.connect(db_path)
    count = 0
    
    try:
        # Use INSERT OR REPLACE for idempotent upsert
        conn.execute(f"""
            INSERT OR REPLACE INTO records
            SELECT 
                record_id as id,
                title,
                description,
                topic,
                keywords,
                organization,
                url,
                license,
                source,
                ingested_at,
                COALESCE(quality_score, 0.5) as quality_score
            FROM read_json_auto('{jsonl_path}')
        """)
        
        count = conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]
        logger.info(f"Loaded {jsonl_path.name}, total records: {count}")
    except Exception as e:
        logger.error(f"Error loading {jsonl_path}: {e}")
    finally:
        conn.close()
    
    return count


def run_etl(raw_dir: Path = Path("raw"), clean_dir: Path = Path("clean"), db_path: str = "data/govdatastory.duckdb"):
    """Run full ETL pipeline."""
    clean_dir = Path(clean_dir)
    clean_dir.mkdir(parents=True, exist_ok=True)
    
    total_loaded = 0
    
    # Process each raw file
    for raw_file in raw_dir.glob("*.jsonl"):
        logger.info(f"Processing {raw_file}")
        
        # Load to DuckDB (transform + dedup happens in load_to_duckdb)
        count = load_to_duckdb(raw_file, db_path)
        total_loaded = count
    
    return total_loaded


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_etl()
