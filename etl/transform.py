"""ETL transform module for GovDataStory."""

import hashlib
import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def generate_id(name: str, source: str) -> str:
    """Generate deterministic ID from name + source."""
    key = f"{source}:{name.lower().strip()}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def calculate_quality_score(record: dict) -> float:
    """Calculate quality score based on non-null fields."""
    important_fields = ["title", "description", "topic", "source", "ingested_at"]
    
    filled = sum(1 for field in important_fields if record.get(field))
    return round(filled / len(important_fields), 2)


def transform_ons_record(record: dict) -> dict[str, Any]:
    """Transform ONS record to canonical schema."""
    return {
        "id": generate_id(record.get("id", ""), "ons_api"),
        "title": record.get("title"),
        "description": record.get("description"),
        "topic": record.get("topic"),
        "keywords": record.get("keywords", []),
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
        "id": generate_id(record.get("name", ""), "data_gov_uk"),
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


def transform_record(record: dict) -> dict[str, Any]:
    """Transform any record to canonical schema based on source."""
    source = record.get("source", "")
    
    if source == "ons_api":
        return transform_ons_record(record)
    elif source == "data_gov_uk":
        return transform_data_gov_uk_record(record)
    else:
        logger.warning(f"Unknown source: {source}")
        return record


def load_raw_records(input_file: Path) -> list[dict]:
    """Load raw records from JSONL file."""
    records = []
    with open(input_file) as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    return records


def deduplicate(records: list[dict]) -> list[dict]:
    """Deduplicate records by ID, keeping first occurrence."""
    seen = set()
    unique = []
    
    for record in records:
        record_id = record.get("id")
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


def run_etl(raw_dir: Path = Path("raw"), clean_dir: Path = Path("clean")):
    """Run full ETL pipeline."""
    clean_dir = Path(clean_dir)
    clean_dir.mkdir(parents=True, exist_ok=True)
    
    # Process each raw file
    for raw_file in raw_dir.glob("*.jsonl"):
        logger.info(f"Processing {raw_file}")
        
        # Load and transform
        records = load_raw_records(raw_file)
        transformed = [transform_record(r) for r in records]
        
        # Deduplicate
        unique = deduplicate(transformed)
        
        # Write clean output
        output_file = clean_dir / f"{raw_file.stem}_clean.jsonl"
        with open(output_file, "w") as f:
            for record in unique:
                f.write(json.dumps(record) + "\n")
        
        logger.info(f"Wrote {len(unique)} unique records to {output_file}")
    
    return clean_dir


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_etl()
