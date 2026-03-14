"""data.gov.uk CKAN API scraper."""

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# 7 topics to scrape
TOPICS = ["crime", "economy", "health", "housing", "transport", "population", "parliament"]


class DataGovUkScraper(BaseScraper):
    """Scrape datasets from data.gov.uk CKAN API."""

    BASE_URL = "https://www.data.gov.uk/api/action"

    def __init__(self, output_dir: Path = Path("raw")):
        super().__init__()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.seen_ids: set[str] = set()

    def _derive_record_id(self, record: dict) -> str:
        """Derive deterministic record ID from CKAN package id."""
        # Use CKAN package id field as-is
        return record.get("id", "")

    def search_by_topic(self, topic: str, limit: int = 100) -> list[dict]:
        """Search packages by topic query."""
        url = f"{self.BASE_URL}/package_search"
        all_packages = []
        
        offset = 0
        while len(all_packages) < limit:
            params = {"q": topic, "rows": min(100, limit - offset), "start": offset}
            data = self.fetch_with_retry(url, params=params)
            
            if not data or not data.get("result"):
                break
            
            results = data.get("result", {}).get("results", [])
            if not results:
                break
            
            for pkg in results:
                # Add topic from search query
                pkg["topic"] = topic
            
            all_packages.extend(results)
            logger.info(f"Topic '{topic}': fetched {len(results)} (total: {len(all_packages)})")
            
            if len(results) < params["rows"]:
                break
            offset += params["rows"]
        
        return all_packages

    def fetch_all_topics(self, max_per_topic: int = 100) -> list[dict]:
        """Fetch datasets for all 7 topics."""
        all_packages = []
        
        for topic in TOPICS:
            packages = self.search_by_topic(topic, limit=max_per_topic)
            # Deduplicate within this run
            for pkg in packages:
                record_id = self._derive_record_id(pkg)
                if record_id and record_id not in self.seen_ids:
                    self.seen_ids.add(record_id)
                    all_packages.append(pkg)
            logger.info(f"Topic '{topic}': {len(packages)} packages, {len(all_packages)} total so far")
        
        return all_packages

    def transform_record(self, package: dict, topic: str) -> dict[str, Any]:
        """Transform data.gov.uk package to canonical schema."""
        org = package.get("organization")
        org_name = org.get("title") if org else None
        
        tags = [t.get("name", t) for t in package.get("tags", [])]
        
        # Get first resource URL if available
        resources = package.get("resources", [])
        resource_url = resources[0].get("url") if resources else None
        
        return {
            "record_id": self._derive_record_id(package),
            "id": package.get("id"),
            "title": package.get("title"),
            "description": package.get("notes"),
            "organization": org_name,
            "tags": tags,
            "url": resource_url,
            "license": package.get("license_title"),
            "source": "data_gov_uk",
            "topic": topic or package.get("topic", "other"),
            "updated_at": package.get("metadata_modified"),
            "ingested_at": self._timestamp()
        }

    def run(self, max_per_topic: int = 100):
        """Run the scraper for all topics."""
        logger.info(f"Starting data.gov.uk scraper, max_per_topic={max_per_topic}")
        
        # Reset seen_ids for fresh run
        self.seen_ids = set()
        
        packages = self.fetch_all_topics(max_per_topic=max_per_topic)
        
        if not packages:
            logger.warning("No packages fetched")
            return []
        
        # Write to JSONL with dedup
        output_file = self.output_dir / "data_gov_uk.jsonl"
        written = 0
        with open(output_file, "a") as f:
            for package in packages:
                topic = package.get("topic", "other")
                record = self.transform_record(package, topic)
                f.write(json.dumps(record) + "\n")
                written += 1
        
        logger.info(f"Wrote {written} records to {output_file}")
        return packages


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scraper = DataGovUkScraper()
    scraper.run(max_per_topic=100)
