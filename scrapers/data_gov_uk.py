"""data.gov.uk CKAN API scraper."""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class DataGovUkScraper(BaseScraper):
    """Scrape datasets from data.gov.uk CKAN API."""

    BASE_URL = "https://data.gov.uk/api/action"

    def __init__(self, output_dir: Path = Path("raw")):
        super().__init__()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def list_packages(self, limit: int = 100) -> list[dict]:
        """Fetch list of available packages/datasets."""
        url = f"{self.BASE_URL}/package_list"
        all_packages = []
        
        # CKAN uses 'limit' and 'offset' for pagination
        offset = 0
        while True:
            params = {"limit": min(limit, 1000), "offset": offset}
            data = self.fetch_with_retry(url, params=params)
            
            if not data or not data.get("result"):
                break
            
            result = data.get("result", {})
            packages = result if isinstance(result, list) else result.get("packages", [])
            
            if not packages:
                break
                
            all_packages.extend(packages)
            logger.info(f"Fetched {len(packages)} packages (offset: {offset})")
            
            if len(packages) < params["limit"]:
                break
            offset += params["limit"]
        
        return all_packages

    def search_packages(self, query: str, limit: int = 100) -> list[dict]:
        """Search packages by query."""
        url = f"{self.BASE_URL}/package_search"
        params = {"q": query, "rows": limit}
        
        data = self.fetch_with_retry(url, params=params)
        
        if not data or not data.get("result"):
            return []
        
        return data.get("result", {}).get("results", [])

    def get_package_details(self, package_name: str) -> dict | None:
        """Get detailed info for a specific package."""
        url = f"{self.BASE_URL}/package_show"
        params = {"id": package_name}
        data = self.fetch_with_retry(url, params=params)
        
        if data and data.get("result"):
            return data["result"]
        return None

    def transform_record(self, package: dict) -> dict[str, Any]:
        """Transform data.gov.uk package to canonical schema."""
        # Extract organization name
        org = package.get("organization")
        org_name = org.get("title") if org else None
        
        # Extract tags
        tags = [t.get("name", t) for t in package.get("tags", [])]
        
        return {
            "id": package.get("id"),
            "name": package.get("name"),
            "title": package.get("title"),
            "description": package.get("notes"),  # CKAN uses 'notes' for description
            "organization": org_name,
            "tags": tags,
            "url": package.get("url"),
            "license": package.get("license_title"),
            "source": "data_gov_uk",
            "topic": self._infer_topic(tags, package.get("title", "")),
            "ingested_at": self._timestamp()
        }

    def _infer_topic(self, tags: list, title: str) -> str | None:
        """Infer topic from tags or title."""
        text = " ".join(tags + [title]).lower()
        
        topics = {
            "environment": ["environment", "climate", "emissions", "air", "water"],
            "transport": ["transport", "traffic", "roads", "rail", "bus"],
            "health": ["health", "nhs", "hospital", "medical"],
            "education": ["education", "schools", "university"],
            "economy": ["economy", "business", "employment", "trade"],
            "demographics": ["population", "census", "demographics"],
            "housing": ["housing", "property", "planning"],
            "crime": ["crime", "police", "justice"],
        }
        
        for topic, terms in topics.items():
            if any(term in text for term in terms):
                return topic
        return "other"

    def run(self, max_packages: int = 200):
        """Run the scraper and save results."""
        logger.info(f"Starting data.gov.uk scraper, max_packages={max_packages}")
        
        packages = self.list_packages(limit=max_packages)
        
        if not packages:
            logger.warning("No packages fetched")
            return
        
        # Transform and save
        output_file = self.output_dir / "data_gov_uk.jsonl"
        with open(output_file, "a") as f:
            for package in packages:
                record = self.transform_record(package)
                f.write(json.dumps(record) + "\n")
        
        logger.info(f"Wrote {len(packages)} records to {output_file}")
        return packages


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scraper = DataGovUkScraper()
    scraper.run()
