"""ONS (Office for National Statistics) API scraper."""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class ONSScraper(BaseScraper):
    """Scrape datasets from ONS Beta API."""

    BASE_URL = "https://api.beta.ons.gov.uk/v1"

    def __init__(self, output_dir: Path = Path("raw")):
        super().__init__()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def list_datasets(self, limit: int = 100) -> list[dict]:
        """Fetch list of available datasets."""
        url = f"{self.BASE_URL}/datasets"
        all_datasets = []
        
        # ONS uses pagination via 'limit' and 'offset'
        offset = 0
        while True:
            params = {"limit": min(limit, 100), "offset": offset}
            data = self.fetch_with_retry(url, params=params)
            
            if not data or "items" not in data:
                break
            
            items = data["items"]
            all_datasets.extend(items)
            logger.info(f"Fetched {len(items)} datasets (offset: {offset})")
            
            if len(items) < params["limit"]:
                break
            offset += params["limit"]
        
        return all_datasets

    def get_dataset_info(self, dataset_id: str) -> dict | None:
        """Get detailed info for a specific dataset."""
        url = f"{self.BASE_URL}/datasets/{dataset_id}"
        return self.fetch_with_retry(url)

    def transform_record(self, dataset: dict) -> dict[str, Any]:
        """Transform ONS dataset to canonical schema."""
        return {
            "id": dataset.get("id"),
            "title": dataset.get("title"),
            "description": dataset.get("description"),
            "keywords": dataset.get("keywords", []),
            "last_updated": dataset.get("last_updated"),
            "url": f"https://www.ons.gov.uk/{dataset.get('id', '')}",
            "source": "ons_api",
            "topic": self._infer_topic(dataset.get("keywords", []), dataset.get("title", "")),
            "ingested_at": self._timestamp()
        }

    def _infer_topic(self, keywords: list, title: str) -> str | None:
        """Infer topic from keywords or title."""
        text = " ".join(keywords + [title]).lower()
        
        topics = {
            "population": ["population", "census", "migration", "births", "deaths"],
            "economy": ["gdp", "inflation", "employment", "unemployment", "trade"],
            "health": ["health", "wellbeing", "life expectancy", "disease"],
            "education": ["education", "schools", "university", "qualifications"],
            "housing": ["housing", "property", "homelessness"],
            "environment": ["environment", "climate", "emissions", "energy"],
        }
        
        for topic, terms in topics.items():
            if any(term in text for term in terms):
                return topic
        return "other"

    def run(self, max_datasets: int = 100):
        """Run the scraper and save results."""
        logger.info(f"Starting ONS scraper, max_datasets={max_datasets}")
        
        datasets = self.list_datasets(limit=max_datasets)
        
        if not datasets:
            logger.warning("No datasets fetched")
            return
        
        # Transform and save
        output_file = self.output_dir / "ons_api.jsonl"
        with open(output_file, "a") as f:
            for dataset in datasets:
                record = self.transform_record(dataset)
                f.write(json.dumps(record) + "\n")
        
        logger.info(f"Wrote {len(datasets)} records to {output_file}")
        return datasets


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scraper = ONSScraper()
    scraper.run()
