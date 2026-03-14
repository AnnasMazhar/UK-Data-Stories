"""CKAN Publishing Service scraper (ckan.publishing.service.gov.uk)."""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# Map CKAN theme-primary to our topics
THEME_MAP = {
    "health": "health",
    "crime-and-justice": "crime",
    "business-and-economy": "economy",
    "transport": "transport",
    "education": "education",
    "environment": "environment",
    "society": "population",
    "towns-and-cities": "housing",
    "government": "parliament",
    "government-spending": "economy",
    "defence": "other",
    "mapping": "environment",
    "digital-services-performance": "other",
    "government-reference-data": "other",
}

# Themes we care about most for data stories
PRIORITY_THEMES = [
    "health",
    "crime-and-justice",
    "business-and-economy",
    "transport",
    "education",
    "environment",
    "society",
    "towns-and-cities",
    "government",
    "government-spending",
]


class CkanGovUkScraper(BaseScraper):
    """Scrape datasets from ckan.publishing.service.gov.uk."""

    BASE_URL = "https://ckan.publishing.service.gov.uk/api/action"

    def __init__(self, output_dir: Path = Path("raw")):
        super().__init__()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.seen_ids: set[str] = set()

    def search_by_theme(self, theme: str, limit: int = 200) -> list[dict]:
        """Search packages by theme-primary facet."""
        url = f"{self.BASE_URL}/package_search"
        all_packages = []
        start = 0
        page_size = min(100, limit)

        while len(all_packages) < limit:
            data = self.fetch_with_retry(url, params={
                "fq": f'theme-primary:"{theme}"',
                "rows": page_size,
                "start": start,
                "sort": "metadata_modified desc",
            })

            if not data or not data.get("result"):
                break

            total_available = data["result"].get("count", 0)
            results = data["result"].get("results", [])
            if not results:
                break

            all_packages.extend(results)
            logger.info(f"Theme '{theme}': fetched {len(results)} (total: {len(all_packages)})")

            if len(results) < page_size:
                break
            start += page_size

        return all_packages[:limit]

    def transform_record(self, pkg: dict) -> dict:
        """Transform CKAN package to canonical schema."""
        org = pkg.get("organization") or {}
        tags = [t["name"] for t in pkg.get("tags", []) if isinstance(t, dict)]
        theme = pkg.get("theme-primary", "")
        topic = THEME_MAP.get(theme, "other")

        resources = pkg.get("resources", [])
        formats = list({r.get("format", "").upper() for r in resources if r.get("format")})
        first_url = resources[0].get("url") if resources else None

        return {
            "record_id": pkg.get("id", ""),
            "title": pkg.get("title"),
            "description": pkg.get("notes"),
            "topic": topic,
            "keywords": tags,
            "organization": org.get("title"),
            "url": first_url or pkg.get("url"),
            "license": pkg.get("license_title"),
            "source": "ckan_gov_uk",
            "theme": theme,
            "formats": formats,
            "num_resources": len(resources),
            "metadata_created": pkg.get("metadata_created"),
            "metadata_modified": pkg.get("metadata_modified"),
            "ingested_at": self._timestamp(),
        }

    def run(self, max_per_theme: int = 200, themes: list[str] | None = None):
        """Run the scraper for priority themes."""
        themes = themes or PRIORITY_THEMES
        logger.info(f"Starting CKAN gov.uk scraper, {len(themes)} themes, max_per_theme={max_per_theme}")

        self.seen_ids = set()
        all_records = []

        for theme in themes:
            packages = self.search_by_theme(theme, limit=max_per_theme)
            for pkg in packages:
                pid = pkg.get("id", "")
                if pid and pid not in self.seen_ids:
                    self.seen_ids.add(pid)
                    all_records.append(self.transform_record(pkg))

            logger.info(f"Theme '{theme}': {len(packages)} packages, {len(all_records)} total unique")

        if not all_records:
            logger.warning("No packages fetched")
            return []

        output_file = self.output_dir / "ckan_gov_uk.jsonl"
        tmp_file = self.output_dir / "ckan_gov_uk.jsonl.tmp"
        with open(tmp_file, "w") as f:
            for record in all_records:
                f.write(json.dumps(record) + "\n")
        tmp_file.replace(output_file)

        logger.info(f"Wrote {len(all_records)} records to {output_file}")
        return all_records


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scraper = CkanGovUkScraper()
    scraper.run(max_per_theme=200)
