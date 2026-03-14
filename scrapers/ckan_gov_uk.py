"""CKAN Publishing Service scraper (ckan.publishing.service.gov.uk)."""

import json
import logging
from pathlib import Path

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

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
    "mapping",
]

# Key government publishers to scrape by org (catches datasets missed by theme search)
PRIORITY_ORGS = [
    "office-for-national-statistics",
    "nhs-digital",
    "home-office",
    "department-for-transport",
    "department-for-education",
    "department-for-environment-food-and-rural-affairs",
    "environment-agency",
    "ministry-of-justice",
    "department-for-communities-and-local-government",
    "hm-revenue-and-customs",
    "greater-london-authority",
    "ministry-of-defence",
    "natural-england",
]

# Formats that indicate machine-readable data (higher quality)
MACHINE_READABLE_FORMATS = {"CSV", "JSON", "XML", "XLSX", "XLS", "GEOJSON", "API", "WFS"}


class CkanGovUkScraper(BaseScraper):
    """Scrape datasets from ckan.publishing.service.gov.uk."""

    BASE_URL = "https://ckan.publishing.service.gov.uk/api/action"

    def __init__(self, output_dir: Path = Path("raw")):
        super().__init__()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.seen_ids: set[str] = set()

    def _paginated_search(self, fq: str, limit: int) -> list[dict]:
        """Paginated package_search with a filter query."""
        url = f"{self.BASE_URL}/package_search"
        results = []
        start = 0
        page_size = 100

        while len(results) < limit:
            data = self.fetch_with_retry(url, params={
                "fq": fq,
                "rows": min(page_size, limit - len(results)),
                "start": start,
                "sort": "metadata_modified desc",
            })
            if not data or not data.get("result"):
                break
            batch = data["result"].get("results", [])
            if not batch:
                break
            results.extend(batch)
            if len(batch) < page_size:
                break
            start += page_size

        return results[:limit]

    def search_by_theme(self, theme: str, limit: int = 1000) -> list[dict]:
        """Search packages by theme-primary facet."""
        logger.info(f"Searching theme '{theme}' (limit={limit})")
        return self._paginated_search(f'theme-primary:"{theme}"', limit)

    def search_by_org(self, org_name: str, limit: int = 500) -> list[dict]:
        """Search packages by organization."""
        logger.info(f"Searching org '{org_name}' (limit={limit})")
        return self._paginated_search(f"organization:{org_name}", limit)

    def _extract_extras(self, pkg: dict) -> dict:
        """Extract useful fields from CKAN extras."""
        extras = {}
        for e in pkg.get("extras", []):
            if e["key"] in ("dcat_issued", "dcat_modified", "harvest_source_title"):
                extras[e["key"]] = e["value"]
        return extras

    def _extract_formats(self, resources: list[dict]) -> list[str]:
        """Extract unique resource formats."""
        return list({r.get("format", "").upper() for r in resources if r.get("format")})

    def _has_machine_readable(self, formats: list[str]) -> bool:
        return bool(set(formats) & MACHINE_READABLE_FORMATS)

    def transform_record(self, pkg: dict) -> dict:
        """Transform CKAN package to canonical schema."""
        org = pkg.get("organization") or {}
        tags = [t["name"] for t in pkg.get("tags", []) if isinstance(t, dict)]
        theme = pkg.get("theme-primary", "")
        topic = THEME_MAP.get(theme, "other")
        resources = pkg.get("resources", [])
        formats = self._extract_formats(resources)
        extras = self._extract_extras(pkg)
        first_url = resources[0].get("url") if resources else None

        # Prefer dcat dates over CKAN metadata dates
        metadata_created = extras.get("dcat_issued") or pkg.get("metadata_created")
        metadata_modified = extras.get("dcat_modified") or pkg.get("metadata_modified")

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
            "has_machine_readable": self._has_machine_readable(formats),
            "metadata_created": metadata_created,
            "metadata_modified": metadata_modified,
            "harvest_source": extras.get("harvest_source_title"),
            "ingested_at": self._timestamp(),
        }

    def run(self, max_per_theme: int = 1000, max_per_org: int = 500, themes: list[str] | None = None):
        """Run the scraper: themes first, then fill gaps with org-based scraping."""
        themes = themes or PRIORITY_THEMES
        logger.info(f"Starting CKAN scraper: {len(themes)} themes (max={max_per_theme}), "
                     f"{len(PRIORITY_ORGS)} orgs (max={max_per_org})")

        self.seen_ids = set()
        all_records = []

        # Phase 1: Theme-based scraping
        for theme in themes:
            packages = self.search_by_theme(theme, limit=max_per_theme)
            new = 0
            for pkg in packages:
                pid = pkg.get("id", "")
                if pid and pid not in self.seen_ids:
                    self.seen_ids.add(pid)
                    all_records.append(self.transform_record(pkg))
                    new += 1
            logger.info(f"Theme '{theme}': {len(packages)} found, {new} new (total: {len(all_records)})")

        # Phase 2: Org-based scraping (fills gaps)
        for org_name in PRIORITY_ORGS:
            packages = self.search_by_org(org_name, limit=max_per_org)
            new = 0
            for pkg in packages:
                pid = pkg.get("id", "")
                if pid and pid not in self.seen_ids:
                    self.seen_ids.add(pid)
                    all_records.append(self.transform_record(pkg))
                    new += 1
            if new:
                logger.info(f"Org '{org_name}': {new} new datasets")

        if not all_records:
            logger.warning("No packages fetched")
            return []

        # Write atomically
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
    scraper.run()
