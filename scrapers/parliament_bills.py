"""Parliament Bills scraper."""

import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class ParliamentBillsScraper(BaseScraper):
    """Scrape UK Parliament bills."""

    BASE_URL = "https://bills-api.parliament.uk/api/v1/Bills"

    def __init__(self, output_dir: Path = Path("raw")):
        super().__init__()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.seen_ids: set[str] = set()

    def _derive_record_id(self, bill_id: int) -> str:
        """Derive deterministic record ID."""
        return f"bill-{bill_id}"

    def fetch_bills(self, skip: int = 0, take: int = 20) -> list[dict]:
        """Fetch bills from Parliament API."""
        url = self.BASE_URL
        params = {
            "CurrentHouse": "All",
            "SortBy": "DateUpdatedDescending",
            "skip": skip,
            "take": take
        }
        
        data = self.fetch_with_retry(url, params=params)
        
        if not data or "items" not in data:
            return []
        
        return data.get("items", [])

    def fetch_all_bills(self, max_bills: int = 200) -> list[dict]:
        """Fetch bills from Parliament API."""
        all_bills = []
        skip = 0
        take = 20
        
        logger.info(f"Fetching up to {max_bills} bills...")
        
        while len(all_bills) < max_bills:
            bills = self.fetch_bills(skip=skip, take=take)
            
            if not bills:
                break
            
            all_bills.extend(bills)
            logger.info(f"  Fetched {len(bills)} bills (total: {len(all_bills)})")
            
            if len(bills) < take:
                break
            
            skip += take
            time.sleep(1)  # Rate limit
        
        return all_bills[:max_bills]

    def transform_record(self, bill: dict) -> dict[str, Any]:
        """Transform Parliament bill to canonical schema."""
        title = bill.get("title", "Unknown Bill")
        bill_type = bill.get("billType", {}).get("name", "Unknown") if bill.get("billType") else "Unknown"
        
        return {
            "record_id": self._derive_record_id(bill.get("billId", 0)),
            "title": title,
            "description": f"{bill_type} - {bill.get('description', '')[:200]}",
            "topic": "parliament",
            "keywords": [bill_type, bill.get("currentHouse", "")],
            "organization": "UK Parliament",
            "url": f"https://bills.parliament.uk/bills/{bill.get('billId', '')}",
            "license": "Open Parliament Licence",
            "source": "parliament_bills",
            "ingested_at": self._timestamp()
        }

    def run(self, max_bills: int = 200):
        """Run the scraper."""
        logger.info(f"Starting Parliament bills scraper, max={max_bills}")
        
        self.seen_ids = set()
        bills = self.fetch_all_bills(max_bills=max_bills)
        
        if not bills:
            logger.warning("No bills fetched")
            return []
        
        # Write to JSONL
        output_file = self.output_dir / "parliament_bills.jsonl"
        written = 0
        with open(output_file, "a") as f:
            for bill in bills:
                record_id = self._derive_record_id(bill.get("billId", 0))
                if record_id in self.seen_ids:
                    continue
                self.seen_ids.add(record_id)
                
                record = self.transform_record(bill)
                f.write(json.dumps(record) + "\n")
                written += 1
        
        logger.info(f"Wrote {written} records to {output_file}")
        return bills


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scraper = ParliamentBillsScraper()
    scraper.run(max_bills=50)  # Start with 50 for testing
