"""Parliament Members scraper."""

import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class ParliamentMembersScraper(BaseScraper):
    """Scrape UK Parliament members."""

    BASE_URL = "https://members-api.parliament.uk/api/Members"

    def __init__(self, output_dir: Path = Path("raw")):
        super().__init__()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.seen_ids: set[str] = set()

    def _derive_record_id(self, member_id: int) -> str:
        """Derive deterministic record ID."""
        return f"member-{member_id}"

    def fetch_members(self, house: int = 1, skip: int = 0, take: int = 20) -> list[dict]:
        """Fetch members from Parliament API."""
        url = f"{self.BASE_URL}/Search"
        params = {
            "House": house,
            "IsCurrentMember": "true",
            "skip": skip,
            "take": take
        }
        
        data = self.fetch_with_retry(url, params=params)
        
        if not data or "items" not in data:
            return []
        
        return data.get("items", [])

    def fetch_all_members(self) -> list[dict]:
        """Fetch all current members from both houses."""
        all_members = []
        
        for house in [1, 2]:  # 1 = Commons, 2 = Lords
            logger.info(f"Fetching House {house} members...")
            skip = 0
            take = 20
            
            while True:
                members = self.fetch_members(house=house, skip=skip, take=take)
                
                if not members:
                    break
                
                all_members.extend(members)
                logger.info(f"  House {house}: fetched {len(members)} (total: {len(all_members)})")
                
                if len(members) < take:
                    break
                
                skip += take
                time.sleep(1)  # Rate limit
        
        return all_members

    def transform_record(self, member: dict) -> dict[str, Any]:
        """Transform Parliament member to canonical schema."""
        name = member.get("nameDisplayAs", "Unknown")
        party = member.get("latestParty", {}).get("name", "Unknown") if member.get("latestParty") else "Unknown"
        
        return {
            "record_id": self._derive_record_id(member.get("id", 0)),
            "title": name,
            "description": f"{member.get('house', 'Unknown')} - {party}",
            "topic": "parliament",
            "keywords": [party, member.get("house", ""), "MP" if member.get("house") == 1 else "Lord"],
            "organization": "UK Parliament",
            "url": f"https://members.parliament.uk/member/{member.get('id', '')}/profile",
            "license": "Open Parliament Licence",
            "source": "parliament_members",
            "ingested_at": self._timestamp()
        }

    def run(self):
        """Run the scraper."""
        logger.info("Starting Parliament members scraper")
        
        self.seen_ids = set()
        members = self.fetch_all_members()
        
        if not members:
            logger.warning("No members fetched")
            return []
        
        # Write to JSONL
        output_file = self.output_dir / "parliament_members.jsonl"
        written = 0
        with open(output_file, "a") as f:
            for member in members:
                record_id = self._derive_record_id(member.get("id", 0))
                if record_id in self.seen_ids:
                    continue
                self.seen_ids.add(record_id)
                
                record = self.transform_record(member)
                f.write(json.dumps(record) + "\n")
                written += 1
        
        logger.info(f"Wrote {written} records to {output_file}")
        return members


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scraper = ParliamentMembersScraper()
    scraper.run()
