"""Police UK scraper."""

import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# 10 cities to scrape
CITIES = [
    {"name": "London", "lat": 51.5074, "lng": -0.1278},
    {"name": "Manchester", "lat": 53.4808, "lng": -2.2426},
    {"name": "Birmingham", "lat": 52.4862, "lng": -1.8904},
    {"name": "Leeds", "lat": 53.8008, "lng": -1.5491},
    {"name": "Glasgow", "lat": 55.8642, "lng": -4.2518},
    {"name": "Liverpool", "lat": 53.4084, "lng": -2.9916},
    {"name": "Bristol", "lat": 51.4545, "lng": -2.5879},
    {"name": "Sheffield", "lat": 53.3811, "lng": -1.4701},
    {"name": "Edinburgh", "lat": 55.9533, "lng": -3.1883},
    {"name": "Cardiff", "lat": 51.4816, "lng": -3.1791},
]


class PoliceUkScraper(BaseScraper):
    """Scrape crime data from Police UK API."""

    BASE_URL = "https://data.police.uk/api"

    def __init__(self, output_dir: Path = Path("raw")):
        super().__init__()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.seen_ids: set[str] = set()

    def _derive_record_id(self, crime: dict, city: str, date: str) -> str:
        """Derive deterministic record ID from crime persistent_id or hash."""
        # Use persistent_id if available
        if crime.get("persistent_id"):
            return f"crime-{crime['persistent_id']}"
        
        # Fall back to hash of key fields
        key = f"{city}:{date}:{crime.get('category', '')}:{crime.get('location', {}).get('street', {}).get('id', '')}"
        return f"crime-{hashlib.sha256(key.encode()).hexdigest()[:16]}"

    def fetch_crimes(self, lat: float, lng: float, date: str) -> list[dict]:
        """Fetch crimes for a location and month."""
        url = f"{self.BASE_URL}/crimes-street/all-crime"
        params = {"lat": lat, "lng": lng, "date": date}
        
        # Add 2s delay to respect rate limits
        time.sleep(2)
        
        data = self.fetch_with_retry(url, params=params)
        
        if not data:
            return []
        
        # Add city and date to each crime
        for crime in data:
            crime["city"] = CITIES[0]["name"]  # Will be set properly in loop
            crime["fetch_date"] = date
        
        return data

    def fetch_all_cities(self, months_back: int = 3) -> list[dict]:
        """Fetch crimes for all cities over multiple months."""
        all_crimes = []
        
        # Generate last N months
        from datetime import timedelta
        today = datetime.now(timezone.utc)
        
        for city in CITIES:
            logger.info(f"Fetching crimes for {city['name']}...")
            
            for i in range(months_back):
                # Calculate month (YYYY-MM)
                month_date = today - timedelta(days=30 * i)
                date_str = month_date.strftime("%Y-%m")
                
                crimes = self.fetch_crimes(city["lat"], city["lng"], date_str)
                
                # Add city name and derive IDs
                for crime in crimes:
                    crime["city"] = city["name"]
                    crime["fetch_date"] = date_str
                    
                    record_id = self._derive_record_id(crime, city["name"], date_str)
                    crime["record_id"] = record_id
                    
                    if record_id not in self.seen_ids:
                        self.seen_ids.add(record_id)
                        all_crimes.append(crime)
                
                logger.info(f"  {city['name']} {date_str}: {len(crimes)} crimes")
        
        return all_crimes

    def transform_record(self, crime: dict) -> dict[str, Any]:
        """Transform police.uk crime to canonical schema."""
        location = crime.get("location", {})
        street = location.get("street", {})
        
        return {
            "record_id": crime.get("record_id", ""),
            "title": crime.get("category", "unknown").replace("-", " ").title(),
            "description": f"{crime.get('category', 'unknown')} in {crime.get('city', 'unknown')}",
            "topic": "crime",
            "keywords": [crime.get("category", ""), crime.get("city", "")],
            "organization": "Police UK",
            "url": f"https://data.police.uk/article/{crime.get('id', '')}",
            "license": "Open Government Licence",
            "source": "police_uk",
            "ingested_at": self._timestamp()
        }

    def run(self, months_back: int = 3):
        """Run the scraper."""
        logger.info(f"Starting police.uk scraper, months_back={months_back}")
        
        self.seen_ids = set()
        crimes = self.fetch_all_cities(months_back=months_back)
        
        if not crimes:
            logger.warning("No crimes fetched")
            return []
        
        # Write to JSONL
        output_file = self.output_dir / "police_uk.jsonl"
        written = 0
        with open(output_file, "a") as f:
            for crime in crimes:
                record = self.transform_record(crime)
                f.write(json.dumps(record) + "\n")
                written += 1
        
        logger.info(f"Wrote {written} records to {output_file}")
        return crimes


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scraper = PoliceUkScraper()
    scraper.run(months_back=1)  # Start with 1 month for testing
