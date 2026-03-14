"""Scheduler for periodic data refresh."""

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler

from db.schema import init_db, get_write_connection
from scrapers.ons_api import ONSScraper
from scrapers.data_gov_uk import DataGovUkScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DB_PATH = "data/govdatastory.duckdb"
RAW_DIR = Path("raw")
CONSECUTIVE_FAILURES_THRESHOLD = 3


class DataRefreshScheduler:
    """Scheduler for refreshing government data."""

    def __init__(self, interval_hours: int = 6):
        self.interval_hours = interval_hours
        self.scheduler = BlockingScheduler()
        self.consecutive_failures = {}  # source -> count
        self.db_path = DB_PATH

    def log_ingest_run(self, run_id: str, source: str, started_at: datetime,
                       inserted: int, updated: int, errors: int) -> None:
        """Log ingest run to database."""
        try:
            conn = get_write_connection(self.db_path)
            conn.execute("""
                INSERT INTO ingest_runs (run_id, source, started_at, finished_at, inserted, updated, errors)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [run_id, source, started_at.isoformat(), 
                  datetime.now(timezone.utc).isoformat(),
                  inserted, updated, errors])
            conn.close()
            logger.info(f"Logged ingest run: {run_id}")
        except Exception as e:
            logger.error(f"Failed to log ingest run: {e}")

    def refresh_ons(self) -> dict:
        """Refresh ONS data."""
        run_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc)
        inserted = 0
        updated = 0
        errors = 0

        try:
            scraper = ONSScraper(output_dir=RAW_DIR)
            datasets = scraper.run(max_datasets=100)
            
            if not datasets:
                logger.warning("ONS scraper returned no data")
                self.consecutive_failures["ons_api"] = \
                    self.consecutive_failures.get("ons_api", 0) + 1
            else:
                # Load to DuckDB
                conn = get_write_connection(self.db_path)
                for dataset in datasets:
                    try:
                        keywords = json.dumps(dataset.get("keywords", []))
                        conn.execute("""
                            INSERT OR REPLACE INTO records 
                            (id, title, description, topic, keywords, organization, url, license, source, ingested_at, quality_score)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, [
                            dataset.get("id"),
                            dataset.get("title"),
                            dataset.get("description"),
                            dataset.get("topic"),
                            keywords,
                            "Office for National Statistics",
                            dataset.get("url"),
                            "Open Government Licence",
                            "ons_api",
                            dataset.get("ingested_at"),
                            0.8
                        ])
                        inserted += 1
                    except Exception as e:
                        logger.error(f"Error inserting ONS record: {e}")
                        errors += 1
                conn.close()
                
                self.consecutive_failures["ons_api"] = 0
                logger.info(f"ONS refresh complete: {inserted} inserted")

        except Exception as e:
            logger.error(f"ONS refresh failed: {e}")
            errors = 1
            self.consecutive_failures["ons_api"] = \
                self.consecutive_failures.get("ons_api", 0) + 1

        self.log_ingest_run(run_id, "ons_api", started_at, inserted, updated, errors)
        return {"source": "ons_api", "inserted": inserted, "errors": errors}

    def refresh_data_gov_uk(self) -> dict:
        """Refresh data.gov.uk data."""
        run_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc)
        inserted = 0
        updated = 0
        errors = 0

        # Check if we should skip due to consecutive failures
        if self.consecutive_failures.get("data_gov_uk", 0) >= CONSECUTIVE_FAILURES_THRESHOLD:
            logger.warning("Skipping data.gov.uk: too many consecutive failures")
            return {"source": "data_gov_uk", "skipped": True, "reason": "consecutive_failures"}

        try:
            scraper = DataGovUkScraper(output_dir=RAW_DIR)
            packages = scraper.run(max_per_topic=100)
            
            if not packages:
                logger.warning("data.gov.uk scraper returned no data")
                self.consecutive_failures["data_gov_uk"] = \
                    self.consecutive_failures.get("data_gov_uk", 0) + 1
            else:
                conn = get_write_connection(self.db_path)
                for pkg in packages:
                    try:
                        tags = json.dumps(pkg.get("tags", []))
                        conn.execute("""
                            INSERT OR REPLACE INTO records 
                            (id, title, description, topic, keywords, organization, url, license, source, ingested_at, quality_score)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, [
                            pkg.get("name"),
                            pkg.get("title"),
                            pkg.get("description"),
                            pkg.get("topic"),
                            tags,
                            pkg.get("organization"),
                            pkg.get("url"),
                            pkg.get("license"),
                            "data_gov_uk",
                            pkg.get("ingested_at"),
                            0.7
                        ])
                        inserted += 1
                    except Exception as e:
                        logger.error(f"Error inserting data.gov.uk record: {e}")
                        errors += 1
                conn.close()
                
                self.consecutive_failures["data_gov_uk"] = 0
                logger.info(f"data.gov.uk refresh complete: {inserted} inserted")

        except Exception as e:
            logger.error(f"data.gov.uk refresh failed: {e}")
            errors = 1
            self.consecutive_failures["data_gov_uk"] = \
                self.consecutive_failures.get("data_gov_uk", 0) + 1

        self.log_ingest_run(run_id, "data_gov_uk", started_at, inserted, updated, errors)
        return {"source": "data_gov_uk", "inserted": inserted, "errors": errors}

    def run_all(self) -> list[dict]:
        """Run all refresh jobs."""
        logger.info("Starting data refresh cycle")
        results = []
        
        results.append(self.refresh_ons())
        results.append(self.refresh_data_gov_uk())
        
        logger.info(f"Data refresh cycle complete: {results}")
        return results

    def start(self) -> None:
        """Start the scheduler."""
        self.scheduler.add_job(
            self.run_all,
            "interval",
            hours=self.interval_hours,
            id="data_refresh",
            next_run_time=datetime.now(timezone.utc)  # Run immediately
        )
        
        logger.info(f"Scheduler started, refresh every {self.interval_hours} hours")
        self.scheduler.start()

    def stop(self) -> None:
        """Stop the scheduler."""
        self.scheduler.shutdown()
        logger.info("Scheduler stopped")


def main():
    """Main entry point."""
    # Initialize DB
    init_db(DB_PATH)
    
    # Start scheduler
    scheduler = DataRefreshScheduler(interval_hours=6)
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.stop()


if __name__ == "__main__":
    main()
