"""Scheduler for periodic data refresh."""

import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler

from db.schema import init_db, get_write_connection
from etl.transform import run_etl
from scrapers.ckan_gov_uk import CkanGovUkScraper
from scrapers.ons_api import ONSScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
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
        self.consecutive_failures: dict[str, int] = {}
        self.db_path = DB_PATH

    def log_ingest_run(self, run_id: str, source: str, started_at: datetime,
                       inserted: int, updated: int, errors: int) -> None:
        try:
            conn = get_write_connection(self.db_path)
            conn.execute("""
                INSERT INTO ingest_runs (run_id, source, started_at, finished_at, inserted, updated, errors)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [run_id, source, started_at.isoformat(),
                  datetime.now(timezone.utc).isoformat(),
                  inserted, updated, errors])
            conn.close()
        except Exception as e:
            logger.error(f"Failed to log ingest run: {e}")

    def _run_source(self, name: str, scrape_fn) -> dict:
        """Run a single source scraper + ETL."""
        run_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc)
        errors = 0

        if self.consecutive_failures.get(name, 0) >= CONSECUTIVE_FAILURES_THRESHOLD:
            logger.warning(f"Skipping {name}: too many consecutive failures")
            return {"source": name, "skipped": True}

        try:
            count = scrape_fn()
            if not count:
                self.consecutive_failures[name] = self.consecutive_failures.get(name, 0) + 1
                return {"source": name, "scraped": 0}
            self.consecutive_failures[name] = 0
            logger.info(f"{name} scraped {count} records")
        except Exception as e:
            logger.error(f"{name} scrape failed: {e}")
            errors = 1
            self.consecutive_failures[name] = self.consecutive_failures.get(name, 0) + 1

        self.log_ingest_run(run_id, name, started_at, count if not errors else 0, 0, errors)
        return {"source": name, "scraped": count if not errors else 0, "errors": errors}

    def refresh_ckan(self) -> dict:
        def scrape():
            s = CkanGovUkScraper(output_dir=RAW_DIR)
            return s.run(max_per_theme=1000)
        return self._run_source("ckan_gov_uk", scrape)

    def refresh_ons(self) -> dict:
        def scrape():
            s = ONSScraper(output_dir=RAW_DIR)
            return s.run(max_datasets=500)
        return self._run_source("ons_api", scrape)

    def run_all(self) -> list[dict]:
        logger.info("Starting data refresh cycle")
        results = [self.refresh_ckan(), self.refresh_ons()]

        # ETL: load scraped JSONL into DuckDB
        try:
            run_etl()
        except Exception as e:
            logger.error(f"ETL failed: {e}")

        # Run analysis pipeline
        try:
            from analysis.patterns import run_analysis
            run_analysis()
        except Exception as e:
            logger.error(f"Analysis failed: {e}")

        logger.info(f"Refresh cycle complete: {results}")
        return results

    def start(self) -> None:
        self.scheduler.add_job(
            self.run_all, "interval", hours=self.interval_hours,
            id="data_refresh", next_run_time=datetime.now(timezone.utc),
        )
        logger.info(f"Scheduler started, refresh every {self.interval_hours} hours")
        self.scheduler.start()

    def stop(self) -> None:
        self.scheduler.shutdown()
        logger.info("Scheduler stopped")


def main():
    init_db(DB_PATH)
    scheduler = DataRefreshScheduler(interval_hours=6)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.stop()


if __name__ == "__main__":
    main()
