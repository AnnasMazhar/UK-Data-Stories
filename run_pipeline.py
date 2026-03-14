"""Run the full UK Data Stories pipeline."""

import logging
import uuid
from datetime import datetime, timezone

from scrapers.ons_api import ONSScraper
from scrapers.ckan_gov_uk import CkanGovUkScraper
from etl.transform import run_etl
from analysis.patterns import run_analysis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_pipeline():
    """Run full pipeline: scrape → ETL → analyze → narrate."""
    run_id = str(uuid.uuid4())
    started = datetime.now(timezone.utc)

    logger.info(f"=== Starting pipeline {run_id} ===")

    # Phase 1: Scrape
    logger.info("Phase 1: Scraping...")

    try:
        scraper = CkanGovUkScraper(output_dir="raw")
        scraper.run(max_per_theme=1000, max_per_org=500)
    except Exception as e:
        logger.error(f"CKAN scraper failed: {e}")

    try:
        scraper = ONSScraper(output_dir="raw")
        scraper.run(max_datasets=500)
    except Exception as e:
        logger.error(f"ONS scraper failed: {e}")

    # Phase 2: ETL
    logger.info("Phase 2: ETL...")
    try:
        run_etl()
    except Exception as e:
        logger.error(f"ETL failed: {e}")

    # Phase 3: Analysis + Insights + Stories
    logger.info("Phase 3: Analysis...")
    try:
        run_analysis(run_id)
    except Exception as e:
        logger.error(f"Analysis failed: {e}")

    finished = datetime.now(timezone.utc)
    duration = (finished - started).total_seconds()

    logger.info(f"=== Pipeline {run_id} complete in {duration:.1f}s ===")

    return {
        "run_id": run_id,
        "started": started.isoformat(),
        "finished": finished.isoformat(),
        "duration_seconds": duration,
    }


if __name__ == "__main__":
    result = run_pipeline()
    print(f"\nPipeline result: {result}")
