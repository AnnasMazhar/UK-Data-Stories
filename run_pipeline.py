"""Run the full GovDataStory pipeline."""

import logging
import uuid
from datetime import datetime, timezone

from scrapers.ons_api import ONSScraper
from scrapers.data_gov_uk import DataGovUkScraper
from scrapers.police_uk import PoliceUkScraper
from scrapers.parliament_members import ParliamentMembersScraper
from scrapers.parliament_bills import ParliamentBillsScraper

from etl.transform import run_etl
from analysis.patterns import run_analysis
from stories.narrator import generate_stories

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_pipeline():
    """Run full pipeline: scrape -> ETL -> analyze -> narrate."""
    run_id = str(uuid.uuid4())
    started = datetime.now(timezone.utc)
    
    logger.info(f"=== Starting pipeline {run_id} ===")
    
    # Phase 1: Scrape
    logger.info("Phase 1: Scraping...")
    
    # ONS (already done)
    # scraper = ONSScraper(output_dir="raw")
    # scraper.run(max_datasets=100)
    
    # data.gov.uk (already done)
    # scraper = DataGovUkScraper()
    # scraper.run(max_per_topic=100)
    
    # Police UK - just 1 month for speed
    try:
        police = PoliceUkScraper(output_dir="raw")
        police.run(months_back=1)
    except Exception as e:
        logger.error(f"Police UK scraper failed: {e}")
    
    # Parliament Members
    try:
        parliament_members = ParliamentMembersScraper(output_dir="raw")
        parliament_members.run()
    except Exception as e:
        logger.error(f"Parliament members scraper failed: {e}")
    
    # Parliament Bills
    try:
        parliament_bills = ParliamentBillsScraper(output_dir="raw")
        parliament_bills.run(max_bills=50)
    except Exception as e:
        logger.error(f"Parliament bills scraper failed: {e}")
    
    # Phase 2: ETL
    logger.info("Phase 2: ETL...")
    try:
        run_etl()
    except Exception as e:
        logger.error(f"ETL failed: {e}")
    
    # Phase 3: Analysis
    logger.info("Phase 3: Analysis...")
    try:
        run_analysis(run_id)
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
    
    # Phase 4: Stories
    logger.info("Phase 4: Generating stories...")
    try:
        generate_stories(run_id)
    except Exception as e:
        logger.error(f"Story generation failed: {e}")
    
    finished = datetime.now(timezone.utc)
    duration = (finished - started).total_seconds()
    
    logger.info(f"=== Pipeline {run_id} complete in {duration:.1f}s ===")
    
    return {
        "run_id": run_id,
        "started": started.isoformat(),
        "finished": finished.isoformat(),
        "duration_seconds": duration
    }


if __name__ == "__main__":
    result = run_pipeline()
    print(f"\nPipeline result: {result}")
