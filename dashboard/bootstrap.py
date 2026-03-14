"""Bootstrap: build DB if missing (for Streamlit Cloud)."""
import os, sys, logging

logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.chdir(ROOT)
DB_PATH = os.path.join(ROOT, "data", "govdatastory.duckdb")


def bootstrap():
    if os.path.exists(DB_PATH):
        import duckdb
        conn = duckdb.connect(DB_PATH, read_only=True)
        count = conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]
        conn.close()
        if count > 0:
            return

    logging.info("Bootstrapping DB...")
    os.makedirs(os.path.join(ROOT, "data"), exist_ok=True)
    os.makedirs(os.path.join(ROOT, "raw"), exist_ok=True)

    from db.schema import init_db
    init_db(DB_PATH)

    from scrapers.ckan_gov_uk import CKANGovUKScraper
    from scrapers.ons_api import ONSScraper
    from pathlib import Path

    raw = Path(os.path.join(ROOT, "raw"))
    CKANGovUKScraper(output_dir=raw).run(max_per_theme=200)
    ONSScraper(output_dir=raw).run(max_datasets=500)

    from etl.transform import run_etl
    run_etl()

    from analysis.patterns import run_analysis
    run_analysis()
    logging.info("Bootstrap complete")
