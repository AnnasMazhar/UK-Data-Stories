# GovDataStory

![MIT License](https://img.shields.io/badge/License-MIT-green.svg)
![Python 3.11+](https://img.shields.io/badge/Python-3.11+-blue.svg)
![DuckDB](https://img.shields.io/badge/DuckDB-1.1+-blue.svg)

UK Government Data API — organized by topic, exposed via REST. Scrapes data from ONS (Office for National Statistics) and data.gov.uk, transforms it into structured datasets, and exposes via a FastAPI REST API.

## Why DuckDB?

- **Embedded**: Single file, no server required
- **Fast**: Columnar store, optimized for analytics  
- **Free**: MIT licensed, no proprietary dependencies
- **Simple**: SQLite-like API with SQL power

## Quickstart

```bash
# 1. Clone and setup
cd govdatastory
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 2. Initialize database
mkdir -p data
.venv/bin/python -c "from db.schema import init_db; init_db('data/govdatastory.duckdb')"

# 3. Run scrapers manually (or use scheduler)
.venv/bin/python -c "from scrapers.ons_api import ONSScraper; ONSScraper(output_dir='raw').run(max_datasets=100)"

# 4. Load data into DuckDB
# (See etl/transform.py for automated loading)

# 5. Start API
.venv/bin/python -m uvicorn api.main:app --host 0.0.0.0 --port 8000
```

## API Examples

All endpoints support optional `X-API-Key` header (configure in `.env`).

```bash
# Root info
curl http://localhost:8000/

# Health check
curl http://localhost:8000/health

# List datasets (paginated)
curl http://localhost:8000/datasets?page=1&limit=20

# Filter by topic
curl "http://localhost:8000/datasets?topic=health"

# Sort by quality
curl "http://localhost:8000/datasets?sort=quality_score&order=desc"

# Get single dataset
curl http://localhost:8000/datasets/wellbeing-quarterly

# Search datasets
curl "http://localhost:8000/datasets/search?q=population"

# Get schema
curl http://localhost:8000/meta/schema

# Get sources
curl http://localhost:8000/meta/sources

# With API key
curl -H "X-API-Key: your-key-here" http://localhost:8000/datasets
```

## Field Reference

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Unique identifier |
| `title` | string | Dataset title |
| `description` | string | Dataset description |
| `topic` | string | Inferred topic (health, economy, etc.) |
| `keywords` | array | Tags/keywords |
| `organization` | string | Publishing organization |
| `url` | string | Source URL |
| `license` | string | License type |
| `source` | string | Data source (ons_api, data_gov_uk) |
| `ingested_at` | timestamp | When data was ingested |
| `quality_score` | number | Quality score (0-1) |

## Running Tests

```bash
# Unit tests only
.venv/bin/python -m pytest tests/ -v

# With coverage
.venv/bin/python -m pytest tests/ --cov=. --cov-report=html

# Integration tests (requires real DB)
.venv/bin/python -m pytest tests/test_integration.py -m integration -v
```

## Configuration

Create `.env` file:

```bash
API_KEYS=changeme1,changeme2
DB_PATH=data/govdatastory.duckdb
PORT=8000
```

## License

MIT License - see LICENSE file.
