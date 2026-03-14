# UK Data Stories

![MIT License](https://img.shields.io/badge/License-MIT-green.svg)
![Python 3.11+](https://img.shields.io/badge/Python-3.11+-blue.svg)
![DuckDB](https://img.shields.io/badge/DuckDB-1.1+-blue.svg)

An open-source **Statistical Insight Discovery Engine** for UK government datasets. Ingests data from data.gov.uk, ONS, Police UK, and Parliament APIs, then applies classical statistical and ML methods to automatically discover trends, anomalies, correlations, and structural patterns — producing ranked insights and narrative summaries.

## Architecture

```
Scrapers → ETL → DuckDB → Analysis Engine → Insight Ranker → Narrator → FastAPI → Streamlit
```

### Analysis Pipeline

| Module | Technique | Reference |
|---|---|---|
| `trend_detection.py` | STL decomposition + linear regression | Cleveland et al. 1990 |
| `change_point_detection.py` | PELT algorithm | Truong et al. 2020 |
| `anomaly_detection.py` | Isolation Forest + Z-score | Liu et al. 2008 |
| `correlation_analysis.py` | Spearman rank + cross-correlation lag | — |
| `association_rules.py` | Apriori rule mining | Agrawal et al. 1994 |
| `graph_analysis.py` | Louvain community detection | Blondel et al. 2008 |
| `insight_ranker.py` | Composite scoring (severity/confidence/novelty/quality) | — |

Each module outputs structured insight objects:

```json
{
  "type": "trend",
  "topic": "transport",
  "direction": "increase",
  "magnitude": 0.42,
  "timeframe": "2022-2024",
  "confidence": 0.88
}
```

### Insight Ranking

```
score = 0.4 × severity + 0.3 × confidence + 0.2 × novelty + 0.1 × data_quality
```

## Quickstart

```bash
# Setup
cd govdatastory
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# Initialize database
mkdir -p data
.venv/bin/python -c "from db.schema import init_db; init_db('data/govdatastory.duckdb')"

# Run full pipeline (scrape → ETL → analysis → stories)
.venv/bin/python run_pipeline.py

# Start API
.venv/bin/python -m uvicorn api.main:app --host 0.0.0.0 --port 8000

# Start dashboard
.venv/bin/python -m streamlit run dashboard/app.py
```

## API Endpoints

All endpoints support optional `X-API-Key` header (configure in `.env`).

### Datasets
```bash
GET /datasets                          # List datasets (paginated)
GET /datasets?topic=health             # Filter by topic
GET /datasets/search?q=population      # Full-text search
GET /datasets/{id}                     # Single dataset
```

### Insights
```bash
GET /insights                          # All insights (paginated, ranked)
GET /insights/top?limit=10             # Top-ranked insights
GET /insights/ranked-feed?limit=20     # Ranked feed from all analysis modules
GET /insights/change-points            # Structural shift detections
GET /insights/change-points?topic=crime
GET /insights/associations?limit=10    # Association rule mining results
GET /insights/graph                    # Graph community detection results
```

### Stories & Topics
```bash
GET /stories                           # Narrative data stories
GET /stories?topic=health
GET /topics                            # Topics with counts and top insight
```

### Meta
```bash
GET /health                            # Health check
GET /meta/schema                       # API field reference
GET /meta/sources                      # Data sources with last ingest
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
| `source` | string | Data source (ons_api, data_gov_uk, etc.) |
| `ingested_at` | timestamp | When data was ingested |
| `quality_score` | number | Quality score (0-1) |

## Running Tests

```bash
# All tests (79 tests)
.venv/bin/python -m pytest tests/ -v

# With coverage
.venv/bin/python -m pytest tests/ --cov=analysis --cov=api --cov=stories --cov-report=term-missing

# Analysis engine tests only
.venv/bin/python -m pytest tests/test_analysis_engine.py -v

# Integration tests (requires real DB)
.venv/bin/python -m pytest tests/test_integration.py -m integration -v
```

## Project Structure

```
govdatastory/
├── analysis/
│   ├── patterns.py              # Main pipeline orchestrator
│   ├── insights.py              # Insight extraction from analysis results
│   ├── trend_detection.py       # STL decomposition trends
│   ├── change_point_detection.py # PELT change-point detection
│   ├── anomaly_detection.py     # Isolation Forest + Z-score
│   ├── correlation_analysis.py  # Spearman + cross-correlation lag
│   ├── association_rules.py     # Apriori rule mining
│   ├── graph_analysis.py        # NetworkX + Louvain communities
│   ├── insight_ranker.py        # Composite scoring
│   ├── clustering.py            # TF-IDF topic clustering
│   └── synthesis.py             # Cross-topic narrative synthesis
├── api/
│   └── main.py                  # FastAPI application
├── dashboard/
│   ├── app.py                   # Streamlit main page
│   ├── components.py            # Shared dashboard components
│   └── pages/                   # Topic-specific pages
├── db/
│   └── schema.py                # DuckDB schema and connections
├── etl/
│   └── transform.py             # ETL pipeline
├── scrapers/
│   ├── base.py                  # Base scraper class
│   ├── ckan_gov_uk.py           # CKAN API scraper
│   ├── data_gov_uk.py           # data.gov.uk scraper
│   ├── ons_api.py               # ONS API scraper
│   ├── police_uk.py             # Police UK API scraper
│   ├── parliament_bills.py      # Parliament Bills scraper
│   └── parliament_members.py    # Parliament Members scraper
├── stories/
│   └── narrator.py              # LLM + template narrative generation
├── scheduler/
│   └── refresh.py               # Scheduled pipeline runs
├── tests/
│   ├── test_analysis_engine.py  # Analysis engine tests (30 tests)
│   ├── test_api.py              # API endpoint tests
│   ├── test_db.py               # Database tests
│   ├── test_etl.py              # ETL tests
│   ├── test_integration.py      # Integration tests
│   └── ...
├── run_pipeline.py              # Full pipeline runner
├── requirements.txt
└── Dockerfile
```

## Configuration

Create `.env` file:

```bash
API_KEYS=changeme1,changeme2
DB_PATH=data/govdatastory.duckdb
PORT=8000
```

## Performance

- Runs locally on DuckDB (no external database required)
- Scales to 10k+ datasets
- No deep learning — classical statistical/ML methods only
- Incremental analysis via run_id tracking

## License

MIT License
