"""FastAPI application for GovDataStory."""

import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query, Request, Header, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from db.schema import init_db, get_connection, close_connection

# Rate limiter
limiter = Limiter(key_func=get_remote_address)

# Start time for uptime calculation
START_TIME = time.time()

# Database path
DB_PATH = Path("data/govdatastory.duckdb")

# API keys from environment
API_KEYS = os.getenv("API_KEYS", "").split(",") if os.getenv("API_KEYS") else []


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database on startup."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not DB_PATH.exists():
        init_db(DB_PATH)
    yield
    close_connection()


app = FastAPI(
    title="GovDataStory API",
    description="UK Government Data Stories - Organized by topic from data.gov.uk and ONS",
    version="1.0.0",
    lifespan=lifespan,
)

# Versioned router
from fastapi import APIRouter  # noqa: E402
v1 = APIRouter(prefix="/api/v1", tags=["v1"])

app.state.limiter = limiter

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Error handler must be added AFTER app is created
async def rate_limit_handler(request, exc):
    return JSONResponse(
        status_code=429,
        content={"error": {"message": "Rate limit exceeded", "code": "RATE_LIMITED"}}
    )


app.add_exception_handler(RateLimitExceeded, rate_limit_handler)


def verify_api_key(x_api_key: Annotated[str | None, Header()] = None):
    """Verify API key - FastAPI dependency."""
    if API_KEYS and x_api_key not in API_KEYS:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": {"message": "Invalid or missing API key", "code": "UNAUTHORIZED"}}
        )
    return x_api_key


# Optional auth dependency (doesn't raise if no keys configured)
def optional_auth(x_api_key: Annotated[str | None, Header()] = None):
    """Optional auth - passes if no keys configured or valid key provided."""
    if API_KEYS and x_api_key not in API_KEYS:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": {"message": "Invalid or missing API key", "code": "UNAUTHORIZED"}}
        )
    return True


# Models
class RecordList(BaseModel):
    data: list[dict]
    meta: dict


class SourceInfo(BaseModel):
    source: str
    last_ingest: str | None = None
    record_count: int


# Helpers
def get_db():
    """Get database connection (read-only)."""
    return get_connection(DB_PATH)


def record_to_dict(row: tuple, columns: list[str]) -> dict:
    """Convert DB row to dict."""
    return dict(zip(columns, row))


# Endpoints - NO AUTH REQUIRED
@app.get("/", tags=["info"])
async def root():
    """Root endpoint with API info."""
    db = get_db()
    total = db.execute("SELECT COUNT(*) FROM records").fetchone()[0]
    last_updated = db.execute("SELECT MAX(ingested_at) FROM records").fetchone()[0]
    
    return {
        "name": "GovDataStory API",
        "version": "1.0.0",
        "record_count": total,
        "last_updated": last_updated
    }


@app.get("/health", tags=["health"])
async def health():
    """Health check endpoint."""
    db = get_db()
    try:
        record_count = db.execute("SELECT COUNT(*) FROM records").fetchone()[0]
    except Exception:
        record_count = 0
    
    return {
        "status": "ok",
        "db": "connected" if DB_PATH.exists() else "missing",
        "record_count": record_count,
        "uptime_seconds": int(time.time() - START_TIME)
    }


# SEARCH endpoint must come BEFORE /datasets/{record_id} to avoid route conflicts
@app.get("/datasets/search", response_model=RecordList, tags=["search"])
@limiter.limit("100/minute")
async def search_datasets(
    request: Request,
    q: str = Query(..., min_length=1, description="Search query"),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    _: bool = Depends(verify_api_key)
):
    """Search datasets using full-text search."""
    if limit > 100:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"error": {"message": "Limit cannot exceed 100", "code": "INVALID_LIMIT"}}
        )
    
    db = get_db()
    offset = (page - 1) * limit
    
    search_term = f"%{q}%"
    total = db.execute("""
        SELECT COUNT(*) FROM records 
        WHERE title LIKE ? OR description LIKE ? OR topic LIKE ?
    """, [search_term, search_term, search_term]).fetchone()[0]
    
    rows = db.execute("""
        SELECT id, title, description, topic, keywords, organization, url,
               license, source, ingested_at, quality_score
        FROM records 
        WHERE title LIKE ? OR description LIKE ? OR topic LIKE ?
        ORDER BY quality_score DESC, ingested_at DESC
        LIMIT ? OFFSET ?
    """, [search_term, search_term, search_term, limit, offset]).fetchall()
    
    columns = [desc[0] for desc in db.description]
    data = [record_to_dict(row, columns) for row in rows]
    
    # Convert keywords
    for d in data:
        if d.get("keywords") and isinstance(d["keywords"], str):
            import json
            try:
                d["keywords"] = json.loads(d["keywords"])
            except json.JSONDecodeError:
                d["keywords"] = []
    
    return {
        "data": data,
        "meta": {
            "query": q,
            "page": page,
            "limit": limit,
            "total": total,
            "generated_at": datetime.now(timezone.utc).isoformat()
        }
    }


@app.get("/datasets", response_model=RecordList, tags=["datasets"])
@limiter.limit("100/minute")
async def list_datasets(
    request: Request,
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Results per page"),
    sort: str = Query("ingested_at", description="Sort field"),
    order: str = Query("desc", pattern="^(asc|desc)$"),
    topic: str | None = Query(None, description="Filter by topic"),
    _: bool = Depends(verify_api_key)
):
    """List all datasets with pagination."""
    if limit > 100:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"error": {"message": "Limit cannot exceed 100", "code": "INVALID_LIMIT"}}
        )
    
    ALLOWED_SORT = {"id", "title", "topic", "source", "ingested_at", "quality_score"}
    if sort not in ALLOWED_SORT:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"error": {"message": f"Invalid sort field. Allowed: {', '.join(sorted(ALLOWED_SORT))}", "code": "INVALID_SORT"}}
        )
    
    db = get_db()
    offset = (page - 1) * limit
    where_clause = "WHERE 1=1"
    params = []
    
    if topic:
        where_clause += " AND topic = ?"
        params.append(topic)
    
    total = db.execute(f"SELECT COUNT(*) FROM records {where_clause}", params).fetchone()[0]
    
    order = order.upper()
    query = f"""
        SELECT id, title, description, topic, keywords, organization, url, 
               license, source, ingested_at, quality_score
        FROM records 
        {where_clause}
        ORDER BY {sort} {order}
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])
    
    rows = db.execute(query, params).fetchall()
    columns = [desc[0] for desc in db.description]
    data = [record_to_dict(row, columns) for row in rows]
    
    for d in data:
        if d.get("keywords") and isinstance(d["keywords"], str):
            import json
            try:
                d["keywords"] = json.loads(d["keywords"])
            except json.JSONDecodeError:
                d["keywords"] = []
    
    return {
        "data": data,
        "meta": {
            "page": page,
            "limit": limit,
            "total": total,
            "pages": (total + limit - 1) // limit,
            "generated_at": datetime.now(timezone.utc).isoformat()
        }
    }


@app.get("/datasets/{record_id}", response_model=dict, tags=["datasets"])
@limiter.limit("100/minute")
async def get_dataset(
    request: Request,
    record_id: str,
    _: bool = Depends(verify_api_key)
):
    """Get a single dataset by ID."""
    db = get_db()
    row = db.execute("""
        SELECT id, title, description, topic, keywords, organization, url,
               license, source, ingested_at, quality_score
        FROM records WHERE id = ?
    """, [record_id]).fetchone()
    
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": {"message": f"Dataset '{record_id}' not found", "code": "NOT_FOUND"}}
        )
    
    columns = [desc[0] for desc in db.description]
    return record_to_dict(row, columns)


@app.get("/meta/schema", tags=["meta"])
async def get_schema(_: bool = Depends(verify_api_key)):
    """Get API schema."""
    return {
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "title", "type": "string"},
            {"name": "description", "type": "string"},
            {"name": "topic", "type": "string"},
            {"name": "keywords", "type": "array"},
            {"name": "organization", "type": "string"},
            {"name": "url", "type": "string"},
            {"name": "license", "type": "string"},
            {"name": "source", "type": "string"},
            {"name": "ingested_at", "type": "timestamp"},
            {"name": "quality_score", "type": "number"}
        ]
    }


@app.get("/meta/sources", response_model=list[SourceInfo], tags=["meta"])
async def get_sources(_: bool = Depends(verify_api_key)):
    """Get data sources with last ingest timestamps."""
    db = get_db()
    rows = db.execute("""
        SELECT source, MAX(ingested_at) as last_ingest, COUNT(*) as record_count
        FROM records GROUP BY source
    """).fetchall()
    
    return [
        {
            "source": row[0], 
            "last_ingest": str(row[1]) if row[1] else None, 
            "record_count": row[2]
        }
        for row in rows
    ]


@app.get("/insights", tags=["insights"])
@limiter.limit("100/minute")
async def list_insights(
    request: Request,
    topic: str | None = Query(None),
    severity: str | None = Query(None, pattern="^(high|medium|low)$"),
    insight_type: str | None = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    _: bool = Depends(verify_api_key),
):
    """List structured insights with pagination, ranked by composite score."""
    db = get_db()
    where, params = ["1=1"], []
    if topic:
        where.append("topic = ?")
        params.append(topic)
    if severity:
        where.append("severity = ?")
        params.append(severity)
    if insight_type:
        where.append("insight_type = ?")
        params.append(insight_type)

    w = " AND ".join(where)
    total = db.execute(f"SELECT COUNT(*) FROM insights WHERE {w}", params).fetchone()[0]
    offset = (page - 1) * limit

    rows = db.execute(f"""
        SELECT id, topic, insight_type, severity, confidence, title, summary, evidence, run_id, rank_score, created_at
        FROM insights WHERE {w}
        ORDER BY rank_score DESC
        LIMIT ? OFFSET ?
    """, params + [limit, offset]).fetchall()
    cols = [d[0] for d in db.description]
    return {"data": [dict(zip(cols, r)) for r in rows], "meta": {"page": page, "limit": limit, "total": total, "pages": (total + limit - 1) // limit}}


@app.get("/insights/top", tags=["insights"])
@limiter.limit("100/minute")
async def top_insights(
    request: Request,
    limit: int = Query(10, ge=1, le=50),
    _: bool = Depends(verify_api_key),
):
    """Get top-ranked insights across all topics."""
    db = get_db()
    rows = db.execute("""
        SELECT id, topic, insight_type, severity, confidence, title, summary, evidence, rank_score
        FROM insights ORDER BY rank_score DESC LIMIT ?
    """, [limit]).fetchall()
    cols = [d[0] for d in db.description]
    return {"data": [dict(zip(cols, r)) for r in rows]}


@app.get("/insights/ranked-feed", tags=["insights"])
@limiter.limit("100/minute")
async def ranked_feed(
    request: Request,
    limit: int = Query(20, ge=1, le=100),
    _: bool = Depends(verify_api_key),
):
    """Get the ranked insight feed from all advanced analysis modules."""
    import json as _json
    db = get_db()
    row = db.execute("""
        SELECT value FROM analysis_results WHERE metric = 'ranked_feed'
        ORDER BY created_at DESC LIMIT 1
    """).fetchone()
    if not row:
        return {"data": [], "meta": {"total": 0}}
    feed = _json.loads(row[0]) if isinstance(row[0], str) else row[0]
    return {"data": feed[:limit], "meta": {"total": len(feed)}}


@app.get("/insights/change-points", tags=["insights"])
@limiter.limit("100/minute")
async def change_points(
    request: Request,
    topic: str | None = Query(None),
    _: bool = Depends(verify_api_key),
):
    """Get change-point detection results."""
    import json as _json
    db = get_db()
    if topic:
        rows = db.execute(
            "SELECT topic, value FROM analysis_results WHERE metric = 'change_points' AND topic = ? ORDER BY created_at DESC",
            [topic],
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT topic, value FROM analysis_results WHERE metric = 'change_points' ORDER BY created_at DESC"
        ).fetchall()
    data = [{"topic": r[0], **(_json.loads(r[1]) if isinstance(r[1], str) else r[1])} for r in rows]
    return {"data": data}


@app.get("/insights/associations", tags=["insights"])
@limiter.limit("100/minute")
async def associations(
    request: Request,
    limit: int = Query(20, ge=1, le=100),
    _: bool = Depends(verify_api_key),
):
    """Get association rule mining results."""
    import json as _json
    db = get_db()
    row = db.execute(
        "SELECT value FROM analysis_results WHERE metric = 'association_rules' ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    if not row:
        return {"data": []}
    rules = _json.loads(row[0]) if isinstance(row[0], str) else row[0]
    return {"data": rules[:limit]}


@app.get("/insights/graph", tags=["insights"])
@limiter.limit("100/minute")
async def graph_insights(
    request: Request,
    _: bool = Depends(verify_api_key),
):
    """Get graph-based community detection results."""
    import json as _json
    db = get_db()
    row = db.execute(
        "SELECT value FROM analysis_results WHERE metric = 'graph_analysis' ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    if not row:
        return {"data": {}}
    return {"data": _json.loads(row[0]) if isinstance(row[0], str) else row[0]}


@app.get("/stories", tags=["stories"])
@limiter.limit("100/minute")
async def list_stories(
    request: Request,
    topic: str | None = Query(None),
    limit: int = Query(10, ge=1, le=50),
    _: bool = Depends(verify_api_key),
):
    """List narrative data stories."""
    db = get_db()
    if topic:
        rows = db.execute("""
            SELECT topic, headline, key_finding, context, outlook, annotations, model_used, created_at
            FROM data_stories WHERE topic = ? ORDER BY created_at DESC LIMIT ?
        """, [topic, limit]).fetchall()
    else:
        rows = db.execute("""
            SELECT topic, headline, key_finding, context, outlook, annotations, model_used, created_at
            FROM data_stories ORDER BY created_at DESC LIMIT ?
        """, [limit]).fetchall()
    cols = [d[0] for d in db.description]
    return {"data": [dict(zip(cols, r)) for r in rows]}


@app.get("/topics", tags=["topics"])
async def list_topics(_: bool = Depends(verify_api_key)):
    """List topics with dataset counts and latest insight."""
    db = get_db()
    topics = db.execute("""
        SELECT topic, COUNT(*) as count FROM records GROUP BY topic ORDER BY count DESC
    """).fetchall()

    result = []
    for topic, count in topics:
        insight = db.execute("""
            SELECT title, severity FROM insights
            WHERE topic = ? ORDER BY CASE severity WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END, confidence DESC LIMIT 1
        """, [topic]).fetchone()
        result.append({
            "topic": topic, "count": count,
            "top_insight": {"title": insight[0], "severity": insight[1]} if insight else None,
        })
    return {"data": result}


if __name__ == "__main__":
    import uvicorn

    # Register versioned routes (mirror existing endpoints)
    v1.add_api_route("/insights", list_insights, methods=["GET"])
    v1.add_api_route("/insights/top", top_insights, methods=["GET"])
    v1.add_api_route("/insights/ranked-feed", ranked_feed, methods=["GET"])
    v1.add_api_route("/insights/change-points", change_points, methods=["GET"])
    v1.add_api_route("/insights/associations", associations, methods=["GET"])
    v1.add_api_route("/insights/graph", graph_insights, methods=["GET"])
    v1.add_api_route("/stories", list_stories, methods=["GET"])
    v1.add_api_route("/topics", list_topics, methods=["GET"])
    v1.add_api_route("/datasets", list_datasets, methods=["GET"])
    v1.add_api_route("/datasets/search", search_datasets, methods=["GET"])
    app.include_router(v1)

    uvicorn.run(app, host="0.0.0.0", port=8000)
else:
    # Also register when imported (e.g. by uvicorn)
    v1.add_api_route("/insights", list_insights, methods=["GET"])
    v1.add_api_route("/insights/top", top_insights, methods=["GET"])
    v1.add_api_route("/insights/ranked-feed", ranked_feed, methods=["GET"])
    v1.add_api_route("/insights/change-points", change_points, methods=["GET"])
    v1.add_api_route("/insights/associations", associations, methods=["GET"])
    v1.add_api_route("/insights/graph", graph_insights, methods=["GET"])
    v1.add_api_route("/stories", list_stories, methods=["GET"])
    v1.add_api_route("/topics", list_topics, methods=["GET"])
    v1.add_api_route("/datasets", list_datasets, methods=["GET"])
    v1.add_api_route("/datasets/search", search_datasets, methods=["GET"])
    app.include_router(v1)
