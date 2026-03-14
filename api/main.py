"""FastAPI application for GovDataStory."""

import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query, Request, Header, Depends, status
from fastapi.middleware.cors import CORSMiddleware
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
    return HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail={"error": {"message": "Rate limit exceeded", "code": "RATE_LIMITED"}}
    )


app.add_exception_handler(RateLimitExceeded, rate_limit_handler)


async def rate_limit_handler(request, exc):
    return HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail={"error": {"message": "Rate limit exceeded", "code": "RATE_LIMITED"}}
    )


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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
