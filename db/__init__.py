"""Database package."""

from db.schema import (
    SCHEMA_SQL,
    FTS_SQL,
    init_db,
    init_fts,
    get_connection,
    get_write_connection,
    close_connection,
)

__all__ = [
    "SCHEMA_SQL",
    "FTS_SQL",
    "init_db",
    "init_fts",
    "get_connection",
    "get_write_connection",
    "close_connection",
]
