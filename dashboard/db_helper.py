"""Shared DB access for dashboard pages."""
import os, sys, duckdb, json

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
DB_PATH = os.path.join(ROOT, "data", "govdatastory.duckdb")


def ensure_db():
    """Bootstrap DB if missing."""
    from bootstrap import bootstrap
    bootstrap()


def query(sql, params=None):
    """Safe query - returns [] on error."""
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        result = conn.execute(sql, params or []).fetchall()
        conn.close()
        return result
    except Exception:
        return []


def query_one(sql, params=None):
    rows = query(sql, params)
    return rows[0] if rows else None


def get_topic_data(topic):
    result = {}
    ts = query_one("SELECT value FROM analysis_results WHERE topic = ? AND metric = 'timeseries' ORDER BY created_at DESC LIMIT 1", [topic])
    if ts:
        data = json.loads(ts[0])
        result['months'] = data.get('months', [])
        result['counts'] = data.get('counts', [])
    trend = query_one("SELECT value FROM analysis_results WHERE topic = ? AND metric = 'trend' ORDER BY created_at DESC LIMIT 1", [topic])
    if trend:
        result['trend'] = json.loads(trend[0])
    summary = query_one("SELECT value FROM analysis_results WHERE topic = ? AND metric = 'summary' ORDER BY created_at DESC LIMIT 1", [topic])
    if summary:
        result['summary'] = json.loads(summary[0])
    story = query_one("SELECT headline, key_finding, context, outlook FROM data_stories WHERE topic = ? ORDER BY created_at DESC LIMIT 1", [topic])
    if story:
        result['story'] = {"headline": story[0], "key_finding": story[1], "context": story[2], "outlook": story[3]}
    return result


def get_datasets(topic, limit=20):
    return query("SELECT title, organization FROM records WHERE topic = ? ORDER BY quality_score DESC LIMIT ?", [topic, limit])
