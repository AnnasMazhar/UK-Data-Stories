"""Insight engine — transforms raw analysis results into ranked structured insights."""

import json
import logging
from datetime import datetime, timezone

import duckdb

logger = logging.getLogger(__name__)

DB_PATH = "data/govdatastory.duckdb"

INIT_SQL = """
CREATE TABLE IF NOT EXISTS insights (
    id VARCHAR PRIMARY KEY,
    topic VARCHAR NOT NULL,
    insight_type VARCHAR NOT NULL,
    severity VARCHAR NOT NULL,
    confidence DOUBLE NOT NULL,
    title VARCHAR NOT NULL,
    summary TEXT NOT NULL,
    evidence JSON,
    run_id VARCHAR NOT NULL,
    rank_score DOUBLE DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def init_insights_table(conn=None):
    own = conn is None
    if own:
        conn = duckdb.connect(DB_PATH, read_only=False)
    conn.execute(INIT_SQL)
    # Migrate: add rank_score if missing
    try:
        conn.execute("ALTER TABLE insights ADD COLUMN rank_score DOUBLE DEFAULT 0")
    except Exception:
        pass  # column already exists
    if own:
        conn.close()


def _severity(score: float) -> str:
    if score >= 0.7:
        return "high"
    if score >= 0.4:
        return "medium"
    return "low"


def extract_trend_insight(topic: str, analysis: dict) -> dict | None:
    trend = analysis.get("trend")
    ts = analysis.get("timeseries")
    if not trend or trend.get("direction") == "unknown":
        return None

    slope = trend["slope"]
    r2 = trend["r_squared"]
    direction = trend["direction"]
    n_months = len(ts["months"]) if ts else 0

    # Confidence from R² and data length
    confidence = min(r2 * 0.7 + min(n_months / 100, 0.3), 1.0)
    score = abs(slope) * r2

    word = "increasing" if direction == "up" else "declining"
    return {
        "insight_type": "trend",
        "severity": _severity(score),
        "confidence": round(confidence, 3),
        "title": f"{topic.title()} dataset publishing is {word}",
        "summary": f"Over {n_months} months, {topic} datasets show a {word} trend "
                   f"(slope={slope:.2f}/month, R²={r2:.2f}).",
        "evidence": {"slope": slope, "r_squared": r2, "direction": direction, "months": n_months},
    }


def extract_anomaly_insight(topic: str, analysis: dict) -> dict | None:
    anom = analysis.get("anomalies")
    if not anom or not anom.get("months"):
        return None

    months = anom["months"]
    count = len(months)
    confidence = min(0.5 + count * 0.1, 0.95)

    return {
        "insight_type": "anomaly",
        "severity": _severity(count / 5),
        "confidence": round(confidence, 3),
        "title": f"{count} unusual spike{'s' if count != 1 else ''} in {topic} publishing",
        "summary": f"Anomalous publishing activity detected in {topic} during: {', '.join(months)}.",
        "evidence": {"anomaly_months": months, "count": count},
    }


def extract_seasonality_insight(topic: str, analysis: dict) -> dict | None:
    seas = analysis.get("seasonality")
    if not seas or seas.get("strength", 0) < 0.15:
        return None

    period = seas["dominant_period"]
    strength = seas["strength"]

    if 11 <= period <= 13:
        label = "annual"
    elif 5 <= period <= 7:
        label = "semi-annual"
    elif 2.5 <= period <= 3.5:
        label = "quarterly"
    else:
        label = f"{period:.0f}-month"

    return {
        "insight_type": "seasonality",
        "severity": _severity(strength),
        "confidence": round(min(strength + 0.3, 0.95), 3),
        "title": f"{topic.title()} shows {label} publishing cycle",
        "summary": f"{topic.title()} datasets follow a {label} pattern (strength={strength:.2f}).",
        "evidence": {"period": period, "strength": strength, "label": label},
    }


def extract_correlation_insights(analysis: dict) -> list[dict]:
    corr = analysis.get("correlations", {}).get("correlations", [])
    results = []
    for c in corr:
        r = abs(c["correlation"])
        strength = "strongly" if r > 0.6 else "moderately"
        direction = "positively" if c["correlation"] > 0 else "negatively"

        results.append({
            "topic": "cross_topic",
            "insight_type": "correlation",
            "severity": _severity(r),
            "confidence": round(min(1 - c["p_value"] + 0.1, 0.99), 3),
            "title": f"{c['topic1'].title()} and {c['topic2'].title()} are {strength} linked",
            "summary": f"{c['topic1'].title()} and {c['topic2'].title()} publishing trends are "
                       f"{strength} {direction} correlated (r={c['correlation']:.2f}, "
                       f"p={c['p_value']:.4f}, over {c['shared_months']} months).",
            "evidence": c,
        })
    return results


def extract_summary_insight(topic: str, analysis: dict) -> dict | None:
    s = analysis.get("summary")
    if not s:
        return None

    return {
        "insight_type": "summary",
        "severity": "low",
        "confidence": 0.99,
        "title": f"{topic.title()}: {s['total_datasets']} datasets from {s['num_organizations']} organisations",
        "summary": f"{topic.title()} has {s['total_datasets']} datasets published by "
                   f"{s['num_organizations']} organisations (avg quality {s['avg_quality']:.2f})."
                   + (f" Data spans {s['earliest_modified']} to {s['latest_modified']}."
                      if s.get("earliest_modified") else ""),
        "evidence": s,
    }


def _rank_score(ins: dict, existing_titles: set) -> float:
    """Compute composite rank score: 0.4*severity + 0.3*confidence + 0.2*novelty + 0.1*quality."""
    sev_map = {"high": 1.0, "medium": 0.6, "low": 0.3}
    severity_score = sev_map.get(ins.get("severity", "low"), 0.3)
    confidence_score = ins.get("confidence", 0.5)

    # Novelty: penalize if similar title already exists
    novelty = 1.0
    title_words = set(ins.get("title", "").lower().split())
    for existing in existing_titles:
        overlap = len(title_words & set(existing.lower().split())) / max(len(title_words), 1)
        if overlap > 0.6:
            novelty = 0.3
            break

    # Data quality from evidence
    evidence = ins.get("evidence", {})
    quality = min(evidence.get("avg_quality", 0.7), 1.0) if isinstance(evidence, dict) else 0.7

    return round(0.4 * severity_score + 0.3 * confidence_score + 0.2 * novelty + 0.1 * quality, 4)


def generate_insights(run_id: str, conn=None) -> int:
    """Read analysis_results for run_id, produce structured insights, store them."""
    own = conn is None
    if own:
        conn = duckdb.connect(DB_PATH, read_only=False)

    init_insights_table(conn)

    # Clear previous insights for this run (avoid duplicates across runs)
    conn.execute("DELETE FROM insights WHERE run_id != ?", [run_id])

    # Load all analysis results for this run
    rows = conn.execute(
        "SELECT topic, metric, value FROM analysis_results WHERE run_id = ?", [run_id]
    ).fetchall()

    # Group by topic
    by_topic: dict[str, dict] = {}
    for topic, metric, value in rows:
        by_topic.setdefault(topic, {})[metric] = json.loads(value) if isinstance(value, str) else value

    insights = []
    for topic, analysis in by_topic.items():
        if topic == "cross_topic":
            insights.extend(extract_correlation_insights(analysis))
            continue

        for extractor in (extract_trend_insight, extract_anomaly_insight,
                          extract_seasonality_insight, extract_summary_insight):
            ins = extractor(topic, analysis)
            if ins:
                ins["topic"] = topic
                insights.append(ins)

    # Rank with composite scoring
    existing_titles: set = set()
    for ins in insights:
        ins["rank_score"] = _rank_score(ins, existing_titles)
        existing_titles.add(ins.get("title", ""))
    insights.sort(key=lambda x: -x["rank_score"])

    # Store
    now = datetime.now(timezone.utc).isoformat()
    for ins in insights:
        conn.execute("""
            INSERT OR REPLACE INTO insights
            (id, topic, insight_type, severity, confidence, title, summary, evidence, run_id, rank_score, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            f"{ins['topic']}_{ins['insight_type']}_{run_id}",
            ins["topic"], ins["insight_type"], ins["severity"],
            ins["confidence"], ins["title"], ins["summary"],
            json.dumps(ins.get("evidence", {})), run_id, ins["rank_score"], now,
        ])

    if own:
        conn.close()

    logger.info(f"Generated {len(insights)} insights for run {run_id}")
    return len(insights)
