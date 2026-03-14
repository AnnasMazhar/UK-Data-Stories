"""Cross-topic narrative synthesis — lag detection and pairwise trend analysis."""

import json
import logging
from datetime import datetime, timezone

import duckdb
import numpy as np
from scipy import stats as scipy_stats

logger = logging.getLogger(__name__)

DB_PATH = "data/govdatastory.duckdb"


def _align_series(months_a, data_a, months_b, data_b):
    """Align two monthly series to shared months, filtering mutual zeros."""
    set_a, set_b = set(months_a), set(months_b)
    shared = sorted(set_a & set_b)
    if len(shared) < 6:
        return None, None, None
    idx_a = {m: i for i, m in enumerate(months_a)}
    idx_b = {m: i for i, m in enumerate(months_b)}
    a = np.array([data_a[idx_a[m]] for m in shared])
    b = np.array([data_b[idx_b[m]] for m in shared])
    mask = (a > 0) | (b > 0)
    if mask.sum() < 6:
        return None, None, None
    return shared, a[mask], b[mask]


def detect_lag(a: np.ndarray, b: np.ndarray, max_lag: int = 6) -> dict:
    """Find optimal lag between two series using cross-correlation."""
    if len(a) < max_lag + 3:
        return {"lag": 0, "correlation": 0}
    a_norm = (a - a.mean()) / (a.std() + 1e-9)
    b_norm = (b - b.mean()) / (b.std() + 1e-9)
    best_lag, best_corr = 0, 0
    for lag in range(-max_lag, max_lag + 1):
        if lag >= 0:
            x, y = a_norm[lag:], b_norm[:len(a_norm) - lag]
        else:
            x, y = a_norm[:len(a_norm) + lag], b_norm[-lag:]
        if len(x) < 4:
            continue
        r = float(np.corrcoef(x, y)[0, 1])
        if abs(r) > abs(best_corr):
            best_corr, best_lag = r, lag
    return {"lag": best_lag, "correlation": round(best_corr, 4)}


def _timeframe(shared_months):
    """Extract year range string from shared months list."""
    if not shared_months:
        return ""
    years = sorted({m[:4] for m in shared_months})
    if len(years) == 1:
        return years[0]
    return f"{years[0]}-{years[-1]}"


def generate_cross_topic_insights(run_id: str, conn=None) -> list[dict]:
    """Generate cross-topic synthesis insights from analysis results."""
    own = conn is None
    if own:
        conn = duckdb.connect(DB_PATH, read_only=False)

    # Load timeseries for all topics
    rows = conn.execute(
        "SELECT topic, value FROM analysis_results WHERE metric = 'timeseries' AND run_id = ?",
        [run_id],
    ).fetchall()

    series = {}
    months_map = {}
    for topic, val in rows:
        if topic == "cross_topic":
            continue
        d = json.loads(val) if isinstance(val, str) else val
        if d.get("months"):
            months_map[topic] = d["months"]
            series[topic] = np.array(d["counts"], dtype=float)

    topics = list(series.keys())
    insights = []
    now = datetime.now(timezone.utc).isoformat()

    for i, t1 in enumerate(topics):
        for t2 in topics[i + 1:]:
            shared, a, b = _align_series(months_map[t1], series[t1], months_map[t2], series[t2])
            if shared is None:
                continue

            # Spearman correlation
            corr, p = scipy_stats.spearmanr(a, b)
            if np.isnan(corr) or p >= 0.05 or abs(corr) < 0.3:
                continue

            # Lag detection
            lag_info = detect_lag(a, b)
            timeframe = _timeframe(shared)
            strength = "strongly" if abs(corr) > 0.6 else "moderately"
            direction = "positively" if corr > 0 else "negatively"

            insight = {
                "type": "cross_topic_trend",
                "topics": [t1, t2],
                "correlation": round(float(corr), 4),
                "p_value": round(float(p), 6),
                "timeframe": timeframe,
                "lag_months": lag_info["lag"],
                "lag_correlation": lag_info["correlation"],
                "confidence": round(min(1 - p + 0.1, 0.99), 3),
                "shared_months": len(shared),
            }

            # Build narrative
            lag_text = ""
            if abs(lag_info["lag"]) >= 2 and abs(lag_info["correlation"]) > abs(corr):
                leader, follower = (t1, t2) if lag_info["lag"] > 0 else (t2, t1)
                lag_text = (f" {leader.title()} leads {follower} by ~{abs(lag_info['lag'])} months "
                            f"(lagged r={lag_info['correlation']:.2f}).")

            title = f"{t1.title()} and {t2.title()} publishing trends are {strength} linked"
            summary = (f"{t1.title()} and {t2.title()} show {strength} {direction} correlation "
                       f"(r={corr:.2f}, p={p:.4f}) over {timeframe}.{lag_text}")

            # Store as insight
            severity = "high" if abs(corr) > 0.6 else "medium" if abs(corr) > 0.4 else "low"
            conn.execute("""
                INSERT OR REPLACE INTO insights
                (id, topic, insight_type, severity, confidence, title, summary, evidence, run_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                f"synthesis_{t1}_{t2}_{run_id}",
                "cross_topic", "cross_topic_trend", severity,
                insight["confidence"], title, summary,
                json.dumps(insight), run_id, now,
            ])
            insights.append(insight)

    if own:
        conn.close()

    logger.info(f"Generated {len(insights)} cross-topic synthesis insights")
    return insights
