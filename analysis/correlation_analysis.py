"""Spearman correlation and cross-correlation lag detection for GovDataStory."""

import json
import logging
from datetime import datetime, timezone
from itertools import combinations

import duckdb
import numpy as np
from scipy import stats

logger = logging.getLogger(__name__)
DB_PATH = "data/govdatastory.duckdb"


def analyze_cross_correlations(run_id: str, conn=None) -> list[dict]:
    own = conn is None
    if own:
        conn = duckdb.connect(DB_PATH, read_only=False)

    rows = conn.execute(
        "SELECT topic, value FROM analysis_results WHERE run_id = ? AND metric = 'timeseries'",
        [run_id],
    ).fetchall()

    # Build {topic: {month: count}} lookup
    series = {}
    for topic, raw in rows:
        if topic == "cross_topic":
            continue
        data = json.loads(raw) if isinstance(raw, str) else raw
        months = data.get("months", [])
        counts = data.get("counts", [])
        if months and counts:
            series[topic] = dict(zip(months, counts))

    insights = []
    now = datetime.now(timezone.utc).isoformat()

    for t1, t2 in combinations(sorted(series.keys()), 2):
        shared = sorted(set(series[t1]) & set(series[t2]))
        if len(shared) < 6:
            continue

        v1 = np.array([series[t1][m] for m in shared], dtype=float)
        v2 = np.array([series[t2][m] for m in shared], dtype=float)

        # Skip if either is constant
        if v1.std() == 0 or v2.std() == 0:
            continue

        r, p = stats.spearmanr(v1, v2)
        if np.isnan(r) or p >= 0.05 or abs(r) < 0.3:
            continue

        # Cross-correlation for lag detection
        max_lag = 6
        a = (v1 - v1.mean()) / (v1.std() + 1e-10)
        b = (v2 - v2.mean()) / (v2.std() + 1e-10)
        xcorr = np.correlate(a, b, mode="full")
        mid = len(v1) - 1
        lo, hi = max(0, mid - max_lag), min(len(xcorr), mid + max_lag + 1)
        lag_months = int(lo + np.argmax(xcorr[lo:hi]) - mid)

        years = sorted({m[:4] for m in shared})
        timeframe = f"{years[0]}-{years[-1]}" if len(years) > 1 else years[0]

        insight = {
            "type": "correlation",
            "topics": [t1, t2],
            "correlation": round(float(r), 4),
            "p_value": round(float(p), 6),
            "lag_months": lag_months,
            "timeframe": timeframe,
            "confidence": round(min(1 - float(p), 0.99), 4),
            "evidence": {"shared_months": len(shared), "spearman_r": round(float(r), 4), "lag": lag_months},
        }
        insights.append(insight)

    # Store all as single result
    if insights:
        conn.execute(
            "INSERT OR REPLACE INTO analysis_results (id, topic, metric, value, run_id, created_at) VALUES (?,?,?,?,?,?)",
            [f"cross_correlations_{run_id}", "cross_topic", "cross_correlations", json.dumps(insights), run_id, now],
        )

    if own:
        conn.close()
    logger.info("Found %d cross-correlation insights for run %s", len(insights), run_id)
    return insights
