"""STL-based trend detection for GovDataStory."""

import json
import logging
from datetime import datetime, timezone

import duckdb
import numpy as np
from sklearn.linear_model import LinearRegression
from statsmodels.tsa.seasonal import STL

logger = logging.getLogger(__name__)
DB_PATH = "data/govdatastory.duckdb"


def _stl_trend(values: np.ndarray) -> tuple[np.ndarray, str]:
    return STL(values, period=12).fit().trend, "stl"


def _linear_trend(values: np.ndarray) -> tuple[np.ndarray, str]:
    X = np.arange(len(values)).reshape(-1, 1)
    return LinearRegression().fit(X, values).predict(X), "linear_regression"


def detect_trends(run_id: str, conn=None) -> list[dict]:
    own = conn is None
    if own:
        conn = duckdb.connect(DB_PATH, read_only=False)

    rows = conn.execute(
        "SELECT topic, value FROM analysis_results WHERE run_id = ? AND metric = 'timeseries'",
        [run_id],
    ).fetchall()

    insights = []
    now = datetime.now(timezone.utc).isoformat()

    for topic, raw in rows:
        if topic == "cross_topic":
            continue
        data = json.loads(raw) if isinstance(raw, str) else raw
        months = data.get("months", [])
        counts = np.array(data.get("counts", []), dtype=float)

        if len(months) < 6:
            continue

        if len(months) >= 24:
            trend, method = _stl_trend(counts)
        else:
            trend, method = _linear_trend(counts)

        growth = float((trend[-1] - trend[0]) / max(trend[0], 1))
        if abs(growth) <= 0.1:
            continue

        X = np.arange(len(trend)).reshape(-1, 1)
        reg = LinearRegression().fit(X, trend)
        r2 = float(reg.score(X, trend))

        years = sorted({m[:4] for m in months})
        insight = {
            "type": "trend",
            "topic": topic,
            "direction": "increase" if growth > 0 else "decrease",
            "magnitude": round(growth, 4),
            "timeframe": f"{years[0]}-{years[-1]}" if len(years) > 1 else years[0],
            "confidence": round(r2, 4),
            "evidence": {"slope": float(reg.coef_[0]), "r_squared": r2, "method": method},
        }
        insights.append(insight)

        conn.execute(
            "INSERT OR REPLACE INTO analysis_results (id, topic, metric, value, run_id, created_at) VALUES (?,?,?,?,?,?)",
            [f"{topic}_stl_trend_{run_id}", topic, "stl_trend", json.dumps(insight), run_id, now],
        )

    if own:
        conn.close()
    logger.info("Detected %d STL trends for run %s", len(insights), run_id)
    return insights
