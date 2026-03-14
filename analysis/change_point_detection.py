"""PELT change-point detection for GovDataStory."""

import json
import logging
from datetime import datetime, timezone

import duckdb
import numpy as np
import ruptures

logger = logging.getLogger(__name__)
DB_PATH = "data/govdatastory.duckdb"


def detect_change_points(run_id: str, conn=None) -> list[dict]:
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

        if len(counts) < 12:
            continue

        signal = counts.reshape(-1, 1)
        bkps = ruptures.Pelt(model="rbf", min_size=3, jump=1).fit_predict(signal, pen=10)
        # ruptures appends len(signal) as last breakpoint — exclude it
        change_idx = [b for b in bkps if b < len(months)]

        if not change_idx:
            continue

        change_months = [months[i] for i in change_idx]
        insight = {
            "type": "change_point",
            "topic": topic,
            "change_months": change_months,
            "description": f"{topic.title()} publication regime shifted in {', '.join(change_months)}",
            "confidence": 0.75,
            "evidence": {"breakpoint_indices": change_idx, "n_points": len(counts)},
        }
        insights.append(insight)

        conn.execute(
            "INSERT OR REPLACE INTO analysis_results (id, topic, metric, value, run_id, created_at) VALUES (?,?,?,?,?,?)",
            [f"{topic}_change_points_{run_id}", topic, "change_points", json.dumps(insight), run_id, now],
        )

    if own:
        conn.close()
    logger.info("Detected %d change-point insights for run %s", len(insights), run_id)
    return insights
