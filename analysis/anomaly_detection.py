"""Anomaly detection using Isolation Forest + Z-score for GovDataStory."""

import json
import logging
from datetime import datetime, timezone

import duckdb
import numpy as np
from scipy.stats import zscore
from sklearn.ensemble import IsolationForest

logger = logging.getLogger(__name__)
DB_PATH = "data/govdatastory.duckdb"


def detect_anomalies(run_id: str, conn=None) -> list[dict]:
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

        if len(counts) < 10:
            continue

        # Isolation Forest
        iso_labels = IsolationForest(contamination=0.05, random_state=42).fit_predict(counts.reshape(-1, 1))
        iso_idx = set(np.where(iso_labels == -1)[0])

        # Z-score
        z = zscore(counts)
        z_idx = set(np.where(np.abs(z) > 2.5)[0])

        anomaly_idx = sorted(iso_idx | z_idx)
        if not anomaly_idx:
            continue

        anomaly_months = [months[i] for i in anomaly_idx if i < len(months)]
        max_z = float(np.max(np.abs(z[anomaly_idx])))
        methods = []
        if iso_idx:
            methods.append("isolation_forest")
        if z_idx:
            methods.append("zscore")

        insight = {
            "type": "anomaly",
            "topic": topic,
            "anomaly_months": anomaly_months,
            "severity": round(max_z, 4),
            "description": f"{topic.title()} datasets spiked abnormally in {', '.join(anomaly_months)}",
            "confidence": round(min(max_z / 5.0, 1.0), 4),
            "evidence": {"methods": methods, "anomaly_count": len(anomaly_idx), "max_zscore": round(max_z, 4)},
        }
        insights.append(insight)

        conn.execute(
            "INSERT OR REPLACE INTO analysis_results (id, topic, metric, value, run_id, created_at) VALUES (?,?,?,?,?,?)",
            [f"{topic}_advanced_anomalies_{run_id}", topic, "advanced_anomalies", json.dumps(insight), run_id, now],
        )

    if own:
        conn.close()
    logger.info("Detected %d anomaly insights for run %s", len(insights), run_id)
    return insights
