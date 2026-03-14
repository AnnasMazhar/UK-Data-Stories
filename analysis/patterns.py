"""ML Pattern Analysis for GovDataStory."""

import json
import logging
import uuid
from datetime import datetime, timezone

import duckdb
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import IsolationForest
from scipy import stats as scipy_stats
from scipy.fft import rfft
from sklearn.cluster import KMeans

logger = logging.getLogger(__name__)

DB_PATH = "data/govdatastory.duckdb"


def get_db(read_only: bool = True):
    """Get DuckDB connection."""
    return duckdb.connect(DB_PATH, read_only=read_only)


def init_analysis_table():
    """Initialize analysis results table."""
    conn = get_db(read_only=False)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS analysis_results (
            id VARCHAR PRIMARY KEY,
            topic VARCHAR NOT NULL,
            metric VARCHAR NOT NULL,
            value JSON,
            run_id VARCHAR NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.close()


def store_analysis(topic: str, metric: str, value: dict, run_id: str, conn=None):
    """Store analysis result."""
    own_conn = conn is None
    if own_conn:
        conn = get_db(read_only=False)
    conn.execute("""
        INSERT OR REPLACE INTO analysis_results (id, topic, metric, value, run_id, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, [f"{topic}_{metric}_{run_id}", topic, metric, json.dumps(value), run_id, datetime.now(timezone.utc).isoformat()])
    if own_conn:
        conn.close()


def get_topic_timeseries(conn, topic: str) -> tuple[list[str], np.ndarray]:
    """Get monthly dataset counts for a topic from metadata_modified.

    Returns (months, counts) where months is a list of 'YYYY-MM' strings
    and counts is a numpy array of ints.
    """
    rows = conn.execute("""
        SELECT DATE_TRUNC('month', metadata_modified) as mo, COUNT(*) as cnt
        FROM records
        WHERE topic = ? AND metadata_modified IS NOT NULL
          AND metadata_modified <= CURRENT_TIMESTAMP
        GROUP BY mo
        ORDER BY mo
    """, [topic]).fetchall()

    if not rows:
        rows = conn.execute("""
            SELECT DATE_TRUNC('month', ingested_at) as mo, COUNT(*) as cnt
            FROM records
            WHERE topic = ?
            GROUP BY mo
            ORDER BY mo
        """, [topic]).fetchall()

    if not rows:
        return [], np.array([], dtype=float)

    # Fill gaps: create continuous monthly series
    raw = {str(r[0])[:7]: r[1] for r in rows}
    all_months = sorted(raw.keys())
    start, end = all_months[0], all_months[-1]

    filled_months = []
    y, m = int(start[:4]), int(start[5:7])
    ey, em = int(end[:4]), int(end[5:7])
    while (y, m) <= (ey, em):
        filled_months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1

    counts = np.array([raw.get(mo, 0) for mo in filled_months], dtype=float)
    return filled_months, counts


def analyze_trend(data: np.ndarray) -> dict:
    """Analyze trend using linear regression."""
    if len(data) < 2:
        return {"direction": "unknown", "slope": 0, "r_squared": 0}

    X = np.arange(len(data)).reshape(-1, 1)
    model = LinearRegression().fit(X, data)
    r_squared = model.score(X, data)

    return {
        "direction": "up" if model.coef_[0] > 0 else "down",
        "slope": float(model.coef_[0]),
        "r_squared": float(r_squared),
    }


def analyze_anomalies(data: np.ndarray) -> list:
    """Detect anomalies using IsolationForest."""
    if len(data) < 10:
        return []

    X = data.reshape(-1, 1)
    clf = IsolationForest(contamination=0.05, random_state=42)
    preds = clf.fit_predict(X)
    return [int(i) for i in range(len(data)) if preds[i] == -1]


def analyze_seasonality(data: np.ndarray) -> dict:
    """Analyze seasonality using FFT."""
    if len(data) < 8:
        return {"dominant_period": 0, "strength": 0}

    fft_vals = rfft(data)
    power = np.abs(fft_vals)

    if len(power) > 1:
        dominant_idx = np.argmax(power[1:]) + 1
        period = len(data) / dominant_idx if dominant_idx > 0 else 0
        strength = float(power[dominant_idx] / np.sum(power)) if np.sum(power) > 0 else 0
        return {"dominant_period": float(period), "strength": strength}

    return {"dominant_period": 0, "strength": 0}


def analyze_correlations(topic_series: dict[str, np.ndarray], topic_months: dict[str, list[str]]) -> list:
    """Analyze correlations between topic time-series aligned by month."""
    correlations = []
    topics = list(topic_series.keys())

    for i, t1 in enumerate(topics):
        for t2 in topics[i + 1:]:
            m1, m2 = set(topic_months.get(t1, [])), set(topic_months.get(t2, []))
            shared = sorted(m1 & m2)
            if len(shared) < 6:
                continue

            idx1 = {m: j for j, m in enumerate(topic_months[t1])}
            idx2 = {m: j for j, m in enumerate(topic_months[t2])}
            d1 = np.array([topic_series[t1][idx1[m]] for m in shared])
            d2 = np.array([topic_series[t2][idx2[m]] for m in shared])

            # Filter out months where both are zero (gap-fill artifacts)
            mask = (d1 > 0) | (d2 > 0)
            if mask.sum() < 6:
                continue
            d1, d2 = d1[mask], d2[mask]

            corr, p_value = scipy_stats.spearmanr(d1, d2)
            if not np.isnan(corr) and p_value < 0.05:
                correlations.append({
                    "topic1": t1,
                    "topic2": t2,
                    "correlation": float(corr),
                    "p_value": float(p_value),
                    "shared_months": len(shared),
                })

    return correlations


def analyze_org_clusters(conn) -> dict:
    """Cluster organizations by dataset volume per topic."""
    rows = conn.execute("""
        SELECT organization, topic, COUNT(*) as cnt
        FROM records
        WHERE organization IS NOT NULL
        GROUP BY organization, topic
        ORDER BY organization
    """).fetchall()

    if not rows:
        return {"clusters": {}, "centroids": []}

    # Build org -> topic vector
    all_topics = sorted({r[1] for r in rows})
    org_data: dict[str, dict] = {}
    for org, topic, cnt in rows:
        org_data.setdefault(org, {})[topic] = cnt

    orgs = list(org_data.keys())
    matrix = np.array([[org_data[o].get(t, 0) for t in all_topics] for o in orgs], dtype=float)

    # Normalize to proportions per org so we cluster by topic profile, not size
    row_sums = matrix.sum(axis=1, keepdims=True)
    row_sums[row_sums == 0] = 1
    matrix = matrix / row_sums

    if len(orgs) < 3:
        return {"clusters": {}, "centroids": []}

    n_clusters = min(5, len(orgs))
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    labels = kmeans.fit_predict(matrix)

    clusters: dict[int, list] = {}
    for i, org in enumerate(orgs):
        clusters.setdefault(int(labels[i]), []).append(org)

    return {
        "clusters": clusters,
        "centroids": kmeans.cluster_centers_.tolist(),
        "topics": all_topics,
    }


def analyze_topic_summary(conn, topic: str) -> dict:
    """Compute summary stats for a topic."""
    row = conn.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(DISTINCT organization) as num_orgs,
            AVG(quality_score) as avg_quality,
            MIN(metadata_modified) as earliest,
            MAX(metadata_modified) as latest
        FROM records WHERE topic = ?
    """, [topic]).fetchone()

    return {
        "total_datasets": row[0],
        "num_organizations": row[1],
        "avg_quality": float(row[2]) if row[2] else 0,
        "earliest_modified": str(row[3])[:10] if row[3] else None,
        "latest_modified": str(row[4])[:10] if row[4] else None,
    }


def run_analysis(run_id: str = None):
    """Run full analysis pipeline with error recovery."""
    if run_id is None:
        run_id = str(uuid.uuid4())

    init_analysis_table()
    conn = get_db(read_only=False)

    # Pipeline state table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_state (
            stage VARCHAR PRIMARY KEY,
            last_run VARCHAR,
            status VARCHAR,
            error_message TEXT,
            updated_at TIMESTAMP
        )
    """)

    def _set_stage(stage: str, status: str, error: str = None):
        conn.execute("""
            INSERT OR REPLACE INTO pipeline_state (stage, last_run, status, error_message, updated_at)
            VALUES (?, ?, ?, ?, ?)
        """, [stage, run_id, status, error, datetime.now(timezone.utc).isoformat()])

    topics = conn.execute("SELECT DISTINCT topic FROM records").fetchall()
    topic_names = [t[0] for t in topics]
    logger.info(f"Running analysis for {len(topic_names)} topics")

    topic_series: dict[str, np.ndarray] = {}
    topic_months: dict[str, list[str]] = {}

    # Stage 1: Per-topic analysis
    _set_stage("topic_analysis", "running")
    try:
        for topic in topic_names:
            logger.info(f"Analyzing topic: {topic}")
            months, data = get_topic_timeseries(conn, topic)
            topic_series[topic] = data
            topic_months[topic] = months

            summary = analyze_topic_summary(conn, topic)
            store_analysis(topic, "summary", summary, run_id, conn)
            store_analysis(topic, "timeseries", {"months": months, "counts": data.tolist()}, run_id, conn)
            trend = analyze_trend(data)
            store_analysis(topic, "trend", trend, run_id, conn)
            anomalies = analyze_anomalies(data)
            store_analysis(topic, "anomalies", {"indices": anomalies, "months": [months[i] for i in anomalies] if anomalies else []}, run_id, conn)
            seasonality = analyze_seasonality(data)
            store_analysis(topic, "seasonality", seasonality, run_id, conn)
        _set_stage("topic_analysis", "complete")
    except Exception as e:
        _set_stage("topic_analysis", "failed", str(e))
        logger.error(f"Topic analysis failed: {e}")
        conn.close()
        return run_id

    # Stage 2: Cross-topic correlations
    _set_stage("cross_topic", "running")
    try:
        correlations = analyze_correlations(topic_series, topic_months)
        store_analysis("cross_topic", "correlations", {"correlations": correlations}, run_id, conn)
        org_clusters = analyze_org_clusters(conn)
        store_analysis("cross_topic", "org_clusters", org_clusters, run_id, conn)
        _set_stage("cross_topic", "complete")
    except Exception as e:
        _set_stage("cross_topic", "failed", str(e))
        logger.error(f"Cross-topic analysis failed: {e}")

    conn.close()
    logger.info(f"Analysis complete, run_id: {run_id}")

    # Stage 3: Advanced analysis modules
    _set_stage_standalone("advanced_analysis", "running", run_id)
    try:
        from analysis.trend_detection import detect_trends
        from analysis.change_point_detection import detect_change_points
        from analysis.anomaly_detection import detect_anomalies
        from analysis.correlation_analysis import analyze_cross_correlations
        from analysis.association_rules import mine_association_rules
        from analysis.graph_analysis import analyze_graph

        trends = detect_trends(run_id)
        logger.info(f"STL trends: {len(trends)}")
        cps = detect_change_points(run_id)
        logger.info(f"Change points: {len(cps)}")
        anoms = detect_anomalies(run_id)
        logger.info(f"Advanced anomalies: {len(anoms)}")
        corrs = analyze_cross_correlations(run_id)
        logger.info(f"Cross-correlations: {len(corrs)}")
        rules = mine_association_rules(run_id)
        logger.info(f"Association rules: {len(rules)}")
        graph_ins = analyze_graph(run_id)
        logger.info(f"Graph communities: {len(graph_ins)}")
        _set_stage_standalone("advanced_analysis", "complete", run_id)
    except Exception as e:
        _set_stage_standalone("advanced_analysis", "failed", run_id, str(e))
        logger.error(f"Advanced analysis failed: {e}")

    # Stage 4: Insights
    _set_stage_standalone("insights", "running", run_id)
    try:
        from analysis.insights import generate_insights
        n = generate_insights(run_id)
        logger.info(f"Generated {n} insights")
        _set_stage_standalone("insights", "complete", run_id)
    except Exception as e:
        _set_stage_standalone("insights", "failed", run_id, str(e))
        logger.error(f"Insight generation failed: {e}")

    # Stage 5: Cross-topic synthesis
    _set_stage_standalone("synthesis", "running", run_id)
    try:
        from analysis.synthesis import generate_cross_topic_insights
        synth = generate_cross_topic_insights(run_id)
        logger.info(f"Generated {len(synth)} cross-topic insights")
        _set_stage_standalone("synthesis", "complete", run_id)
    except Exception as e:
        _set_stage_standalone("synthesis", "failed", run_id, str(e))
        logger.error(f"Synthesis failed: {e}")

    # Stage 6: Ranked insight feed (all insight types)
    _set_stage_standalone("ranking", "running", run_id)
    try:
        from analysis.insight_ranker import rank_insights
        all_insights = (trends or []) + (cps or []) + (anoms or []) + (corrs or []) + (rules or []) + (graph_ins or [])
        ranked = rank_insights(all_insights)
        # Store ranked feed
        import json as _json
        rconn = get_db(read_only=False)
        rconn.execute(
            "INSERT OR REPLACE INTO analysis_results (id, topic, metric, value, run_id, created_at) VALUES (?,?,?,?,?,?)",
            [f"ranked_feed_{run_id}", "cross_topic", "ranked_feed",
             _json.dumps([r for r in ranked[:100]]), run_id, datetime.now(timezone.utc).isoformat()],
        )
        rconn.close()
        logger.info(f"Ranked {len(ranked)} insights")
        _set_stage_standalone("ranking", "complete", run_id)
    except Exception as e:
        _set_stage_standalone("ranking", "failed", run_id, str(e))
        logger.error(f"Ranking failed: {e}")

    # Stage 7: Stories
    _set_stage_standalone("stories", "running", run_id)
    try:
        from stories.narrator import generate_stories
        generate_stories(run_id)
        logger.info("Stories generated")
        _set_stage_standalone("stories", "complete", run_id)
    except Exception as e:
        _set_stage_standalone("stories", "failed", run_id, str(e))
        logger.error(f"Story generation failed: {e}")

    return run_id


def _set_stage_standalone(stage: str, status: str, run_id: str, error: str = None):
    """Update pipeline state with a fresh connection (for stages after conn.close)."""
    conn = get_db(read_only=False)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_state (
            stage VARCHAR PRIMARY KEY, last_run VARCHAR, status VARCHAR,
            error_message TEXT, updated_at TIMESTAMP
        )
    """)
    conn.execute("""
        INSERT OR REPLACE INTO pipeline_state (stage, last_run, status, error_message, updated_at)
        VALUES (?, ?, ?, ?, ?)
    """, [stage, run_id, status, error, datetime.now(timezone.utc).isoformat()])
    conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_analysis()
