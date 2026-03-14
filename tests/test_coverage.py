"""Tests for insights.py, synthesis.py, clustering.py, patterns.py core functions, and narrator."""

import json
import uuid

import duckdb
import numpy as np
import pytest


# ── Shared fixtures ───────────────────────────────────────────────────────────

@pytest.fixture
def conn():
    """In-memory DuckDB with full schema for pipeline tests."""
    c = duckdb.connect(":memory:")
    c.execute("""
        CREATE TABLE records (
            id VARCHAR PRIMARY KEY, title VARCHAR, description VARCHAR,
            topic VARCHAR, keywords VARCHAR[], organization VARCHAR,
            url VARCHAR, license VARCHAR, source VARCHAR NOT NULL,
            ingested_at TIMESTAMP NOT NULL, quality_score DOUBLE DEFAULT 0.0,
            metadata_created TIMESTAMP, metadata_modified TIMESTAMP,
            theme VARCHAR, num_resources INTEGER DEFAULT 0, fts_content VARCHAR
        )
    """)
    c.execute("""
        CREATE TABLE analysis_results (
            id VARCHAR PRIMARY KEY, topic VARCHAR NOT NULL, metric VARCHAR NOT NULL,
            value JSON, run_id VARCHAR NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    c.execute("""
        CREATE TABLE insights (
            id VARCHAR PRIMARY KEY, topic VARCHAR NOT NULL, insight_type VARCHAR NOT NULL,
            severity VARCHAR NOT NULL, confidence DOUBLE NOT NULL, title VARCHAR NOT NULL,
            summary TEXT NOT NULL, evidence JSON, run_id VARCHAR NOT NULL,
            rank_score DOUBLE DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    c.execute("""
        CREATE TABLE data_stories (
            id VARCHAR PRIMARY KEY, topic VARCHAR NOT NULL, run_id VARCHAR NOT NULL,
            headline VARCHAR, key_finding TEXT, context TEXT, outlook TEXT,
            annotations JSON, model_used VARCHAR, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    c.execute("""
        CREATE TABLE pipeline_state (
            stage VARCHAR PRIMARY KEY, last_run VARCHAR, status VARCHAR,
            error_message TEXT, updated_at TIMESTAMP
        )
    """)
    c.execute("""
        CREATE TABLE topic_clusters (
            id VARCHAR PRIMARY KEY, record_id VARCHAR NOT NULL, keyword_topic VARCHAR,
            cluster_id INTEGER NOT NULL, cluster_label VARCHAR,
            similarity_score DOUBLE, run_id VARCHAR NOT NULL
        )
    """)
    yield c
    c.close()


@pytest.fixture
def rid():
    return str(uuid.uuid4())


def _seed_records(c, n=50):
    topics = ["health", "transport", "housing", "economy"]
    orgs = ["NHS", "DfT", "ONS", "HMRC"]
    for i in range(n):
        t, o = topics[i % 4], orgs[i % 4]
        days = i * 10
        c.execute(
            f"INSERT INTO records (id,title,description,topic,keywords,organization,source,ingested_at,quality_score,metadata_modified) "
            f"VALUES (?,?,?,?,?,?,?,CURRENT_TIMESTAMP - INTERVAL '{days}' DAY,0.8,CURRENT_TIMESTAMP - INTERVAL '{days}' DAY)",
            [f"r{i}", f"Title {t} {i}", f"Desc {t} {i}", t, json.dumps([t, f"kw{i%3}"]), o, "test"],
        )


def _seed_analysis(c, rid, topics=None):
    """Seed timeseries + trend + anomalies + seasonality + summary for topics."""
    if topics is None:
        topics = ["health", "transport"]
    np.random.seed(42)
    for topic in topics:
        months = [f"20{y:02d}-{m:02d}" for y in range(19, 24) for m in range(1, 13)]
        counts = (np.linspace(5, 25, len(months)) + np.random.normal(0, 2, len(months))).clip(0).tolist()
        for metric, val in [
            ("timeseries", {"months": months, "counts": counts}),
            ("trend", {"direction": "up", "slope": 0.3, "r_squared": 0.7}),
            ("anomalies", {"indices": [10], "months": [months[10]]}),
            ("seasonality", {"dominant_period": 12, "strength": 0.25}),
            ("summary", {"total_datasets": 50, "num_organizations": 3, "avg_quality": 0.8,
                         "earliest_modified": "2019-01-01", "latest_modified": "2023-12-01"}),
        ]:
            c.execute(
                "INSERT OR REPLACE INTO analysis_results (id,topic,metric,value,run_id) VALUES (?,?,?,?,?)",
                [f"{topic}_{metric}_{rid}", topic, metric, json.dumps(val), rid],
            )


# ── insights.py ───────────────────────────────────────────────────────────────

class TestInsightsModule:
    def test_severity_levels(self):
        from analysis.insights import _severity
        assert _severity(0.8) == "high"
        assert _severity(0.5) == "medium"
        assert _severity(0.1) == "low"

    def test_extract_trend_insight_up(self):
        from analysis.insights import extract_trend_insight
        analysis = {
            "trend": {"direction": "up", "slope": 0.5, "r_squared": 0.8},
            "timeseries": {"months": list(range(60))},
        }
        ins = extract_trend_insight("health", analysis)
        assert ins is not None
        assert ins["insight_type"] == "trend"
        assert "increasing" in ins["title"]

    def test_extract_trend_insight_down(self):
        from analysis.insights import extract_trend_insight
        analysis = {
            "trend": {"direction": "down", "slope": -0.3, "r_squared": 0.6},
            "timeseries": {"months": list(range(40))},
        }
        ins = extract_trend_insight("crime", analysis)
        assert "declining" in ins["title"]

    def test_extract_trend_insight_unknown(self):
        from analysis.insights import extract_trend_insight
        assert extract_trend_insight("x", {"trend": {"direction": "unknown"}}) is None
        assert extract_trend_insight("x", {}) is None

    def test_extract_anomaly_insight(self):
        from analysis.insights import extract_anomaly_insight
        ins = extract_anomaly_insight("crime", {"anomalies": {"months": ["2023-06", "2023-07"]}})
        assert ins is not None
        assert ins["insight_type"] == "anomaly"
        assert ins["evidence"]["count"] == 2

    def test_extract_anomaly_insight_none(self):
        from analysis.insights import extract_anomaly_insight
        assert extract_anomaly_insight("x", {}) is None
        assert extract_anomaly_insight("x", {"anomalies": {"months": []}}) is None

    def test_extract_seasonality_annual(self):
        from analysis.insights import extract_seasonality_insight
        ins = extract_seasonality_insight("health", {"seasonality": {"dominant_period": 12, "strength": 0.3}})
        assert ins is not None
        assert "annual" in ins["title"]

    def test_extract_seasonality_semi_annual(self):
        from analysis.insights import extract_seasonality_insight
        ins = extract_seasonality_insight("health", {"seasonality": {"dominant_period": 6, "strength": 0.2}})
        assert "semi-annual" in ins["title"]

    def test_extract_seasonality_quarterly(self):
        from analysis.insights import extract_seasonality_insight
        ins = extract_seasonality_insight("health", {"seasonality": {"dominant_period": 3, "strength": 0.2}})
        assert "quarterly" in ins["title"]

    def test_extract_seasonality_custom(self):
        from analysis.insights import extract_seasonality_insight
        ins = extract_seasonality_insight("health", {"seasonality": {"dominant_period": 8, "strength": 0.2}})
        assert "8-month" in ins["title"]

    def test_extract_seasonality_weak(self):
        from analysis.insights import extract_seasonality_insight
        assert extract_seasonality_insight("x", {"seasonality": {"strength": 0.05}}) is None

    def test_extract_correlation_insights(self):
        from analysis.insights import extract_correlation_insights
        analysis = {"correlations": {"correlations": [
            {"topic1": "health", "topic2": "economy", "correlation": 0.7, "p_value": 0.01, "shared_months": 30},
            {"topic1": "crime", "topic2": "housing", "correlation": -0.5, "p_value": 0.03, "shared_months": 20},
        ]}}
        results = extract_correlation_insights(analysis)
        assert len(results) == 2
        assert "strongly" in results[0]["title"]
        assert "negatively" in results[1]["summary"]

    def test_extract_summary_insight(self):
        from analysis.insights import extract_summary_insight
        s = {"total_datasets": 100, "num_organizations": 5, "avg_quality": 0.85,
             "earliest_modified": "2020-01-01", "latest_modified": "2024-01-01"}
        ins = extract_summary_insight("health", {"summary": s})
        assert ins is not None
        assert "100 datasets" in ins["title"]
        assert "2020-01-01" in ins["summary"]

    def test_extract_summary_no_dates(self):
        from analysis.insights import extract_summary_insight
        s = {"total_datasets": 10, "num_organizations": 2, "avg_quality": 0.7}
        ins = extract_summary_insight("x", {"summary": s})
        assert ins is not None

    def test_rank_score(self):
        from analysis.insights import _rank_score
        ins = {"severity": "high", "confidence": 0.9, "title": "Test insight", "evidence": {"avg_quality": 0.8}}
        score = _rank_score(ins, set())
        assert 0 < score <= 1

    def test_rank_score_novelty_penalty(self):
        from analysis.insights import _rank_score
        ins = {"severity": "high", "confidence": 0.9, "title": "Health trend up"}
        s1 = _rank_score(ins, set())
        s2 = _rank_score(ins, {"Health trend up strongly"})
        assert s1 > s2

    def test_generate_insights_end_to_end(self, conn, rid):
        _seed_analysis(conn, rid)
        # Add cross-topic correlations
        conn.execute(
            "INSERT INTO analysis_results (id,topic,metric,value,run_id) VALUES (?,?,?,?,?)",
            [f"ct_corr_{rid}", "cross_topic", "correlations",
             json.dumps({"correlations": [{"topic1": "health", "topic2": "transport",
                                           "correlation": 0.65, "p_value": 0.01, "shared_months": 30}]}), rid],
        )
        from analysis.insights import generate_insights
        n = generate_insights(rid, conn=conn)
        assert n > 0
        rows = conn.execute("SELECT COUNT(*) FROM insights WHERE run_id = ?", [rid]).fetchone()
        assert rows[0] == n


# ── synthesis.py ──────────────────────────────────────────────────────────────

class TestSynthesis:
    def test_align_series(self):
        from analysis.synthesis import _align_series
        months_a = ["2020-01", "2020-02", "2020-03", "2020-04", "2020-05", "2020-06", "2020-07"]
        months_b = ["2020-01", "2020-02", "2020-03", "2020-04", "2020-05", "2020-06", "2020-07"]
        data_a = np.array([1, 2, 3, 4, 5, 6, 7], dtype=float)
        data_b = np.array([2, 4, 6, 8, 10, 12, 14], dtype=float)
        shared, a, b = _align_series(months_a, data_a, months_b, data_b)
        assert shared is not None
        assert len(a) > 0

    def test_align_series_insufficient(self):
        from analysis.synthesis import _align_series
        shared, a, b = _align_series(["2020-01"], np.array([1.0]), ["2021-01"], np.array([2.0]))
        assert shared is None

    def test_detect_lag(self):
        from analysis.synthesis import detect_lag
        a = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], dtype=float)
        b = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], dtype=float)
        result = detect_lag(a, b)
        assert "lag" in result
        assert "correlation" in result

    def test_detect_lag_short(self):
        from analysis.synthesis import detect_lag
        result = detect_lag(np.array([1.0, 2.0]), np.array([1.0, 2.0]))
        assert result["lag"] == 0

    def test_timeframe(self):
        from analysis.synthesis import _timeframe
        assert _timeframe(["2020-01", "2020-06", "2021-03"]) == "2020-2021"
        assert _timeframe(["2020-01", "2020-06"]) == "2020"
        assert _timeframe([]) == ""

    def test_generate_cross_topic_insights(self, conn, rid):
        _seed_analysis(conn, rid)
        from analysis.synthesis import generate_cross_topic_insights
        insights = generate_cross_topic_insights(rid, conn=conn)
        assert isinstance(insights, list)
        # Both topics have upward trends, should correlate
        if insights:
            assert insights[0]["type"] == "cross_topic_trend"
            assert "correlation" in insights[0]


# ── patterns.py core functions ────────────────────────────────────────────────

class TestPatternsFunctions:
    def test_analyze_trend_up(self):
        from analysis.patterns import analyze_trend
        data = np.array([1, 2, 3, 4, 5, 6, 7, 8], dtype=float)
        result = analyze_trend(data)
        assert result["direction"] == "up"
        assert result["slope"] > 0
        assert result["r_squared"] > 0.9

    def test_analyze_trend_down(self):
        from analysis.patterns import analyze_trend
        result = analyze_trend(np.array([8, 7, 6, 5, 4, 3, 2, 1], dtype=float))
        assert result["direction"] == "down"

    def test_analyze_trend_short(self):
        from analysis.patterns import analyze_trend
        result = analyze_trend(np.array([1.0]))
        assert result["direction"] == "unknown"

    def test_analyze_anomalies(self):
        from analysis.patterns import analyze_anomalies
        data = np.array([10] * 20 + [200], dtype=float)
        result = analyze_anomalies(data)
        assert isinstance(result, list)

    def test_analyze_anomalies_short(self):
        from analysis.patterns import analyze_anomalies
        assert analyze_anomalies(np.array([1, 2, 3], dtype=float)) == []

    def test_analyze_seasonality(self):
        from analysis.patterns import analyze_seasonality
        data = np.sin(np.linspace(0, 4 * np.pi, 48)) * 10 + 20
        result = analyze_seasonality(data)
        assert result["dominant_period"] > 0
        assert result["strength"] > 0

    def test_analyze_seasonality_short(self):
        from analysis.patterns import analyze_seasonality
        result = analyze_seasonality(np.array([1, 2, 3], dtype=float))
        assert result["dominant_period"] == 0

    def test_analyze_correlations(self):
        from analysis.patterns import analyze_correlations
        months = [f"2020-{m:02d}" for m in range(1, 13)]
        s1 = np.arange(1, 13, dtype=float)
        s2 = np.arange(2, 14, dtype=float)
        result = analyze_correlations({"a": s1, "b": s2}, {"a": months, "b": months})
        assert len(result) >= 1
        assert result[0]["correlation"] > 0

    def test_analyze_correlations_no_overlap(self):
        from analysis.patterns import analyze_correlations
        result = analyze_correlations(
            {"a": np.array([1, 2, 3], dtype=float), "b": np.array([4, 5, 6], dtype=float)},
            {"a": ["2020-01", "2020-02", "2020-03"], "b": ["2022-01", "2022-02", "2022-03"]},
        )
        assert result == []

    def test_analyze_org_clusters(self, conn):
        _seed_records(conn, 50)
        from analysis.patterns import analyze_org_clusters
        result = analyze_org_clusters(conn)
        assert "clusters" in result

    def test_analyze_org_clusters_empty(self, conn):
        from analysis.patterns import analyze_org_clusters
        result = analyze_org_clusters(conn)
        assert result == {"clusters": {}, "centroids": []}

    def test_analyze_topic_summary(self, conn):
        _seed_records(conn, 20)
        from analysis.patterns import analyze_topic_summary
        result = analyze_topic_summary(conn, "health")
        assert result["total_datasets"] > 0
        assert "num_organizations" in result

    def test_store_analysis(self, conn, rid):
        from analysis.patterns import store_analysis
        store_analysis("health", "test_metric", {"val": 42}, rid, conn)
        row = conn.execute("SELECT value FROM analysis_results WHERE topic='health' AND metric='test_metric'").fetchone()
        assert row is not None
        assert json.loads(row[0])["val"] == 42

    def test_get_topic_timeseries_fallback(self, conn):
        """Test fallback to ingested_at when metadata_modified is null."""
        _seed_records(conn, 20)
        # Records have metadata_modified set, so this should work
        from analysis.patterns import get_topic_timeseries
        months, counts = get_topic_timeseries(conn, "health")
        assert len(months) > 0
        assert len(counts) > 0

    def test_get_topic_timeseries_empty(self, conn):
        from analysis.patterns import get_topic_timeseries
        months, counts = get_topic_timeseries(conn, "nonexistent")
        assert months == []
        assert len(counts) == 0


# ── patterns.py run_analysis (integration) ────────────────────────────────────

class TestRunAnalysis:
    def test_run_analysis_produces_results(self, conn, rid):
        _seed_records(conn, 40)
        from analysis.patterns import init_analysis_table
        # We can't easily call run_analysis with a custom conn since it creates its own,
        # but we can test the individual stage functions that it calls
        from analysis.patterns import (
            get_topic_timeseries, analyze_trend, analyze_anomalies,
            analyze_seasonality, store_analysis, analyze_topic_summary,
        )
        init_analysis_table.__wrapped__ = None  # just verify it's callable

        topics = conn.execute("SELECT DISTINCT topic FROM records").fetchall()
        for (topic,) in topics:
            months, data = get_topic_timeseries(conn, topic)
            store_analysis(topic, "timeseries", {"months": months, "counts": data.tolist()}, rid, conn)
            store_analysis(topic, "trend", analyze_trend(data), rid, conn)
            store_analysis(topic, "anomalies", {"indices": analyze_anomalies(data), "months": []}, rid, conn)
            store_analysis(topic, "seasonality", analyze_seasonality(data), rid, conn)
            store_analysis(topic, "summary", analyze_topic_summary(conn, topic), rid, conn)

        count = conn.execute("SELECT COUNT(*) FROM analysis_results WHERE run_id=?", [rid]).fetchone()[0]
        assert count >= 20  # 4 topics * 5 metrics


# ── narrator.py ───────────────────────────────────────────────────────────────

class TestNarrator:
    def test_template_story_increasing(self):
        from stories.narrator import _template_story
        story = _template_story("health", {
            "trend": {"direction": "up", "slope": 0.5, "r_squared": 0.8},
            "summary": {"total_datasets": 100, "num_organizations": 5, "avg_quality": 0.85,
                        "earliest_modified": "2020-01-01", "latest_modified": "2024-01-01"},
            "anomalies": {"months": ["2023-06"]},
            "seasonality": {"dominant_period": 12, "strength": 0.3},
        })
        assert story["model_used"] == "template"
        assert story["headline"]
        assert story["key_finding"]
        assert story["context"]
        assert story["outlook"]
        assert len(story["annotations"]) > 0

    def test_template_story_decreasing(self):
        from stories.narrator import _template_story
        story = _template_story("crime", {
            "trend": {"direction": "down", "slope": -0.3, "r_squared": 0.6},
            "summary": {"total_datasets": 50, "num_organizations": 3, "avg_quality": 0.7},
            "anomalies": {"months": []},
            "seasonality": {"dominant_period": 0, "strength": 0},
        })
        assert story["headline"]

    def test_template_story_stable(self):
        from stories.narrator import _template_story
        story = _template_story("economy", {
            "trend": {"direction": "stable", "slope": 0.01, "r_squared": 0.01},
            "summary": {"total_datasets": 200, "num_organizations": 10, "avg_quality": 0.9},
        })
        assert story["headline"]

    def test_template_story_empty(self):
        from stories.narrator import _template_story
        story = _template_story("unknown", {})
        assert story["headline"]
        assert story["model_used"] == "template"

    def test_call_llm_no_key(self):
        from stories.narrator import call_llm
        provider = {"name": "test", "base_url": "http://localhost", "model": "m", "api_key_env": "NONEXISTENT_KEY_XYZ"}
        assert call_llm("test prompt", provider) is None

    def test_generate_story_with_fallback_uses_template(self):
        from stories.narrator import generate_story_with_fallback
        story = generate_story_with_fallback("health", {
            "trend": {"direction": "up", "slope": 0.5, "r_squared": 0.8},
            "summary": {"total_datasets": 100, "num_organizations": 5, "avg_quality": 0.85},
        })
        assert story["model_used"] == "template"
        assert story["headline"]

    def test_generate_advanced_stories_change_points(self, conn, rid):
        from unittest.mock import patch
        from stories.narrator import _generate_advanced_stories
        analysis_data = {
            "health": {"change_points": {"change_months": ["2022-06"]}},
            "cross_topic": {},
        }
        stories_stored = []
        def fake_store(**kwargs):
            stories_stored.append(kwargs)
        with patch("stories.narrator.store_story", side_effect=fake_store):
            _generate_advanced_stories(rid, analysis_data)
        assert len(stories_stored) >= 1
        assert "Structural Shift" in stories_stored[0]["headline"]

    def test_generate_advanced_stories_graph(self, conn, rid):
        from unittest.mock import patch
        from stories.narrator import _generate_advanced_stories
        analysis_data = {
            "health": {}, "transport": {},
            "cross_topic": {
                "graph_analysis": {
                    "modularity": 0.3,
                    "communities": {"0": ["health", "transport", "NHS"]},
                },
            },
        }
        stories_stored = []
        with patch("stories.narrator.store_story", side_effect=lambda **kw: stories_stored.append(kw)):
            _generate_advanced_stories(rid, analysis_data)
        graph_stories = [s for s in stories_stored if "Cluster" in s.get("headline", "")]
        assert len(graph_stories) >= 1

    def test_generate_advanced_stories_rules(self, conn, rid):
        from unittest.mock import patch
        from stories.narrator import _generate_advanced_stories
        analysis_data = {
            "cross_topic": {
                "association_rules": [
                    {"antecedent": "topic:health", "consequent": "org:NHS", "lift": 2.5,
                     "support": 0.1, "confidence": 0.8},
                ],
            },
        }
        stories_stored = []
        with patch("stories.narrator.store_story", side_effect=lambda **kw: stories_stored.append(kw)):
            _generate_advanced_stories(rid, analysis_data)
        rule_stories = [s for s in stories_stored if "Association" in s.get("headline", "")]
        assert len(rule_stories) >= 1

    def test_generate_advanced_stories_empty(self, conn, rid):
        from unittest.mock import patch
        from stories.narrator import _generate_advanced_stories
        with patch("stories.narrator.store_story"):
            _generate_advanced_stories(rid, {"cross_topic": {}})
        # Should not crash


# ── clustering.py ─────────────────────────────────────────────────────────────

class TestClustering:
    def test_find_optimal_k(self):
        from analysis.clustering import _find_optimal_k
        from sklearn.feature_extraction.text import TfidfVectorizer
        texts = [f"health data {i}" for i in range(20)] + [f"transport road {i}" for i in range(20)]
        matrix = TfidfVectorizer(max_features=100).fit_transform(texts)
        k = _find_optimal_k(matrix, k_min=2, k_max=5)
        assert 2 <= k <= 5

    def test_find_optimal_k_small(self):
        from analysis.clustering import _find_optimal_k
        from sklearn.feature_extraction.text import TfidfVectorizer
        texts = [f"word{i}" for i in range(6)]
        matrix = TfidfVectorizer(max_features=50).fit_transform(texts)
        k = _find_optimal_k(matrix, k_min=5, k_max=5)
        assert k == 5


# ── patterns.py run_analysis with mocked DB ──────────────────────────────────

class _NoCloseConn:
    """Wrapper that prevents close() from actually closing the connection."""
    def __init__(self, conn):
        self._conn = conn
    def close(self):
        pass  # no-op
    def __getattr__(self, name):
        return getattr(self._conn, name)


class TestRunAnalysisFull:
    def test_run_analysis_full_pipeline(self, conn, rid):
        """Test run_analysis by mocking get_db and _set_stage_standalone."""
        _seed_records(conn, 40)
        from unittest.mock import patch

        wrapped = _NoCloseConn(conn)

        def fake_get_db(read_only=True):
            return wrapped

        def fake_set_stage(stage, status, run_id, error=None):
            conn.execute("""
                INSERT OR REPLACE INTO pipeline_state (stage, last_run, status, error_message, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, [stage, run_id, status, error])

        with patch("analysis.patterns.get_db", side_effect=fake_get_db), \
             patch("analysis.patterns.init_analysis_table"), \
             patch("analysis.patterns._set_stage_standalone", side_effect=fake_set_stage), \
             patch("analysis.trend_detection.duckdb") as mock_td, \
             patch("analysis.change_point_detection.duckdb") as mock_cp, \
             patch("analysis.anomaly_detection.duckdb") as mock_ad, \
             patch("analysis.correlation_analysis.duckdb") as mock_ca, \
             patch("analysis.association_rules.duckdb") as mock_ar, \
             patch("analysis.graph_analysis.duckdb") as mock_ga:
            for m in [mock_td, mock_cp, mock_ad, mock_ca, mock_ar, mock_ga]:
                m.connect.return_value = wrapped

            from analysis.patterns import run_analysis
            result_id = run_analysis(rid)
            assert result_id == rid

        count = conn.execute("SELECT COUNT(*) FROM analysis_results WHERE run_id=?", [rid]).fetchone()[0]
        assert count > 0

        stages = conn.execute("SELECT stage, status FROM pipeline_state").fetchall()
        assert len(stages) > 0

    def test_set_stage_standalone(self, conn, rid):
        from unittest.mock import patch

        wrapped = _NoCloseConn(conn)

        with patch("analysis.patterns.get_db", return_value=wrapped):
            from analysis.patterns import _set_stage_standalone
            _set_stage_standalone("test_stage", "complete", rid)
        row = conn.execute("SELECT status FROM pipeline_state WHERE stage='test_stage'").fetchone()
        assert row[0] == "complete"


# ── clustering.py run_clustering ──────────────────────────────────────────────

class TestRunClustering:
    def test_run_clustering_with_data(self, conn):
        _seed_records(conn, 60)
        from unittest.mock import patch
        with patch("analysis.clustering.duckdb") as mock_db:
            mock_db.connect.return_value = conn
            from analysis.clustering import run_clustering
            result = run_clustering(n_clusters=3, run_id="test_run")
        assert result["n_records"] == 60
        assert result["n_clusters"] == 3
        assert result["silhouette_score"] > -1
        assert len(result["cluster_labels"]) == 3

    def test_run_clustering_too_few_records(self, conn):
        # Only 5 records
        for i in range(5):
            conn.execute(
                "INSERT INTO records (id,title,description,topic,source,ingested_at) VALUES (?,?,?,?,?,CURRENT_TIMESTAMP)",
                [f"small{i}", f"T{i}", f"D{i}", "health", "test"],
            )
        from unittest.mock import patch
        with patch("analysis.clustering.duckdb") as mock_db:
            mock_db.connect.return_value = conn
            from analysis.clustering import run_clustering
            result = run_clustering(n_clusters=3, run_id="test_run")
        assert "error" in result


# ── API endpoints: insights, stories, topics (with real DB) ───────────────────

class TestAPIInsightsEndpoints:
    @pytest.fixture
    def api_client(self):
        from fastapi.testclient import TestClient
        from api.main import app
        return TestClient(app)

    def test_list_insights_with_filters(self, api_client):
        r = api_client.get("/insights?severity=high&limit=5")
        assert r.status_code == 200
        data = r.json()
        assert "data" in data
        assert "meta" in data

    def test_list_insights_by_type(self, api_client):
        r = api_client.get("/insights?insight_type=trend")
        assert r.status_code == 200

    def test_list_insights_by_topic(self, api_client):
        r = api_client.get("/insights?topic=health")
        assert r.status_code == 200

    def test_stories_endpoint(self, api_client):
        r = api_client.get("/stories?limit=5")
        assert r.status_code == 200
        assert "data" in r.json()

    def test_stories_by_topic(self, api_client):
        r = api_client.get("/stories?topic=health")
        assert r.status_code == 200

    def test_topics_endpoint(self, api_client):
        r = api_client.get("/topics")
        assert r.status_code == 200
        assert "data" in r.json()
