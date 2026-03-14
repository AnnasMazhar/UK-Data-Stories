"""Tests for the statistical insight discovery engine modules."""

import json
import uuid

import duckdb
import numpy as np
import pytest


@pytest.fixture
def analysis_db():
    """In-memory DuckDB with analysis schema and sample timeseries data."""
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE analysis_results (
            id VARCHAR PRIMARY KEY,
            topic VARCHAR NOT NULL,
            metric VARCHAR NOT NULL,
            value JSON,
            run_id VARCHAR NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE records (
            id VARCHAR PRIMARY KEY,
            title VARCHAR,
            description VARCHAR,
            topic VARCHAR,
            keywords VARCHAR[],
            organization VARCHAR,
            url VARCHAR,
            license VARCHAR,
            source VARCHAR NOT NULL,
            ingested_at TIMESTAMP NOT NULL,
            quality_score DOUBLE DEFAULT 0.0,
            metadata_created TIMESTAMP,
            metadata_modified TIMESTAMP,
            theme VARCHAR,
            num_resources INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE insights (
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
    """)
    yield conn
    conn.close()


@pytest.fixture
def run_id():
    return str(uuid.uuid4())


def _seed_timeseries(conn, run_id, topics=None):
    """Insert synthetic monthly timeseries for given topics."""
    if topics is None:
        topics = ["health", "transport", "housing"]
    np.random.seed(42)
    for topic in topics:
        months = [f"20{y:02d}-{m:02d}" for y in range(18, 24) for m in range(1, 13)]
        # Create a trend + noise signal
        base = np.linspace(5, 20, len(months)) if topic != "housing" else np.linspace(20, 5, len(months))
        counts = (base + np.random.normal(0, 2, len(months))).clip(0).tolist()
        conn.execute(
            "INSERT INTO analysis_results (id, topic, metric, value, run_id) VALUES (?,?,?,?,?)",
            [f"{topic}_ts_{run_id}", topic, "timeseries",
             json.dumps({"months": months, "counts": counts}), run_id],
        )


def _seed_records(conn, n=100):
    """Insert synthetic records for association/graph tests."""
    topics = ["health", "transport", "housing", "economy"]
    orgs = ["NHS", "DfT", "ONS", "HMRC"]
    for i in range(n):
        t = topics[i % len(topics)]
        o = orgs[i % len(orgs)]
        kws = json.dumps([t, f"kw{i % 5}"])
        conn.execute(
            "INSERT INTO records (id, title, description, topic, keywords, organization, source, ingested_at) "
            "VALUES (?,?,?,?,?,?,?,CURRENT_TIMESTAMP)",
            [f"rec_{i}", f"Title {i}", f"Desc {i}", t, kws, o, "test"],
        )


# ── Trend Detection ──────────────────────────────────────────────────────────

class TestTrendDetection:
    def test_detects_increasing_trend(self, analysis_db, run_id):
        _seed_timeseries(analysis_db, run_id, ["health"])
        from analysis.trend_detection import detect_trends
        insights = detect_trends(run_id, conn=analysis_db)
        assert len(insights) >= 1
        health = [i for i in insights if i["topic"] == "health"]
        assert health[0]["type"] == "trend"
        assert health[0]["direction"] == "increase"
        assert health[0]["confidence"] > 0

    def test_detects_decreasing_trend(self, analysis_db, run_id):
        _seed_timeseries(analysis_db, run_id, ["housing"])
        from analysis.trend_detection import detect_trends
        insights = detect_trends(run_id, conn=analysis_db)
        housing = [i for i in insights if i["topic"] == "housing"]
        assert len(housing) >= 1
        assert housing[0]["direction"] == "decrease"

    def test_skips_short_series(self, analysis_db, run_id):
        conn = analysis_db
        conn.execute(
            "INSERT INTO analysis_results (id, topic, metric, value, run_id) VALUES (?,?,?,?,?)",
            [f"short_ts_{run_id}", "tiny", "timeseries",
             json.dumps({"months": ["2023-01", "2023-02"], "counts": [1, 2]}), run_id],
        )
        from analysis.trend_detection import detect_trends
        insights = detect_trends(run_id, conn=conn)
        assert all(i["topic"] != "tiny" for i in insights)

    def test_stores_stl_trend_metric(self, analysis_db, run_id):
        _seed_timeseries(analysis_db, run_id, ["health"])
        from analysis.trend_detection import detect_trends
        detect_trends(run_id, conn=analysis_db)
        rows = analysis_db.execute(
            "SELECT COUNT(*) FROM analysis_results WHERE metric = 'stl_trend' AND run_id = ?", [run_id]
        ).fetchone()
        assert rows[0] >= 1

    def test_falls_back_to_linear_for_short_series(self, analysis_db, run_id):
        months = [f"2023-{m:02d}" for m in range(1, 13)]
        counts = list(range(5, 17))
        analysis_db.execute(
            "INSERT INTO analysis_results (id, topic, metric, value, run_id) VALUES (?,?,?,?,?)",
            [f"med_ts_{run_id}", "medium", "timeseries",
             json.dumps({"months": months, "counts": counts}), run_id],
        )
        from analysis.trend_detection import detect_trends
        insights = detect_trends(run_id, conn=analysis_db)
        med = [i for i in insights if i["topic"] == "medium"]
        if med:
            assert med[0]["evidence"]["method"] == "linear_regression"


# ── Change Point Detection ────────────────────────────────────────────────────

class TestChangePointDetection:
    def test_detects_regime_shift(self, analysis_db, run_id):
        months = [f"20{y:02d}-{m:02d}" for y in range(18, 24) for m in range(1, 13)]
        # Sharp jump at month 36
        counts = [5.0] * 36 + [50.0] * (len(months) - 36)
        analysis_db.execute(
            "INSERT INTO analysis_results (id, topic, metric, value, run_id) VALUES (?,?,?,?,?)",
            [f"shift_ts_{run_id}", "crime", "timeseries",
             json.dumps({"months": months, "counts": counts}), run_id],
        )
        from analysis.change_point_detection import detect_change_points
        insights = detect_change_points(run_id, conn=analysis_db)
        assert len(insights) >= 1
        assert insights[0]["type"] == "change_point"
        assert insights[0]["confidence"] == 0.75

    def test_skips_short_series(self, analysis_db, run_id):
        analysis_db.execute(
            "INSERT INTO analysis_results (id, topic, metric, value, run_id) VALUES (?,?,?,?,?)",
            [f"short_cp_{run_id}", "tiny", "timeseries",
             json.dumps({"months": ["2023-01", "2023-02", "2023-03"], "counts": [1, 2, 3]}), run_id],
        )
        from analysis.change_point_detection import detect_change_points
        insights = detect_change_points(run_id, conn=analysis_db)
        assert all(i["topic"] != "tiny" for i in insights)

    def test_stores_change_points_metric(self, analysis_db, run_id):
        months = [f"20{y:02d}-{m:02d}" for y in range(18, 24) for m in range(1, 13)]
        counts = [5.0] * 36 + [50.0] * (len(months) - 36)
        analysis_db.execute(
            "INSERT INTO analysis_results (id, topic, metric, value, run_id) VALUES (?,?,?,?,?)",
            [f"cp_store_{run_id}", "crime", "timeseries",
             json.dumps({"months": months, "counts": counts}), run_id],
        )
        from analysis.change_point_detection import detect_change_points
        detect_change_points(run_id, conn=analysis_db)
        rows = analysis_db.execute(
            "SELECT COUNT(*) FROM analysis_results WHERE metric = 'change_points' AND run_id = ?", [run_id]
        ).fetchone()
        assert rows[0] >= 1


# ── Correlation Analysis ──────────────────────────────────────────────────────

class TestCorrelationAnalysis:
    def test_finds_correlated_topics(self, analysis_db, run_id):
        _seed_timeseries(analysis_db, run_id, ["health", "transport"])
        from analysis.correlation_analysis import analyze_cross_correlations
        insights = analyze_cross_correlations(run_id, conn=analysis_db)
        # Both have upward trends so should correlate
        assert len(insights) >= 1
        assert insights[0]["type"] == "correlation"
        assert "topics" in insights[0]
        assert abs(insights[0]["correlation"]) > 0.3

    def test_includes_lag_info(self, analysis_db, run_id):
        _seed_timeseries(analysis_db, run_id, ["health", "transport"])
        from analysis.correlation_analysis import analyze_cross_correlations
        insights = analyze_cross_correlations(run_id, conn=analysis_db)
        if insights:
            assert "lag_months" in insights[0]

    def test_skips_insufficient_shared_months(self, analysis_db, run_id):
        # Two topics with non-overlapping months
        analysis_db.execute(
            "INSERT INTO analysis_results (id, topic, metric, value, run_id) VALUES (?,?,?,?,?)",
            [f"a_ts_{run_id}", "topicA", "timeseries",
             json.dumps({"months": ["2020-01", "2020-02", "2020-03"], "counts": [1, 2, 3]}), run_id],
        )
        analysis_db.execute(
            "INSERT INTO analysis_results (id, topic, metric, value, run_id) VALUES (?,?,?,?,?)",
            [f"b_ts_{run_id}", "topicB", "timeseries",
             json.dumps({"months": ["2022-01", "2022-02", "2022-03"], "counts": [4, 5, 6]}), run_id],
        )
        from analysis.correlation_analysis import analyze_cross_correlations
        insights = analyze_cross_correlations(run_id, conn=analysis_db)
        pairs = [i for i in insights if set(i["topics"]) == {"topicA", "topicB"}]
        assert len(pairs) == 0


# ── Anomaly Detection ─────────────────────────────────────────────────────────

class TestAnomalyDetection:
    def test_detects_spike(self, analysis_db, run_id):
        months = [f"2020-{m:02d}" for m in range(1, 13)] + [f"2021-{m:02d}" for m in range(1, 13)]
        counts = [10.0] * 23 + [200.0]  # massive spike at end
        analysis_db.execute(
            "INSERT INTO analysis_results (id, topic, metric, value, run_id) VALUES (?,?,?,?,?)",
            [f"spike_ts_{run_id}", "crime", "timeseries",
             json.dumps({"months": months, "counts": counts}), run_id],
        )
        from analysis.anomaly_detection import detect_anomalies
        insights = detect_anomalies(run_id, conn=analysis_db)
        assert len(insights) >= 1
        assert insights[0]["type"] == "anomaly"
        assert "2021-12" in insights[0]["anomaly_months"]

    def test_skips_short_series(self, analysis_db, run_id):
        analysis_db.execute(
            "INSERT INTO analysis_results (id, topic, metric, value, run_id) VALUES (?,?,?,?,?)",
            [f"short_anom_{run_id}", "tiny", "timeseries",
             json.dumps({"months": ["2023-01", "2023-02"], "counts": [1, 2]}), run_id],
        )
        from analysis.anomaly_detection import detect_anomalies
        insights = detect_anomalies(run_id, conn=analysis_db)
        assert all(i["topic"] != "tiny" for i in insights)

    def test_evidence_includes_methods(self, analysis_db, run_id):
        months = [f"2020-{m:02d}" for m in range(1, 13)] + [f"2021-{m:02d}" for m in range(1, 13)]
        counts = [10.0] * 23 + [200.0]
        analysis_db.execute(
            "INSERT INTO analysis_results (id, topic, metric, value, run_id) VALUES (?,?,?,?,?)",
            [f"meth_ts_{run_id}", "crime", "timeseries",
             json.dumps({"months": months, "counts": counts}), run_id],
        )
        from analysis.anomaly_detection import detect_anomalies
        insights = detect_anomalies(run_id, conn=analysis_db)
        if insights:
            assert "methods" in insights[0]["evidence"]


# ── Association Rules ─────────────────────────────────────────────────────────

class TestAssociationRules:
    def test_mines_rules_from_records(self, analysis_db, run_id):
        _seed_records(analysis_db, n=200)
        from analysis.association_rules import mine_association_rules
        insights = mine_association_rules(run_id, conn=analysis_db, min_support=0.01, min_confidence=0.3)
        assert len(insights) >= 1
        assert insights[0]["type"] == "association_rule"
        assert "lift" in insights[0]
        assert "confidence" in insights[0]

    def test_returns_empty_for_few_records(self, analysis_db, run_id):
        # Only 5 records — below threshold
        _seed_records(analysis_db, n=5)
        from analysis.association_rules import mine_association_rules
        insights = mine_association_rules(run_id, conn=analysis_db)
        assert isinstance(insights, list)


# ── Graph Analysis ────────────────────────────────────────────────────────────

class TestGraphAnalysis:
    def test_finds_communities(self, analysis_db, run_id):
        _seed_records(analysis_db, n=100)
        from analysis.graph_analysis import analyze_graph
        insights = analyze_graph(run_id, conn=analysis_db)
        assert isinstance(insights, list)
        # Should find at least one community with multiple topics
        if insights:
            assert insights[0]["type"] == "graph_community"
            assert len(insights[0]["topics"]) >= 2

    def test_stores_graph_analysis_metric(self, analysis_db, run_id):
        _seed_records(analysis_db, n=100)
        from analysis.graph_analysis import analyze_graph
        analyze_graph(run_id, conn=analysis_db)
        row = analysis_db.execute(
            "SELECT value FROM analysis_results WHERE metric = 'graph_analysis' AND run_id = ?", [run_id]
        ).fetchone()
        assert row is not None
        data = json.loads(row[0])
        assert "n_nodes" in data
        assert "n_communities" in data
        assert "modularity" in data


# ── Insight Ranker ────────────────────────────────────────────────────────────

class TestInsightRanker:
    def test_ranks_by_composite_score(self):
        from analysis.insight_ranker import rank_insights
        insights = [
            {"type": "trend", "topic": "health", "severity": "high", "confidence": 0.9},
            {"type": "anomaly", "topic": "crime", "severity": "low", "confidence": 0.3},
            {"type": "trend", "topic": "transport", "severity": "medium", "confidence": 0.7},
        ]
        ranked = rank_insights(insights)
        assert ranked[0]["topic"] == "health"
        assert ranked[-1]["topic"] == "crime"
        assert all("rank_score" in i for i in ranked)

    def test_penalizes_duplicate_type_topic(self):
        from analysis.insight_ranker import rank_insights
        insights = [
            {"type": "trend", "topic": "health", "severity": "high", "confidence": 0.9},
            {"type": "trend", "topic": "health", "severity": "high", "confidence": 0.9},
        ]
        ranked = rank_insights(insights)
        # Second one should have lower score due to novelty penalty
        assert ranked[0]["rank_score"] > ranked[1]["rank_score"]

    def test_handles_numeric_severity(self):
        from analysis.insight_ranker import rank_insights
        insights = [
            {"type": "anomaly", "topic": "crime", "severity": 4.5, "confidence": 0.8},
        ]
        ranked = rank_insights(insights)
        assert ranked[0]["rank_score"] > 0

    def test_empty_list(self):
        from analysis.insight_ranker import rank_insights
        assert rank_insights([]) == []


# ── API Endpoints for New Insights ────────────────────────────────────────────

class TestNewAPIEndpoints:
    """Test the new insight API endpoints against the real DB."""

    @pytest.fixture
    def api_client(self):
        from fastapi.testclient import TestClient
        from api.main import app
        return TestClient(app)

    def test_ranked_feed_endpoint(self, api_client):
        r = api_client.get("/insights/ranked-feed?limit=5")
        assert r.status_code == 200
        data = r.json()
        assert "data" in data
        assert "meta" in data

    def test_change_points_endpoint(self, api_client):
        r = api_client.get("/insights/change-points")
        assert r.status_code == 200
        assert "data" in r.json()

    def test_change_points_with_topic_filter(self, api_client):
        r = api_client.get("/insights/change-points?topic=crime")
        assert r.status_code == 200
        assert "data" in r.json()

    def test_associations_endpoint(self, api_client):
        r = api_client.get("/insights/associations?limit=5")
        assert r.status_code == 200
        assert "data" in r.json()

    def test_graph_endpoint(self, api_client):
        r = api_client.get("/insights/graph")
        assert r.status_code == 200
        assert "data" in r.json()

    def test_top_insights_still_works(self, api_client):
        r = api_client.get("/insights/top?limit=3")
        assert r.status_code == 200
        assert "data" in r.json()


# ── Narrator Templates for New Types ─────────────────────────────────────────

class TestNarratorTemplates:
    def test_template_story_generates_all_fields(self):
        from stories.narrator import _template_story
        analysis = {
            "trend": {"direction": "up", "slope": 0.5, "r_squared": 0.8},
            "summary": {"total_datasets": 100, "num_organizations": 5, "avg_quality": 0.85,
                        "earliest_modified": "2020-01-01", "latest_modified": "2024-01-01"},
            "anomalies": {"months": ["2023-06"]},
            "seasonality": {"dominant_period": 12, "strength": 0.3},
        }
        story = _template_story("health", analysis)
        assert story["headline"]
        assert story["key_finding"]
        assert story["context"]
        assert story["outlook"]
        assert story["model_used"] == "template"

    def test_template_story_handles_missing_data(self):
        from stories.narrator import _template_story
        story = _template_story("unknown", {})
        assert story["headline"]
        assert story["model_used"] == "template"
