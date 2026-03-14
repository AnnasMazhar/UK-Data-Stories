"""Insight ranker — composite scoring and ranking for all insight types."""

import logging


logger = logging.getLogger(__name__)
DB_PATH = "data/govdatastory.duckdb"


def _severity_score(insight: dict) -> float:
    """Extract severity as 0-1 score."""
    sev = insight.get("severity", 0)
    if isinstance(sev, str):
        return {"high": 1.0, "medium": 0.6, "low": 0.3}.get(sev, 0.3)
    return min(float(sev) / 5.0, 1.0)  # numeric severity (e.g. z-score)


def _novelty_score(insight: dict, seen: set) -> float:
    """Penalize if similar insight already seen."""
    key = f"{insight.get('type')}:{insight.get('topic', '')}"
    if key in seen:
        return 0.3
    seen.add(key)
    return 1.0


def _quality_score(insight: dict) -> float:
    """Data quality proxy from evidence."""
    ev = insight.get("evidence", {})
    if isinstance(ev, dict):
        return min(ev.get("r_squared", ev.get("modularity", 0.7)), 1.0)
    return 0.7


def rank_insights(insights: list[dict]) -> list[dict]:
    """Rank insights by composite score: 0.4*severity + 0.3*confidence + 0.2*novelty + 0.1*quality."""
    seen = set()
    for ins in insights:
        sev = _severity_score(ins)
        conf = float(ins.get("confidence", 0.5))
        nov = _novelty_score(ins, seen)
        qual = _quality_score(ins)
        ins["rank_score"] = round(0.4 * sev + 0.3 * conf + 0.2 * nov + 0.1 * qual, 4)

    insights.sort(key=lambda x: -x["rank_score"])
    return insights
