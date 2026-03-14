"""LLM Data Story Narrator for GovDataStory — IMPROVED VERSION."""

import json
import logging
import os
import uuid
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

DB_PATH = "data/govdatastory.duckdb"

# Provider chain - in priority order
LLM_PROVIDERS = [
    {
        "name": "nvidia",
        "base_url": "https://integrate.api.nvidia.com/v1",
        "model": "meta/llama-3.1-70b-instruct",
        "api_key_env": "NVIDIA_API_KEY",
    },
    {
        "name": "glm",
        "base_url": "https://open.bigmodel.cn/api/paas/v4",
        "model": "glm-4.5-air",
        "api_key_env": "GLM_API_KEY",
    },
    {
        "name": "openrouter",
        "base_url": "https://openrouter.ai/api/v1",
        "model": "meta-llama/llama-3.3-70b-instruct:free",
        "api_key_env": "OPENROUTER_API_KEY",
    },
]


def init_stories_table():
    """Initialize data stories table."""
    import duckdb
    conn = duckdb.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS data_stories (
            id VARCHAR PRIMARY KEY,
            topic VARCHAR NOT NULL,
            run_id VARCHAR NOT NULL,
            headline VARCHAR,
            key_finding TEXT,
            context TEXT,
            outlook TEXT,
            annotations JSON,
            model_used VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.close()


def store_story(
    topic: str,
    run_id: str,
    headline: str,
    key_finding: str,
    context: str,
    outlook: str,
    annotations: list,
    model_used: str,
):
    """Store a generated story."""
    import duckdb
    conn = duckdb.connect(DB_PATH)
    conn.execute("""
        INSERT OR REPLACE INTO data_stories 
        (id, topic, run_id, headline, key_finding, context, outlook, annotations, model_used, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        f"{topic}_{run_id}",
        topic,
        run_id,
        headline,
        key_finding,
        context,
        outlook,
        json.dumps(annotations),
        model_used,
        datetime.now(timezone.utc).isoformat(),
    ])
    conn.close()


def call_llm(prompt: str, provider: dict) -> str | None:
    """Call a single LLM provider."""
    api_key = os.getenv(provider["api_key_env"])
    if not api_key:
        logger.warning(f"No API key for {provider['name']}")
        return None
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    
    payload = {
        "model": provider["model"],
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.7,
        "max_tokens": 600,
    }
    
    try:
        response = httpx.post(
            f"{provider['base_url']}/chat/completions",
            headers=headers,
            json=payload,
            timeout=45.0,
        )
        response.raise_for_status()
        
        result = response.json()
        return result["choices"][0]["message"]["content"]
    except Exception as e:
        logger.warning(f"LLM call failed for {provider['name']}: {e}")
        return None


def generate_story_with_fallback(topic: str, analysis_results: dict) -> dict:
    """Generate a story using the provider fallback chain."""
    
    # Build rich context for the LLM
    summary = analysis_results.get("summary", {})
    trend = analysis_results.get("trend", {})
    anomalies = analysis_results.get("anomalies", {})
    seasonality = analysis_results.get("seasonality", {})
    stl_trend = analysis_results.get("stl_trend", {})
    change_points = analysis_results.get("change_points", {})
    
    # Context for the story
    context_parts = [
        f"TOPIC: {topic.upper()}",
        f"Total datasets: {summary.get('total_datasets', 'N/A')}",
        f"Organizations publishing: {summary.get('num_organizations', 'N/A')}",
        f"Quality score: {summary.get('avg_quality', 'N/A')}",
        f"Date range: {summary.get('earliest_modified', 'N/A')} to {summary.get('latest_modified', 'N/A')}",
        f"Trend direction: {trend.get('direction', 'N/A')}",
        f"Monthly change: {trend.get('slope', 0):+.2f} datasets/month",
        f"R-squared: {trend.get('r_squared', 0):.3f}",
        f"STL trend: {stl_trend.get('trend', 'N/A')} (magnitude: {stl_trend.get('magnitude', 0):.2f})",
        f"Anomaly months: {anomalies.get('months', [])[:5]}",
        f"Change points: {change_points.get('change_months', [])}",
    ]
    
    prompt = f"""You are an expert data journalist writing for UK government policymakers, journalists, and researchers.

Given the analysis for the "{topic}" topic, write a compelling data story using the OIA framework:

=== OBSERVATION (1 sentence) ===
What specific numbers does the data show? Be precise.
Example: "The UK published 1,939 economy datasets across 524 organisations, but output has fallen from 15/month in 2014 to under 5/month in 2026."

=== INSIGHT (2 sentences) ===
Why does this matter? Connect to real UK events/context.
- Reference: Brexit (2016), COVID-19 (2020-21), austerity (2010-2019), 2019 general election, 2015 welfare reform
- Who is affected? (researchers, local councils, journalists, policymakers)

=== ACTION (1 sentence) ===
What specific action should someone take?
NOT "more research needed" — give concrete, actionable recommendations.

=== GUIDELINES ===
- Headline: 8-12 words, catchy, SPECIFIC to THIS data
- NO generic headlines like "X Shows Decline" or "Structural Shift Detected"
- Include 2-3 chart annotations with specific numbers/dates
- Write for decision-makers who use data
- If trend is down, explain WHO loses and HOW
- Connect anomalies to real events when possible

Analysis data:
{chr(10).join(context_parts)}

Generate JSON only:
{{
    "headline": "specific catchy title",
    "key_finding": "INSIGHT text",
    "context": "OBSERVATION text", 
    "outlook": "ACTION text",
    "annotations": ["annotation 1", "annotation 2", "annotation 3"]
}}

Respond ONLY with valid JSON, no other text."""

    for provider in LLM_PROVIDERS:
        logger.info(f"Trying LLM provider: {provider['name']}")
        
        result = call_llm(prompt, provider)
        
        if result:
            try:
                # Try to parse as JSON
                story = json.loads(result)
                
                # Validate required fields
                required = ["headline", "key_finding", "context", "outlook", "annotations"]
                if all(field in story for field in required):
                    story["model_used"] = provider["name"]
                    logger.info(f"Generated story with {provider['name']}: {story['headline']}")
                    return story
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON from {provider['name']}")
        
    
    # Fallback: template-based story
    logger.warning("LLM unavailable, using enhanced template")
    return _enhanced_template_story(topic, analysis_results)


def _enhanced_template_story(topic: str, analysis: dict) -> dict:
    """Enhanced template-based story when LLM fails."""
    import random

    trend = analysis.get("trend", {})
    summary = analysis.get("summary", {})
    anomalies = analysis.get("anomalies", {})
    stl = analysis.get("stl_trend", {})

    total = summary.get("total_datasets", 0)
    orgs = summary.get("num_organizations", 0)
    direction = trend.get("direction", "stable")
    slope = trend.get("slope", 0)
    r2 = trend.get("r_squared", 0)
    months = anomalies.get("months", [])
    
    T = topic.title()

    # Better headlines
    if direction == "up" and r2 > 0.1:
        headline = f"{T} Data Surge: {total} Datasets Across {orgs} Publishers"
    elif direction == "down" and r2 > 0.1:
        headline = f"{T} Crisis: Dataset Output Dropped {abs(slope)*12:.0f}/Year"
    else:
        headline = f"{T}: {total} Datasets From {orgs} UK Organisations"

    # Key finding with context
    if direction == "down":
        key_finding = (
            f"UK government {topic} data output has declined from peak rates to just {abs(slope):.1f} new datasets per month. "
            f"With {total} datasets from {orgs} organisations, this trend threatens research capacity."
        )
    elif direction == "up":
        key_finding = (
            f"{topic.title()} data publishing has grown to {total} datasets across {orgs} organisations. "
            f"This expansion supports better policy research and public accountability."
        )
    else:
        key_finding = (
            f"The UK publishes {total} {topic} datasets through {orgs} organisations, "
            f"providing consistent coverage for researchers and policymakers."
        )

    # Context
    context_parts = [f"Data spans {summary.get('earliest_modified', 'N/A')} to {summary.get('latest_modified', 'N/A')}."]
    if months:
        context_parts.append(f"Notable spikes in: {', '.join(months[:3])}.")
    context = " ".join(context_parts)

    # Outlook with action
    if direction == "down":
        outlook = (
            "Researchers and journalists should request data now before archives shrink further. "
            "Policymakers must prioritize open data funding to maintain transparency."
        )
    elif direction == "up":
        outlook = (
            "Continue expanding data sharing to enable evidence-based policy making. "
            "Consider linking {topic} data with related topics for richer insights."
        )
    else:
        outlook = (
            "Maintain current publishing levels while improving data quality. "
            "Cross-topic integration could unlock new insights."
        )

    annotations = [
        f"{total} datasets",
        f"{orgs} publishers",
        f"Trend: {direction}",
    ]
    if months:
        annotations.append(f"Spikes: {', '.join(months[:2])}")

    return {
        "headline": headline,
        "key_finding": key_finding,
        "context": context,
        "outlook": outlook,
        "annotations": annotations,
        "model_used": "enhanced_template",
    }


def generate_stories(run_id: str = None):
    """Generate stories for all topics."""
    if run_id is None:
        run_id = str(uuid.uuid4())
    
    init_stories_table()
    
    import duckdb
    # Clear old stories
    wconn = duckdb.connect(DB_PATH)
    wconn.execute("DELETE FROM data_stories WHERE run_id != ?", [run_id])
    wconn.close()

    conn = duckdb.connect(DB_PATH, read_only=True)
    
    # Get topics from records + cross_topic from analysis
    topics = conn.execute("SELECT DISTINCT topic FROM records").fetchall()
    all_topics = [t[0] for t in topics] + ["cross_topic"]
    conn.close()
    
    # Get analysis results
    conn = duckdb.connect(DB_PATH, read_only=True)
    analysis_data = {}
    
    for topic in all_topics:
        results = conn.execute("""
            SELECT metric, value FROM analysis_results 
            WHERE topic = ? AND run_id = ?
        """, [topic, run_id]).fetchall()
        
        analysis_data[topic] = {metric: json.loads(value) if isinstance(value, str) else value 
                               for metric, value in results}
    
    conn.close()
    
    # Generate story for each topic
    generated = 0
    for topic in analysis_data:
        if topic == "cross_topic":
            continue
        logger.info(f"Generating story for topic: {topic}")
        
        story = generate_story_with_fallback(topic, analysis_data.get(topic, {}))
        
        store_story(
            topic=topic,
            run_id=run_id,
            headline=story.get("headline", ""),
            key_finding=story.get("key_finding", ""),
            context=story.get("context", ""),
            outlook=story.get("outlook", ""),
            annotations=story.get("annotations", []),
            model_used=story.get("model_used", "unknown"),
        )
        generated += 1
        logger.info(f"  Generated: {story.get('headline', 'N/A')[:50]}...")

    # Generate cross-topic story
    cross = analysis_data.get("cross_topic", {})
    corrs = cross.get("correlations", {}).get("correlations", [])
    if corrs:
        top = sorted(corrs, key=lambda c: abs(c.get("correlation", 0)), reverse=True)[:3]
        pairs = [f"{c['topic1'].title()}-{c['topic2'].title()}" for c in top]
        
        store_story(
            topic="cross_topic", run_id=run_id,
            headline=f"UK Data Link: {pairs[0]} Strongly Connected",
            key_finding=f"Strong correlations between {' and '.join(pairs[:2])} suggest coordinated publishing. "
                        f"This could enable cross-topic research.",
            context=f"Analysis of {len(analysis_data)-1} topics found {len(corrs)} significant correlations. "
                   f"These links reveal hidden relationships in government data.",
            outlook="Policymakers should consider integrated data strategies across correlated topics.",
            annotations=[f"{c['topic1']}<->{c['topic2']}: r={c['correlation']:.2f}" for c in top],
            model_used="enhanced_template",
        )
    
    logger.info(f"Stories generated: {generated + 1}, run_id: {run_id}")
    return run_id


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    generate_stories()
