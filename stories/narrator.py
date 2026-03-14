"""LLM Data Story Narrator for GovDataStory."""

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
        "max_tokens": 500,
    }
    
    try:
        response = httpx.post(
            f"{provider['base_url']}/chat/completions",
            headers=headers,
            json=payload,
            timeout=30.0,
        )
        response.raise_for_status()
        
        result = response.json()
        return result["choices"][0]["message"]["content"]
    except Exception as e:
        logger.warning(f"LLM call failed for {provider['name']}: {e}")
        return None


def _template_story(topic: str, analysis: dict) -> dict:
    """Generate a data-driven narrative from analysis results using diverse templates."""
    import random

    trend = analysis.get("trend", {})
    summary = analysis.get("summary", {})
    anomalies = analysis.get("anomalies", {})
    seasonality = analysis.get("seasonality", {})

    total = summary.get("total_datasets", 0)
    orgs = summary.get("num_organizations", 0)
    direction = trend.get("direction", "stable")
    slope = trend.get("slope", 0)
    r2 = trend.get("r_squared", 0)
    pct = abs(round(slope * 12 / max(total, 1) * 100, 1))  # annualized % change

    T = topic.title()

    # Headline pools
    if direction == "up" and r2 > 0.1:
        headline = random.choice([
            f"{T} Data Publishing Accelerates",
            f"Surge in {T} Open Data Output",
            f"{T}: Government Data Production Expanding",
        ])
    elif direction == "down" and r2 > 0.1:
        headline = random.choice([
            f"{T} Data Output Slowing",
            f"Decline in {T} Dataset Publishing",
            f"{T} Open Data Activity Contracts",
        ])
    else:
        headline = random.choice([
            f"{T} Data Landscape: {total} Datasets",
            f"{T}: Steady State Across {orgs} Publishers",
            f"Mapping the {T} Data Ecosystem",
        ])

    # Key finding pools
    if direction != "unknown" and r2 > 0.05:
        word = "growing" if direction == "up" else "declining"
        key_finding = random.choice([
            f"{T} publishing is {word} at {abs(slope):.1f} datasets/month. "
            f"{orgs} organisations contribute {total} datasets to this topic.",
            f"Dataset production in {T} has been {word} ({pct}% annualised). "
            f"A total of {total} datasets are published by {orgs} organisations.",
            f"Government {topic} data output is {word}, with {orgs} publishers "
            f"maintaining {total} datasets at a rate of {abs(slope):.1f}/month.",
        ])
    else:
        key_finding = random.choice([
            f"{total} datasets from {orgs} organisations cover {topic}. "
            f"Publishing activity has been relatively stable.",
            f"The {topic} data landscape comprises {total} datasets across {orgs} publishers, "
            f"with no significant trend in publishing volume.",
        ])

    # Context
    parts = []
    if summary.get("earliest_modified") and summary.get("latest_modified"):
        parts.append(random.choice([
            f"Data spans {summary['earliest_modified']} to {summary['latest_modified']}.",
            f"Records cover the period {summary['earliest_modified']} through {summary['latest_modified']}.",
        ]))
    avg_q = summary.get("avg_quality", 0)
    if avg_q:
        parts.append(f"Average data quality score is {avg_q:.2f}.")
    anom_months = anomalies.get("months", [])
    if anom_months:
        parts.append(random.choice([
            f"Unusual activity detected in {', '.join(anom_months[:3])}.",
            f"Publishing anomalies were flagged for {', '.join(anom_months[:3])}.",
        ]))
    period = seasonality.get("dominant_period", 0)
    strength = seasonality.get("strength", 0)
    if strength > 0.15 and period > 0:
        parts.append(f"A {period:.0f}-month publishing cycle is evident.")
    context = " ".join(parts) if parts else f"The {topic} topic aggregates UK government open data."

    # Outlook pools
    if direction == "up":
        outlook = random.choice([
            "Continued growth expected as more organisations publish open data.",
            "The upward trend suggests expanding government transparency in this area.",
        ])
    elif direction == "down":
        outlook = random.choice([
            "The decline may reflect dataset consolidation or reduced publishing activity.",
            "Fewer new publications could indicate data maturity or shifting priorities.",
        ])
    else:
        outlook = random.choice([
            "Stable publishing suggests a mature data landscape for this topic.",
            "Consistent output indicates an established publishing cadence.",
        ])

    annotations = []
    if direction != "unknown":
        annotations.append(f"Trend: {direction} ({slope:+.1f}/mo)")
    if anom_months:
        annotations.append(f"Anomalies: {len(anom_months)}")
    annotations.append(f"Quality: {avg_q:.2f}")

    return {
        "headline": headline,
        "key_finding": key_finding,
        "context": context,
        "outlook": outlook,
        "annotations": annotations,
        "model_used": "template",
    }


def generate_story_with_fallback(topic: str, analysis_results: dict) -> dict:
    """Generate a story using the provider fallback chain."""
    
    prompt = f"""You are a data storyteller. Given the following analysis for the topic "{topic}", generate a short data story.

Analysis results:
{json.dumps(analysis_results, indent=2)}

Generate a JSON response with exactly these fields:
{{
    "headline": "10 words maximum, catchy title",
    "key_finding": "2 sentences explaining the main insight",
    "context": "3-4 sentences of background context",
    "outlook": "1-2 sentences about what this means for the future",
    "annotations": ["chart annotation 1", "chart annotation 2", "chart annotation 3"]
}}

Respond ONLY with valid JSON, no additional text."""

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
                    return story
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON from {provider['name']}")
        
    
    # Fallback: template-based story from real analysis data
    logger.info("LLM unavailable, using template-based narrative")
    return _template_story(topic, analysis_results)


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
        # Get latest analysis for this topic
        results = conn.execute("""
            SELECT metric, value FROM analysis_results 
            WHERE topic = ? AND run_id = ?
        """, [topic, run_id]).fetchall()
        
        analysis_data[topic] = {metric: json.loads(value) if isinstance(value, str) else value 
                               for metric, value in results}
    
    conn.close()
    
    # Generate story for each topic (skip cross_topic — handled separately)
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

    # Generate stories for advanced insight types
    _generate_advanced_stories(run_id, analysis_data)

    # Generate cross-topic synthesis story
    cross = analysis_data.get("cross_topic", {})
    corrs = cross.get("correlations", {}).get("correlations", [])
    if corrs:
        top = sorted(corrs, key=lambda c: abs(c.get("correlation", 0)), reverse=True)[:3]
        pairs = [f"{c['topic1'].title()}-{c['topic2'].title()} (r={c['correlation']:.2f})" for c in top]
        store_story(
            topic="cross_topic", run_id=run_id,
            headline="Cross-Topic Patterns in UK Government Data",
            key_finding=f"Strongest linked topic pairs: {'; '.join(pairs)}.",
            context=f"Analysis of {len(analysis_data) - 1} topics reveals {len(corrs)} significant cross-topic correlations in publishing activity.",
            outlook="Cross-topic patterns suggest coordinated government data publishing cycles.",
            annotations=[f"{c['topic1']}<->{c['topic2']}: {c['correlation']:.2f}" for c in top],
            model_used="template",
        )
    
    logger.info(f"Stories generated, run_id: {run_id}")
    return run_id


def _generate_advanced_stories(run_id: str, analysis_data: dict):
    """Generate narrative stories for advanced insight types."""
    import random

    cross = analysis_data.get("cross_topic", {})

    # Change point stories
    for topic, data in analysis_data.items():
        if topic == "cross_topic":
            continue
        cp = data.get("change_points")
        if not cp:
            continue
        months = cp.get("change_months", [])
        if not months:
            continue
        store_story(
            topic=topic, run_id=run_id,
            headline=f"Structural Shift Detected in {topic.title()} Publishing",
            key_finding=random.choice([
                f"{topic.title()} dataset publication underwent a regime change in {', '.join(months)}.",
                f"A structural shift in {topic} publishing activity was detected around {months[0]}.",
            ]),
            context=f"Change-point analysis (PELT algorithm) identified {len(months)} breakpoint(s) in the publication timeline.",
            outlook="This may reflect policy changes, new data mandates, or organisational restructuring.",
            annotations=[f"Shift: {m}" for m in months],
            model_used="template",
        )

    # Graph community stories
    graph = cross.get("graph_analysis", {})
    communities = graph.get("communities", {})
    for comm_id, members in communities.items():
        topic_members = [m for m in members if m in analysis_data and m != "cross_topic"]
        if len(topic_members) < 2:
            continue
        names = ", ".join(t.title() for t in topic_members)
        store_story(
            topic="cross_topic", run_id=run_id,
            headline=f"Policy Cluster: {names}",
            key_finding=f"{names} form a tightly connected cluster in the dataset knowledge graph.",
            context=f"Louvain community detection grouped {len(members)} nodes (topics and organisations) into this cluster (modularity={graph.get('modularity', 0):.2f}).",
            outlook="Clustered topics may benefit from integrated data strategies.",
            annotations=[f"Cluster {comm_id}: {len(members)} nodes"],
            model_used="template",
        )
        break  # one graph story is enough

    # Association rule stories
    rules = cross.get("association_rules")
    if rules and isinstance(rules, list) and len(rules) > 0:
        top = rules[:3]
        lines = [f"{r['antecedent']} → {r['consequent']} (lift={r['lift']:.1f})" for r in top]
        store_story(
            topic="cross_topic", run_id=run_id,
            headline="Hidden Associations in Government Data",
            key_finding=f"Top association rules: {'; '.join(lines)}.",
            context=f"Apriori mining discovered {len(rules)} significant rules linking topics, publishers, and keywords.",
            outlook="These co-occurrence patterns can guide data integration and cross-departmental collaboration.",
            annotations=[f"{r['antecedent']}→{r['consequent']}" for r in top],
            model_used="template",
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    generate_stories()
