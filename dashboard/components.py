"""Dashboard components for UK Data Stories."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import duckdb

import os
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "govdatastory.duckdb")


@st.cache_resource
def get_db():
    """Get thread-safe DuckDB read-only connection."""
    return duckdb.connect(DB_PATH, read_only=True)


@st.cache_data(ttl=300)
def get_record_counts():
    """Get record counts by source and topic."""
    conn = get_db()
    
    sources = conn.execute("SELECT source, COUNT(*) FROM records GROUP BY source").fetchall()
    topics = conn.execute("SELECT topic, COUNT(*) FROM records GROUP BY topic ORDER BY COUNT(*) DESC").fetchall()
    
    return dict(sources), dict(topics)


@st.cache_data(ttl=300)
def get_latest_story(topic: str):
    """Fetch most recent story for a topic."""
    conn = get_db()
    
    story = conn.execute("""
        SELECT headline, key_finding, context, outlook, annotations, created_at
        FROM data_stories 
        WHERE topic = ?
        ORDER BY created_at DESC
        LIMIT 1
    """, [topic]).fetchone()
    
    if story:
        import json
        annotations = json.loads(story[4]) if isinstance(story[4], str) else story[4]
        return {
            "headline": story[0],
            "key_finding": story[1],
            "context": story[2],
            "outlook": story[3],
            "annotations": annotations,
            "created_at": story[5]
        }
    return None


@st.cache_data(ttl=300)
def get_analysis(topic: str):
    """Fetch latest analysis results for a topic."""
    conn = get_db()
    
    results = conn.execute("""
        SELECT metric, value FROM analysis_results 
        WHERE topic = ?
        ORDER BY created_at DESC
        LIMIT 10
    """, [topic]).fetchall()
    
    import json
    return {metric: json.loads(value) if isinstance(value, str) else value for metric, value in results}


@st.cache_data(ttl=300)
def get_datasets(topic: str = None, limit: int = 100):
    """Fetch datasets."""
    conn = get_db()
    
    if topic:
        datasets = conn.execute("""
            SELECT id, title, description, topic, organization, source, quality_score
            FROM records 
            WHERE topic = ?
            ORDER BY quality_score DESC
            LIMIT ?
        """, [topic, limit]).fetchall()
    else:
        datasets = conn.execute("""
            SELECT id, title, description, topic, organization, source, quality_score
            FROM records 
            ORDER BY quality_score DESC
            LIMIT ?
        """, [limit]).fetchall()
    
    return datasets


def render_story_card(story: dict):
    """Render a styled story card."""
    if not story:
        st.info("No story available yet.")
        return
    
    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 10px;
        padding: 20px;
        margin: 10px 0;
        color: white;
    ">
        <h3 style="margin: 0 0 10px 0;">{story.get('headline', '')}</h3>
        <p><strong>Key Finding:</strong> {story.get('key_finding', '')}</p>
        <p><strong>Context:</strong> {story.get('context', '')}</p>
        <p><strong>Outlook:</strong> {story.get('outlook', '')}</p>
    </div>
    """, unsafe_allow_html=True)


@st.cache_data(ttl=300)
def get_insights(topic: str = None, limit: int = 10):
    """Fetch ranked insights."""
    conn = get_db()
    if topic:
        rows = conn.execute("""
            SELECT insight_type, severity, confidence, title, summary
            FROM insights WHERE topic = ?
            ORDER BY CASE severity WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END, confidence DESC
            LIMIT ?
        """, [topic, limit]).fetchall()
    else:
        rows = conn.execute("""
            SELECT insight_type, severity, confidence, title, summary
            FROM insights
            ORDER BY CASE severity WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END, confidence DESC
            LIMIT ?
        """, [limit]).fetchall()
    return [{"type": r[0], "severity": r[1], "confidence": r[2], "title": r[3], "summary": r[4]} for r in rows]


def render_insight_cards(insights: list):
    """Render insight cards with severity badges."""
    colors = {"high": "#e74c3c", "medium": "#f39c12", "low": "#3498db"}
    for ins in insights:
        color = colors.get(ins["severity"], "#95a5a6")
        st.markdown(f"""
        <div style="border-left: 4px solid {color}; padding: 8px 12px; margin: 6px 0; background: #f8f9fa; border-radius: 4px;">
            <span style="background:{color}; color:white; padding:2px 8px; border-radius:3px; font-size:0.75em; text-transform:uppercase;">{ins['severity']}</span>
            <span style="font-size:0.75em; color:#666;"> {ins['type']}</span>
            <div style="font-weight:600; margin-top:4px;">{ins['title']}</div>
            <div style="font-size:0.9em; color:#555;">{ins['summary']}</div>
        </div>
        """, unsafe_allow_html=True)


def build_trend_chart(data, months=None, anomalies=None, title: str = "Trend"):
    """Build a Plotly line chart with annotations."""
    fig = go.Figure()
    
    x_vals = months if months else list(range(len(data)))

    # Main trend line
    fig.add_trace(go.Scatter(
        x=x_vals,
        y=data,
        mode='lines+markers',
        name='Datasets',
        line=dict(color='#667eea', width=2),
    ))
    
    # Add anomaly markers
    if anomalies:
        anomaly_x = [a for a in anomalies if a < len(data)]
        if anomaly_x:
            anomaly_y = [data[a] for a in anomaly_x]
            ax = [x_vals[a] for a in anomaly_x] if months else anomaly_x
            fig.add_trace(go.Scatter(
                x=ax,
                y=anomaly_y,
                mode='markers',
                name='Anomaly',
                marker=dict(color='red', size=10, symbol='x')
            ))
    
    fig.update_layout(
        title=title,
        xaxis_title="Time",
        yaxis_title="Value",
        template="plotly_white",
    )
    
    return fig


def build_heatmap(data, title: str = "Heatmap"):
    """Build a consistent Plotly heatmap."""
    fig = px.imshow(
        data,
        labels=dict(x="X", y="Y", color="Value"),
        title=title,
    )
    
    fig.update_layout(template="plotly_white")
    return fig


def build_distribution_chart(data, title: str = "Distribution"):
    """Build a consistent distribution chart."""
    fig = px.bar(
        x=list(data.keys()),
        y=list(data.values()),
        title=title,
        labels={'x': 'Category', 'y': 'Count'},
    )
    
    fig.update_layout(template="plotly_white")
    return fig


def build_donut_chart(data, title: str = "Distribution"):
    """Build a donut chart."""
    fig = px.pie(
        values=list(data.values()),
        names=list(data.keys()),
        title=title,
        hole=0.4,
    )
    
    fig.update_layout(template="plotly_white")
    return fig


def render_footer(source: str = "UK Data Stories"):
    """Render a consistent footer."""
    import datetime
    st.markdown(f"""
    ---
    *Last updated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Source: {source}*
    """)
