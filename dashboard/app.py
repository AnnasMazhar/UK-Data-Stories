"""UK Data Stories Dashboard - Main App."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import duckdb
import json
from datetime import datetime

st.set_page_config(
    page_title="UK Data Stories",
    page_icon="🇬🇧",
    layout="wide"
)

DB_PATH = "/home/openclaw/workspace/projects/govdatastory/data/govdatastory.duckdb"


# ============== DB FUNCTIONS ==============
def get_sources_and_topics():
    conn = duckdb.connect(DB_PATH, read_only=True)
    sources = conn.execute("SELECT source, COUNT(*) FROM records GROUP BY source").fetchall()
    topics = conn.execute("SELECT topic, COUNT(*) FROM records GROUP BY topic ORDER BY COUNT(*) DESC").fetchall()
    conn.close()
    return dict(sources), dict(topics)


def get_all_stories(limit=10):
    conn = duckdb.connect(DB_PATH, read_only=True)
    stories = conn.execute("""
        SELECT topic, headline, key_finding, context, outlook, annotations
        FROM data_stories 
        WHERE model_used = 'enhanced_template'
        ORDER BY created_at DESC
        LIMIT ?
    """, [limit]).fetchall()
    conn.close()
    return [{"topic": s[0], "headline": s[1], "key_finding": s[2], "context": s[3], "outlook": s[4], "annotations": json.loads(s[5]) if isinstance(s[5], str) else s[5]} for s in stories]


def get_topic_data(topic):
    conn = duckdb.connect(DB_PATH, read_only=True)
    result = {}
    
    ts = conn.execute("SELECT value FROM analysis_results WHERE topic = ? AND metric = 'timeseries' ORDER BY created_at DESC LIMIT 1", [topic]).fetchone()
    if ts:
        data = json.loads(ts[0])
        result['months'] = data.get('months', [])
        result['counts'] = data.get('counts', [])
    
    trend = conn.execute("SELECT value FROM analysis_results WHERE topic = ? AND metric = 'trend' ORDER BY created_at DESC LIMIT 1", [topic]).fetchone()
    if trend:
        result['trend'] = json.loads(trend[0])
    
    summary = conn.execute("SELECT value FROM analysis_results WHERE topic = ? AND metric = 'summary' ORDER BY created_at DESC LIMIT 1", [topic]).fetchone()
    if summary:
        result['summary'] = json.loads(summary[0])
    
    conn.close()
    return result


# ============== UI ==============
st.title("🇬🇧 UK Data Stories")
st.markdown("### Interactive UK Government Data Intelligence")

# Get data
sources, topics = get_sources_and_topics()
stories = get_all_stories(limit=10)

# Metrics
cols = st.columns(4)
with cols[0]:
    st.metric("Total Records", f"{sum(sources.values()):,}")
with cols[1]:
    st.metric("Sources", len(sources))
with cols[2]:
    st.metric("Topics", len(topics))
with cols[3]:
    st.metric("Stories", len(stories))

st.markdown("---")

# Featured Stories
st.subheader("📰 Featured Data Stories")

if stories:
    # Show all stories
    for story in stories:
        emoji_map = {"economy": "📊", "health": "🏥", "crime": "🔍", "education": "🎓",
                     "environment": "🌍", "population": "👥", "housing": "🏠", "transport": "🚗",
                     "parliament": "🏛️", "other": "📁"}
        emoji = emoji_map.get(story.get("topic", "").lower(), "📌")
        
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%); border-radius: 12px; padding: 20px; margin: 12px 0; color: white;">
            <h3 style="margin: 0 0 8px 0;">{emoji} {story.get('headline', '')}</h3>
            <p style="margin: 6px 0;"><strong>📈 Finding:</strong> {story.get('key_finding', '')}</p>
            <p style="margin: 6px 0;"><strong>📅 Context:</strong> {story.get('context', '')}</p>
            <p style="margin: 6px 0;"><strong>🎯 Outlook:</strong> {story.get('outlook', '')}</p>
        </div>
        """, unsafe_allow_html=True)
else:
    st.warning("No stories available.")

st.markdown("---")

# Interactive Analysis
st.subheader("📊 Interactive Analysis")

tab1, tab2, tab3 = st.tabs(["📈 Topic Trends", "🥧 Distribution", "🔎 Explore Data"])

with tab1:
    selected = st.selectbox("Select Topic", list(topics.keys()), key="topic_select")
    data = get_topic_data(selected)
    
    if data.get('months') and data.get('counts'):
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=data['months'], y=data['counts'], mode='lines+markers',
            name='Datasets', line=dict(color='#667eea', width=2)
        ))
        fig.update_layout(
            title=f"{selected.title()} Dataset Publishing Over Time",
            xaxis_title="Month", yaxis_title="Count",
            template="plotly_white", height=350
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Metrics
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Total", data.get('summary', {}).get('total_datasets', 'N/A'))
        with c2:
            trend_dir = data.get('trend', {}).get('direction', 'N/A')
            st.metric("Trend", trend_dir.title() if trend_dir else 'N/A')
        with c3:
            orgs = data.get('summary', {}).get('num_organizations', 'N/A')
            st.metric("Publishers", orgs)
    else:
        st.info("No data available for this topic.")

with tab2:
    fig = px.pie(
        values=list(topics.values()), names=list(topics.keys()),
        title="Records by Topic", hole=0.4
    )
    fig.update_layout(template="plotly_white")
    st.plotly_chart(fig, use_container_width=True)

with tab3:
    topic_filter = st.selectbox("Filter by Topic", ["All"] + list(topics.keys()), key="filter_select")
    conn = duckdb.connect(DB_PATH, read_only=True)
    if topic_filter == "All":
        ds = conn.execute("SELECT title, topic, organization FROM records ORDER BY quality_score DESC LIMIT 50").fetchall()
    else:
        ds = conn.execute("SELECT title, topic, organization FROM records WHERE topic = ? ORDER BY quality_score DESC LIMIT 50", [topic_filter]).fetchall()
    conn.close()
    
    for d in ds[:20]:
        st.markdown(f"- **{d[0][:60]}...** ({d[1]}) - {d[2]}")

# Footer
st.markdown("---")
st.caption(f"🕐 Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')} | UK Data Stories")
