"""UK Data Stories Dashboard - Clean Unified Version."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import duckdb
import json
from datetime import datetime

st.set_page_config(
    page_title="UK Data Stories",
    page_icon="🇬🇧",
    layout="wide",
    initial_sidebar_state="expanded"
)

DB_PATH = "/home/openclaw/workspace/projects/govdatastory/data/govdatastory.duckdb"


# ============== DATABASE FUNCTIONS ==============
@st.cache_resource
def get_db():
    return duckdb.connect(DB_PATH, read_only=True)


@st.cache_data(ttl=60)
def get_record_counts():
    conn = get_db()
    sources = conn.execute("SELECT source, COUNT(*) FROM records GROUP BY source").fetchall()
    topics = conn.execute("SELECT topic, COUNT(*) FROM records GROUP BY topic ORDER BY COUNT(*) DESC").fetchall()
    conn.close()
    return dict(sources), dict(topics)


@st.cache_data(ttl=60)
def get_all_stories(limit=10):
    """Get enhanced stories with fallback."""
    conn = get_db()
    stories = conn.execute("""
        SELECT topic, headline, key_finding, context, outlook, annotations
        FROM data_stories 
        WHERE model_used = 'enhanced_template'
        ORDER BY created_at DESC
        LIMIT ?
    """, [limit]).fetchall()
    conn.close()
    return [{"topic": s[0], "headline": s[1], "key_finding": s[2], "context": s[3], "outlook": s[4], "annotations": json.loads(s[5]) if isinstance(s[5], str) else s[5]} for s in stories]


@st.cache_data(ttl=60)
def get_topic_story(topic):
    """Get story for a specific topic."""
    conn = get_db()
    story = conn.execute("""
        SELECT headline, key_finding, context, outlook, annotations
        FROM data_stories 
        WHERE topic = ? AND model_used = 'enhanced_template'
        ORDER BY created_at DESC LIMIT 1
    """, [topic]).fetchone()
    conn.close()
    if story:
        return {
            "headline": story[0], "key_finding": story[1], "context": story[2],
            "outlook": story[3], "annotations": json.loads(story[4]) if isinstance(story[4], str) else story[4]
        }
    return None


@st.cache_data(ttl=60)
def get_topic_data(topic):
    """Get timeseries and analysis for a topic."""
    conn = get_db()
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


@st.cache_data(ttl=60)
def get_datasets(topic=None, limit=50):
    conn = get_db()
    if topic:
        ds = conn.execute("SELECT id, title, topic, organization, source, quality_score FROM records WHERE topic = ? ORDER BY quality_score DESC LIMIT ?", [topic, limit]).fetchall()
    else:
        ds = conn.execute("SELECT id, title, topic, organization, source, quality_score FROM records ORDER BY quality_score DESC LIMIT ?", [limit]).fetchall()
    conn.close()
    return ds


# ============== UI COMPONENTS ==============
def render_story_card(story, expanded=False):
    """Render a story card with proper styling."""
    if not story:
        return
    
    emoji_map = {
        "economy": "📊", "health": "🏥", "crime": "🔍", "education": "🎓",
        "environment": "🌍", "population": "👥", "housing": "🏠", "transport": "🚗",
        "parliament": "🏛️", "other": "📁"
    }
    emoji = emoji_map.get(story.get("topic", "").lower(), "📌")
    
    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
        border-radius: 12px;
        padding: 20px;
        margin: 12px 0;
        color: white;
    ">
        <h3 style="margin: 0 0 8px 0;">{emoji} {story.get('headline', '')}</h3>
        <p style="margin: 8px 0; opacity: 0.95;"><strong>📈 Finding:</strong> {story.get('key_finding', '')[:200]}</p>
        <p style="margin: 6px 0; opacity: 0.85;"><strong>📅 Context:</strong> {story.get('context', '')}</p>
        <p style="margin: 6px 0; opacity: 0.85;"><strong>🎯 Outlook:</strong> {story.get('outlook', '')}</p>
    </div>
    """, unsafe_allow_html=True)


def render_story_inline(story):
    """Render compact story for insights section."""
    if not story:
        return
    emoji_map = {"economy": "📊", "health": "🏥", "crime": "🔍", "education": "🎓",
                 "environment": "🌍", "population": "👥", "housing": "🏠", "transport": "🚗",
                 "parliament": "🏛️", "other": "📁"}
    emoji = emoji_map.get(story.get("topic", "").lower(), "📌")
    st.markdown(f"**{emoji} {story.get('headline', '')}**")
    st.caption(f"{story.get('key_finding', '')[:120]}...")


def build_trend_chart(data, months, title):
    """Build trend line chart."""
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=months, y=data, mode='lines+markers',
        name='Datasets', line=dict(color='#667eea', width=2)
    ))
    fig.update_layout(title=title, xaxis_title="Month", yaxis_title="Dataset Count",
                     template="plotly_white", height=350)
    return fig


# ============== MAIN APP ==============
def main():
    sources, topics = get_record_counts()
    stories = get_all_stories(limit=10)
    
    # Sidebar
    st.sidebar.title("🇬🇧 UK Data Stories")
    st.sidebar.markdown("### 📊 Quick Stats")
    st.sidebar.metric("Total Records", f"{sum(sources.values()):,}")
    st.sidebar.metric("Sources", len(sources))
    st.sidebar.metric("Topics", len(topics))
    
    st.sidebar.markdown("### 📚 Topics")
    for topic, count in topics.items():
        st.sidebar.markdown(f"- **{topic.title()}**: {count:,}")
    
    st.sidebar.markdown("---")
    st.sidebar.info("💡 Stories powered by AI analysis")
    
    # Main header
    st.title("🇬🇧 UK Data Stories")
    st.markdown("### Interactive UK Government Data Intelligence")
    
    # Metrics row
    cols = st.columns(4)
    with cols[0]:
        st.metric("Records", f"{sum(sources.values()):,}")
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
        # Top 3 featured
        c1, c2, c3 = st.columns(3)
        for i, story in enumerate(stories[:3]):
            with [c1, c2, c3][i]:
                render_story_card(story)
        
        # All stories expandable
        with st.expander("📖 View All Stories"):
            for story in stories:
                render_story_card(story)
    else:
        st.warning("No stories available. Run analysis pipeline.")
    
    # Interactive Analysis
    st.markdown("---")
    st.subheader("📊 Interactive Analysis")
    
    tab1, tab2, tab3 = st.tabs(["📈 Topic Trends", "🥧 Distribution", "🔎 Explore Data"])
    
    with tab1:
        selected = st.selectbox("Select Topic", list(topics.keys()))
        data = get_topic_data(selected)
        
        if data.get('months') and data.get('counts'):
            fig = build_trend_chart(data['counts'], data['months'], f"{selected.title()} Dataset Publishing")
            st.plotly_chart(fig, use_container_width=True)
            
            # Metrics
            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric("Total", data['summary'].get('total_datasets', 'N/A'))
            with c2:
                trend_dir = data.get('trend', {}).get('direction', 'N/A')
                st.metric("Trend", trend_dir.title())
            with c3:
                orgs = data['summary'].get('num_organizations', 'N/A')
                st.metric("Publishers", orgs)
            
            # Story for this topic
            topic_story = get_topic_story(selected)
            if topic_story:
                st.markdown("#### 📖 Story")
                render_story_card(topic_story)
        else:
            st.info("No data available.")
    
    with tab2:
        fig = px.pie(values=list(topics.values()), names=list(topics.keys()), 
                     title="Records by Topic", hole=0.4)
        fig.update_layout(template="plotly_white")
        st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        topic_filter = st.selectbox("Filter by Topic", ["All"] + list(topics.keys()))
        datasets = get_datasets(topic_filter if topic_filter != "All" else None)
        if datasets:
            import pandas as pd
            df = pd.DataFrame(datasets, columns=["ID", "Title", "Topic", "Organization", "Source", "Quality"])
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No datasets found.")
    
    # Footer
    st.markdown("---")
    st.caption(f"🕐 Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')} | UK Data Stories")


if __name__ == "__main__":
    main()
