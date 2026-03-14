"""UK Data Stories Dashboard - IMPROVED VERSION."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(
    page_title="UK Data Stories",
    page_icon="🇬🇧",
    layout="wide",
    initial_sidebar_state="expanded"
)

from components import (
    get_record_counts,
    get_latest_story,
    get_datasets,
    build_trend_chart,
    build_donut_chart,
    render_footer,
)

DB_PATH = "data/govdatastory.duckdb"


@st.cache_resource
def get_db():
    import duckdb
    return duckdb.connect(DB_PATH, read_only=True)


def get_stories(limit: int = 10):
    """Get all enhanced stories."""
    conn = get_db()
    stories = conn.execute("""
        SELECT topic, headline, key_finding, context, outlook, annotations, model_used
        FROM data_stories 
        WHERE model_used = 'enhanced_template'
        ORDER BY created_at DESC
        LIMIT ?
    """, [limit]).fetchall()
    conn.close()
    return stories


def get_topic_trend(topic: str):
    """Get trend data for a topic."""
    import json
    conn = get_db()
    
    # Get timeseries
    ts = conn.execute("""
        SELECT value FROM analysis_results 
        WHERE topic = ? AND metric = 'timeseries'
        ORDER BY created_at DESC LIMIT 1
    """, [topic]).fetchone()
    
    trend = conn.execute("""
        SELECT value FROM analysis_results 
        WHERE topic = ? AND metric = 'trend'
        ORDER BY created_at DESC LIMIT 1
    """, [topic]).fetchone()
    
    anomalies = conn.execute("""
        SELECT value FROM analysis_results 
        WHERE topic = ? AND metric = 'anomalies'
        ORDER BY created_at DESC LIMIT 1
    """, [topic]).fetchone()
    
    conn.close()
    
    result = {}
    if ts:
        data = json.loads(ts[0])
        result['months'] = data.get('months', [])
        result['counts'] = data.get('counts', [])
    if trend:
        result['trend'] = json.loads(trend[0])
    if anomalies:
        result['anomalies'] = json.loads(anomalies[0])
    
    return result


# Sidebar
st.sidebar.title("🇬🇧 UK Data Stories")
st.sidebar.markdown("UK Government Data Intelligence")

sources, topics = get_record_counts()
st.sidebar.metric("Total Records", f"{sum(sources.values()):,}")
st.sidebar.metric("Topics", len(topics))

st.sidebar.markdown("### Topics")
for topic in topics:
    st.sidebar.markdown(f"- **{topic.title()}**: {topics[topic]}")

st.sidebar.markdown("---")
st.sidebar.markdown("*Enhanced data stories powered by AI*")


# Main content
st.title("🇬🇧 UK Data Stories")
st.markdown("### UK Government Data — Narrative Intelligence")

# Hero metrics
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Records", f"{sum(sources.values()):,}", delta=None)
with col2:
    st.metric("Sources", len(sources), delta=None)
with col3:
    st.metric("Topics", len(topics), delta=None)
with col4:
    top_topic = list(topics.keys())[0] if topics else "N/A"
    st.metric("Largest Topic", f"{top_topic.title()}", delta=f"{topics.get(top_topic, 0):,}")


# Featured Stories Section
st.markdown("---")
st.subheader("📰 Featured Data Stories")

stories = get_stories(limit=6)

if stories:
    # Show top 3 as featured
    cols = st.columns(3)
    for i, story in enumerate(stories[:3]):
        with cols[i]:
            st.markdown(f"""
            <div style="
                background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
                border-radius: 12px;
                padding: 16px;
                margin: 8px 0;
                color: white;
                height: 200px;
                overflow: hidden;
            ">
                <h4 style="margin: 0 0 8px 0; font-size: 1.1em;">{story[1]}</h4>
                <p style="font-size: 0.85em; opacity: 0.9;">{story[2][:120]}...</p>
                <span style="background: rgba(255,255,255,0.2); padding: 2px 8px; border-radius: 4px; font-size: 0.75em;">{story[0].upper()}</span>
            </div>
            """, unsafe_allow_html=True)
    
    # Show all stories expandable
    with st.expander("View All Stories"):
        for story in stories:
            with st.container():
                st.markdown(f"**{story[1]}**")
                st.caption(f"[{story[0].upper()}] {story[2][:150]}...")
                st.divider()
else:
    st.info("No enhanced stories available. Run the analysis pipeline.")


# Interactive Charts Section
st.markdown("---")
st.subheader("📊 Data Trends")

tab1, tab2, tab3 = st.tabs(["By Topic", "By Source", "Explore"])

with tab1:
    selected_topic = st.selectbox("Select Topic", list(topics.keys()), index=0)
    trend_data = get_topic_trend(selected_topic)
    
    if trend_data.get('months') and trend_data.get('counts'):
        fig = build_trend_chart(
            trend_data['counts'], 
            months=trend_data['months'],
            anomalies=trend_data.get('anomalies', {}).get('indices', []),
            title=f"{selected_topic.title()} Dataset Publishing Over Time"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Show trend info
        trend = trend_data.get('trend', {})
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Trend", trend.get('direction', 'N/A').title())
        with col2:
            st.metric("Monthly Change", f"{trend.get('slope', 0):+.2f}")
        with col3:
            st.metric("R²", f"{trend.get('r_squared', 0):.3f}")
    else:
        st.info("No trend data available for this topic.")

with tab2:
    fig = build_donut_chart(topics, "Records by Topic")
    st.plotly_chart(fig, use_container_width=True)

with tab3:
    st.markdown("### Explore Datasets")
    topic_filter = st.selectbox("Filter by Topic", ["All"] + list(topics.keys()))
    if topic_filter == "All":
        datasets = get_datasets(limit=50)
    else:
        datasets = get_datasets(topic=topic_filter, limit=50)
    
    if datasets:
        import pandas as pd
        df = pd.DataFrame(datasets, columns=["ID", "Title", "Description", "Topic", "Organization", "Source", "Quality"])
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No datasets found.")


# Quick Insights
st.markdown("---")
st.subheader("💡 Key Insights")

for story in stories[:5]:
    st.markdown(f"""
    **{story[0].title()}**: {story[2]}  
    *{story[4]}*
    """)

render_footer()
