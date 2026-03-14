"""Housing Page - UK Data Stories."""

import streamlit as st
import plotly.graph_objects as go
import duckdb
import json

st.set_page_config(page_title="Housing - UK Data Stories", page_icon="🏠")

DB_PATH = "/home/openclaw/workspace/projects/govdatastory/data/govdatastory.duckdb"

@st.cache_resource
def get_db():
    return duckdb.connect(DB_PATH, read_only=True)

@st.cache_data(ttl=60)
def get_topic_data(topic):
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
    
    story = conn.execute("SELECT headline, key_finding, context, outlook FROM data_stories WHERE topic = ? AND model_used = 'enhanced_template' ORDER BY created_at DESC LIMIT 1", [topic]).fetchone()
    if story:
        result['story'] = {"headline": story[0], "key_finding": story[1], "context": story[2], "outlook": story[3]}
    
    conn.close()
    return result

@st.cache_data(ttl=60)
def get_datasets(topic, limit=20):
    conn = get_db()
    ds = conn.execute("SELECT title, organization FROM records WHERE topic = ? ORDER BY quality_score DESC LIMIT ?", [topic, limit]).fetchall()
    conn.close()
    return ds

st.title("🏠 Housing Data")
st.markdown("### UK Housing Dataset Intelligence")

data = get_topic_data("housing")

if data.get('story'):
    st.markdown(f"""
    <div style="background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%); border-radius: 12px; padding: 20px; margin: 12px 0; color: white;">
        <h3>🏠 {data['story']['headline']}</h3>
        <p><strong>Finding:</strong> {data['story']['key_finding']}</p>
        <p><strong>Context:</strong> {data['story']['context']}</p>
        <p><strong>Outlook:</strong> {data['story']['outlook']}</p>
    </div>
    """, unsafe_allow_html=True)

if data.get('summary'):
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Total Datasets", data['summary'].get('total_datasets', 'N/A'))
    with c2:
        st.metric("Organizations", data['summary'].get('num_organizations', 'N/A'))
    with c3:
        st.metric("Quality", f"{data['summary'].get('avg_quality', 0):.2f}")

if data.get('months') and data.get('counts'):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=data['months'], y=data['counts'], mode='lines+markers', name='Datasets', line=dict(color='#667eea', width=2)))
    fig.update_layout(title="Housing Dataset Publishing Over Time", xaxis_title="Month", yaxis_title="Count", template="plotly_white", height=350)
    st.plotly_chart(fig, use_container_width=True)

st.markdown("### Sample Datasets")
for d in get_datasets("housing")[:10]:
    st.markdown(f"- **{d[0][:50]}...** ({d[1]})")

st.caption("UK Data Stories | Housing")
