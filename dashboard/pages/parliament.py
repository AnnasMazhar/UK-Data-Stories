"""Parliament Page - UK Data Stories."""

import streamlit as st
import plotly.graph_objects as go
import duckdb
import json

st.set_page_config(page_title="Parliament - UK Data Stories", page_icon="🏛️")

import os
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "govdatastory.duckdb")

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
    story = conn.execute("SELECT headline, key_finding, context, outlook FROM data_stories WHERE topic = ? AND model_used = 'enhanced_template' ORDER BY created_at DESC LIMIT 1", [topic]).fetchone()
    if story:
        result['story'] = {"headline": story[0], "key_finding": story[1], "context": story[2], "outlook": story[3]}
    conn.close()
    return result

def get_datasets(topic, limit=20):
    conn = duckdb.connect(DB_PATH, read_only=True)
    ds = conn.execute("SELECT title, organization FROM records WHERE topic = ? ORDER BY quality_score DESC LIMIT ?", [topic, limit]).fetchall()
    conn.close()
    return ds

st.title("🏛️ Parliament Data")
st.markdown("### UK Parliament Dataset Intelligence")

data = get_topic_data("parliament")

if data.get('story'):
    st.markdown(f"""<div style="background:linear-gradient(135deg,#1e3c72,#2a5298);border-radius:12px;padding:20px;color:white"><h3>🏛️ {data['story']['headline']}</h3><p><strong>Finding:</strong> {data['story']['key_finding']}</p><p><strong>Context:</strong> {data['story']['context']}</p><p><strong>Outlook:</strong> {data['story']['outlook']}</p></div>""", unsafe_allow_html=True)

if data.get('summary'):
    c1,c2,c3 = st.columns(3); c1.metric("Datasets",data['summary'].get('total_datasets')); c2.metric("Orgs",data['summary'].get('num_organizations')); c3.metric("Quality",f"{data['summary'].get('avg_quality',0):.2f}")

if data.get('months') and data.get('counts'):
    fig = go.Figure(go.Scatter(x=data['months'],y=data['counts'],mode='lines+markers',line=dict(color='#667eea')))
    fig.update_layout(title="Parliament Publishing",template="plotly_white",height=350); st.plotly_chart(fig, use_container_width=True)

st.markdown("### Sample Datasets")
for d in get_datasets("parliament")[:10]: st.markdown(f"- **{d[0][:50]}** ({d[1]})")
st.caption("UK Data Stories | Parliament")
