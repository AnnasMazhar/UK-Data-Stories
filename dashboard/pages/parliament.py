"""Parliament Page - UK Data Stories."""
import streamlit as st
import plotly.graph_objects as go
import duckdb, json
st.set_page_config(page_title="Parliament - UK Data Stories", page_icon="🏛️")
DB = "data/govdatastory.duckdb"
@st.cache_resource def get_db(): return duckdb.connect(DB, read_only=True)
@st.cache_data(ttl=60)
def get_topic_data(topic):
    c = get_db(); r = {}
    t = c.execute("SELECT value FROM analysis_results WHERE topic = ? AND metric='timeseries' ORDER BY created_at DESC LIMIT 1", [topic]).fetchone()
    if t: d = json.loads(t[0]); r['months'], r['counts'] = d.get('months',[]), d.get('counts',[])
    tr = c.execute("SELECT value FROM analysis_results WHERE topic = ? AND metric='trend' ORDER BY created_at DESC LIMIT 1", [topic]).fetchone()
    if tr: r['trend'] = json.loads(tr[0])
    s = c.execute("SELECT value FROM analysis_results WHERE topic = ? AND metric='summary' ORDER BY created_at DESC LIMIT 1", [topic]).fetchone()
    if s: r['summary'] = json.loads(s[0])
    st = c.execute("SELECT headline, key_finding, context, outlook FROM data_stories WHERE topic = ? AND model_used='enhanced_template' ORDER BY created_at DESC LIMIT 1", [topic]).fetchone()
    if st: r['story'] = dict(zip(['headline','key_finding','context','outlook'], st))
    c.close(); return r
@st.cache_data(ttl=60)
def get_datasets(topic, limit=20):
    c = get_db(); ds = c.execute("SELECT title, organization FROM records WHERE topic = ? ORDER BY quality_score DESC LIMIT ?", [topic, limit]).fetchall()
    c.close(); return ds

st.title("🏛️ Parliament Data")
st.markdown("### UK Parliament Dataset Intelligence")
data = get_topic_data("parliament")
if data.get('story'):
    st.markdown(f"""<div style="background:linear-gradient(135deg,#1e3c72,#2a5298);border-radius:12px;padding:20px;color:white"><h3>🏛️ {data['story']['headline']}</h3><p><strong>Finding:</strong> {data['story']['key_finding']}</p><p><strong>Context:</strong> {data['story']['context']}</p><p><strong>Outlook:</strong> {data['story']['outlook']}</p></div>""", unsafe_allow_html=True)
if data.get('summary'):
    c1,c2,c3 = st.columns(3); c1.metric("Datasets",data['summary'].get('total_datasets')); c2.metric("Orgs",data['summary'].get('num_organizations')); c3.metric("Quality",f"{data['summary'].get('avg_quality',0):.2f}")
if data.get('months') and data.get('counts'):
    fig = go.Figure(go.Scatter(x=data['months'],y=data['counts'],mode='lines+markers',line=dict(color='#667eea')))
    fig.update_layout(title="Parliament Publishing",template="plotly_white",height=350); st.plotly_chart(fig,use_container_width=True)
st.markdown("### Sample Datasets")
for d in get_datasets("parliament")[:10]: st.markdown(f"- **{d[0][:50]}...** ({d[1]})")
st.caption("UK Data Stories | Parliament")
