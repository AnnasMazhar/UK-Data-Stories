"""Population Page - UK Data Stories."""
import streamlit as st
import plotly.graph_objects as go
st.set_page_config(page_title="Population - UK Data Stories", page_icon="👥")

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_helper import ensure_db, get_topic_data, get_datasets
ensure_db()

st.title("👥 Population Data")
data = get_topic_data("population")

if data.get('story'):
    st.markdown(f"""<div style="background:linear-gradient(135deg,#1e3c72,#2a5298);border-radius:12px;padding:20px;color:white"><h3>👥 {data['story']['headline']}</h3><p><b>Finding:</b> {data['story']['key_finding']}</p><p><b>Context:</b> {data['story']['context']}</p><p><b>Outlook:</b> {data['story']['outlook']}</p></div>""", unsafe_allow_html=True)

if data.get('summary'):
    c1,c2,c3 = st.columns(3)
    c1.metric("Datasets", data['summary'].get('total_datasets'))
    c2.metric("Orgs", data['summary'].get('num_organizations'))
    c3.metric("Quality", f"{data['summary'].get('avg_quality',0):.2f}")

if data.get('months') and data.get('counts'):
    fig = go.Figure(go.Scatter(x=data['months'], y=data['counts'], mode='lines+markers', line=dict(color='#667eea')))
    fig.update_layout(title="Population Publishing Over Time", template="plotly_white", height=350)
    st.plotly_chart(fig, use_container_width=True)
    if data.get('trend'):
        t = data['trend']
        st.caption(f"Trend: {t.get('direction','?')} (slope={t.get('slope',0):.2f}/mo, R\u00b2={t.get('r_squared',0):.2f})")

st.markdown("### Sample Datasets")
for d in get_datasets("population")[:10]:
    st.markdown(f"- **{d[0][:60]}** ({d[1]})")
st.caption("UK Data Stories | Population")
