"""Crime Page."""

import streamlit as st
from components import (
    get_datasets, get_latest_story, get_analysis, get_insights,
    build_trend_chart, render_story_card, render_insight_cards, render_footer,
)

st.set_page_config(page_title="Crime - GovDataStory", page_icon="🔍")
st.title("🔍 Crime")

story = get_latest_story("crime")
if story:
    render_story_card(story)

st.markdown("### Key Insights")
insights = get_insights("crime", limit=5)
if insights:
    render_insight_cards(insights)
else:
    st.info("Run the analysis pipeline to generate insights.")

st.markdown("### Publishing Trend")
analysis = get_analysis("crime")
ts = analysis.get("timeseries", {})
anom = analysis.get("anomalies", {})
if ts.get("months"):
    fig = build_trend_chart(ts["counts"], months=ts["months"], anomalies=anom.get("indices"), title="Crime Dataset Publishing")
    st.plotly_chart(fig, use_container_width=True)
    trend = analysis.get("trend", {})
    if trend.get("direction") != "unknown":
        st.caption(f"Trend: {trend['direction']} (slope={trend.get('slope', 0):.2f}/mo, R²={trend.get('r_squared', 0):.2f})")
else:
    st.info("No time-series data available yet.")

st.markdown("### Crime Datasets")
datasets = get_datasets("crime", limit=20)
if datasets:
    st.dataframe(datasets, use_container_width=True)

render_footer()
