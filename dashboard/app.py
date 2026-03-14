"""GovDataStory Dashboard - Main App."""

import streamlit as st

st.set_page_config(
    page_title="GovDataStory",
    page_icon="🇬🇧",
    layout="wide"
)

from dashboard.components import (
    get_record_counts,
    get_latest_story,
    get_insights,
    build_donut_chart,
    render_footer,
    render_story_card,
    render_insight_cards,
)

st.title("🇬🇧 GovDataStory")
st.markdown("UK Government Data — Narrative Intelligence Dashboard")

# Metrics
sources, topics = get_record_counts()
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Records", sum(sources.values()))
with col2:
    st.metric("Sources", len(sources))
with col3:
    st.metric("Topics", len(topics))
with col4:
    st.metric("Top Topic", list(topics.keys())[0] if topics else "N/A")

# Top insights across all topics
st.subheader("🔑 Top Insights")
insights = get_insights(limit=8)
if insights:
    render_insight_cards(insights)
else:
    st.info("Run the analysis pipeline to generate insights.")

# Cross-topic insights
st.subheader("🔗 Cross-Topic Patterns")
cross_insights = get_insights(topic="cross_topic", limit=5)
if cross_insights:
    render_insight_cards(cross_insights)
else:
    st.info("No cross-topic patterns detected yet.")

# Latest stories
st.subheader("📰 Latest Stories")
for topic in list(topics.keys())[:5]:
    story = get_latest_story(topic)
    if story and story.get("headline"):
        render_story_card(story)

# Distribution charts
col1, col2 = st.columns(2)
with col1:
    st.subheader("Records by Topic")
    fig = build_donut_chart(topics, "Topic Distribution")
    st.plotly_chart(fig, use_container_width=True)
with col2:
    st.subheader("Records by Source")
    fig = build_donut_chart(sources, "Source Distribution")
    st.plotly_chart(fig, use_container_width=True)

render_footer()
