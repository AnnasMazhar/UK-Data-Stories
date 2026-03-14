"""Graph-based insight discovery with Louvain community detection for GovDataStory."""

import json
import logging
from datetime import datetime, timezone

import duckdb
import networkx as nx
from community import community_louvain

logger = logging.getLogger(__name__)
DB_PATH = "data/govdatastory.duckdb"


def analyze_graph(run_id: str, conn=None) -> list[dict]:
    own = conn is None
    if own:
        conn = duckdb.connect(DB_PATH, read_only=False)

    G = nx.Graph()

    # Add topic nodes
    topics = conn.execute("SELECT DISTINCT topic FROM records WHERE topic IS NOT NULL").fetchall()
    for (topic,) in topics:
        G.add_node(topic, node_type="topic")

    # Add organization nodes and publishes edges
    org_rows = conn.execute(
        "SELECT DISTINCT organization, topic FROM records WHERE organization IS NOT NULL AND topic IS NOT NULL"
    ).fetchall()
    for org, topic in org_rows:
        G.add_node(org, node_type="organization")
        if G.has_edge(org, topic):
            G[org][topic]["weight"] += 1
        else:
            G.add_edge(org, topic, relation="publishes", weight=1)

    # Add co-occurrence edges between topics (shared organizations)
    topic_orgs = {}
    for org, topic in org_rows:
        topic_orgs.setdefault(topic, set()).add(org)

    topic_list = list(topic_orgs.keys())
    for i, t1 in enumerate(topic_list):
        for t2 in topic_list[i + 1:]:
            shared = topic_orgs[t1] & topic_orgs[t2]
            if shared:
                if G.has_edge(t1, t2):
                    G[t1][t2]["weight"] += len(shared)
                else:
                    G.add_edge(t1, t2, relation="co_occurs", weight=len(shared))

    if len(G.nodes) < 3:
        if own:
            conn.close()
        return []

    # Louvain community detection
    partition = community_louvain.best_partition(G, random_state=42)

    # Group nodes by community
    communities = {}
    for node, comm_id in partition.items():
        communities.setdefault(comm_id, []).append(node)

    # Build insights from communities with multiple topic nodes
    insights = []
    now = datetime.now(timezone.utc).isoformat()

    for comm_id, members in communities.items():
        topic_members = [m for m in members if G.nodes[m].get("node_type") == "topic"]
        if len(topic_members) < 2:
            continue

        insight = {
            "type": "graph_community",
            "community_id": comm_id,
            "topics": topic_members,
            "all_members": members,
            "size": len(members),
            "description": f"{', '.join(t.title() for t in topic_members)} form a strong policy cluster",
            "confidence": round(min(len(topic_members) / len(topic_list), 0.95), 4),
            "evidence": {
                "n_topics": len(topic_members),
                "n_orgs": len([m for m in members if G.nodes[m].get("node_type") == "organization"]),
                "modularity": round(community_louvain.modularity(partition, G), 4),
            },
        }
        insights.append(insight)

    # Store graph stats
    graph_data = {
        "n_nodes": G.number_of_nodes(),
        "n_edges": G.number_of_edges(),
        "n_communities": len(communities),
        "modularity": round(community_louvain.modularity(partition, G), 4),
        "communities": {str(k): v for k, v in communities.items()},
        "insights": insights,
    }
    conn.execute(
        "INSERT OR REPLACE INTO analysis_results (id, topic, metric, value, run_id, created_at) VALUES (?,?,?,?,?,?)",
        [f"graph_analysis_{run_id}", "cross_topic", "graph_analysis", json.dumps(graph_data), run_id, now],
    )

    if own:
        conn.close()
    logger.info("Graph analysis: %d communities, %d insights for run %s", len(communities), len(insights), run_id)
    return insights
