"""Semantic topic clustering with adaptive cluster count via silhouette optimization."""

import logging

import duckdb
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.metrics.pairwise import cosine_similarity

logger = logging.getLogger(__name__)

DB_PATH = "data/govdatastory.duckdb"

INIT_SQL = """
CREATE TABLE IF NOT EXISTS topic_clusters (
    id VARCHAR PRIMARY KEY,
    record_id VARCHAR NOT NULL,
    keyword_topic VARCHAR,
    cluster_id INTEGER NOT NULL,
    cluster_label VARCHAR,
    similarity_score DOUBLE,
    run_id VARCHAR NOT NULL
)
"""


def _find_optimal_k(matrix, k_min: int = 5, k_max: int = 25) -> int:
    """Find optimal k using silhouette score."""
    n = matrix.shape[0]
    k_max = min(k_max, n - 1)
    if k_max <= k_min:
        return k_min

    best_k, best_score = k_min, -1
    for k in range(k_min, k_max + 1, 2):  # step by 2 for speed
        km = KMeans(n_clusters=k, random_state=42, n_init=5, max_iter=100)
        labels = km.fit_predict(matrix)
        score = silhouette_score(matrix, labels, sample_size=min(2000, n))
        logger.debug(f"k={k}, silhouette={score:.4f}")
        if score > best_score:
            best_score, best_k = score, k

    logger.info(f"Optimal k={best_k} (silhouette={best_score:.4f})")
    return best_k


def run_clustering(n_clusters: int = None, run_id: str = "latest") -> dict:
    """Cluster records by TF-IDF. If n_clusters is None, auto-select via silhouette."""
    conn = duckdb.connect(DB_PATH, read_only=False)
    conn.execute(INIT_SQL)

    rows = conn.execute("SELECT id, title, description, topic FROM records").fetchall()
    if len(rows) < 10:
        conn.close()
        return {"error": "not enough records"}

    ids = [r[0] for r in rows]
    texts = [f"{r[1] or ''} {r[2] or ''}" for r in rows]
    kw_topics = [r[3] for r in rows]

    tfidf = TfidfVectorizer(max_features=5000, stop_words="english", min_df=2)
    matrix = tfidf.fit_transform(texts)

    # Adaptive k selection
    if n_clusters is None:
        n_clusters = _find_optimal_k(matrix)

    km = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    labels = km.fit_predict(matrix)

    # Cluster labels from top terms
    terms = tfidf.get_feature_names_out()
    cluster_labels = {}
    for c in range(n_clusters):
        top_idx = km.cluster_centers_[c].argsort()[-3:][::-1]
        cluster_labels[c] = ", ".join(terms[i] for i in top_idx)

    sims = cosine_similarity(matrix, km.cluster_centers_)

    # Store assignments
    conn.execute("DELETE FROM topic_clusters WHERE run_id = ?", [run_id])
    for i, rec_id in enumerate(ids):
        c = int(labels[i])
        conn.execute("""
            INSERT INTO topic_clusters (id, record_id, keyword_topic, cluster_id, cluster_label, similarity_score, run_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [f"{rec_id}_{run_id}", rec_id, kw_topics[i], c, cluster_labels[c], float(sims[i, c]), run_id])

    # Topic similarity matrix
    topic_names = sorted(set(kw_topics))
    topic_vectors = []
    for t in topic_names:
        mask = [i for i, kt in enumerate(kw_topics) if kt == t]
        if mask:
            topic_vectors.append(np.asarray(matrix[mask].mean(axis=0)).flatten())
        else:
            topic_vectors.append(np.zeros(matrix.shape[1]))
    sim_matrix = cosine_similarity(topic_vectors)

    sil = silhouette_score(matrix, labels, sample_size=min(2000, len(ids)))

    conn.close()

    result = {
        "n_records": len(ids),
        "n_clusters": n_clusters,
        "silhouette_score": round(float(sil), 4),
        "cluster_labels": {str(k): v for k, v in cluster_labels.items()},
        "topic_similarity": {"topics": topic_names, "matrix": sim_matrix.tolist()},
    }
    logger.info(f"Clustered {len(ids)} records into {n_clusters} clusters (silhouette={sil:.4f})")
    return result
