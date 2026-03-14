"""Apriori association rule mining for GovDataStory."""

import json
import logging
from datetime import datetime, timezone

import duckdb
import pandas as pd
from mlxtend.frequent_patterns import apriori, association_rules

logger = logging.getLogger(__name__)
DB_PATH = "data/govdatastory.duckdb"


def mine_association_rules(run_id: str, conn=None, min_support: float = 0.05, min_confidence: float = 0.5) -> list[dict]:
    own = conn is None
    if own:
        conn = duckdb.connect(DB_PATH, read_only=False)

    # Build transaction matrix: each dataset is a transaction with topic + keywords as items
    rows = conn.execute(
        "SELECT id, topic, keywords, organization FROM records WHERE topic IS NOT NULL"
    ).fetchall()

    if len(rows) < 20:
        if own:
            conn.close()
        return []

    # Build binary transaction DataFrame
    items = set()
    transactions = []
    for rec_id, topic, keywords, org in rows:
        basket = {f"topic:{topic}"}
        if org:
            basket.add(f"org:{org}")
        if keywords:
            kw_list = json.loads(keywords) if isinstance(keywords, str) else (keywords or [])
            for kw in kw_list[:5]:  # limit keywords per record
                basket.add(f"kw:{kw}")
        items.update(basket)
        transactions.append(basket)

    # Convert to binary DataFrame
    item_list = sorted(items)
    matrix = pd.DataFrame(
        [[item in txn for item in item_list] for txn in transactions],
        columns=item_list,
    )

    # Run Apriori
    freq = apriori(matrix, min_support=min_support, use_colnames=True)
    if freq.empty:
        if own:
            conn.close()
        return []

    rules = association_rules(freq, metric="confidence", min_threshold=min_confidence)
    if rules.empty:
        if own:
            conn.close()
        return []

    # Convert to insight dicts
    insights = []
    now = datetime.now(timezone.utc).isoformat()

    for _, rule in rules.nlargest(50, "lift").iterrows():
        ant = ", ".join(sorted(rule["antecedents"]))
        con = ", ".join(sorted(rule["consequents"]))
        insight = {
            "type": "association_rule",
            "antecedent": ant,
            "consequent": con,
            "support": round(float(rule["support"]), 4),
            "confidence": round(float(rule["confidence"]), 4),
            "lift": round(float(rule["lift"]), 4),
            "description": f"{ant} → {con} (support={rule['support']:.2f}, confidence={rule['confidence']:.2f}, lift={rule['lift']:.1f})",
        }
        insights.append(insight)

    conn.execute(
        "INSERT OR REPLACE INTO analysis_results (id, topic, metric, value, run_id, created_at) VALUES (?,?,?,?,?,?)",
        [f"association_rules_{run_id}", "cross_topic", "association_rules", json.dumps(insights), run_id, now],
    )

    if own:
        conn.close()
    logger.info("Mined %d association rules for run %s", len(insights), run_id)
    return insights
