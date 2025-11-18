# export_graph.py
import sqlite3
import json
from collections import defaultdict

DB_PATH = "data.db"
WEIGHT_CLASS = "138"  # change or loop over these

con = sqlite3.connect(DB_PATH)
con.row_factory = sqlite3.Row
cur = con.cursor()

# 1) Nodes = wrestlers in this weight with aggregated win/loss info
cur.execute(
    """
    SELECT
        w.id,
        w.name,
        SUM(CASE WHEN m.winner_id = w.id THEN 1 ELSE 0 END) AS wins,
        SUM(CASE WHEN m.winner_id != w.id THEN 1 ELSE 0 END) AS losses
    FROM wrestlers w
    JOIN matches m ON (m.topWrestler_id = w.id OR m.bottomWrestler_id = w.id)
    WHERE m.weightClass = ?
    GROUP BY w.id, w.name
    """,
    (WEIGHT_CLASS,),
)

nodes = []
for row in cur.fetchall():
    wins = row["wins"] or 0
    losses = row["losses"] or 0
    total = wins + losses
    win_pct = wins / total if total > 0 else 0
    nodes.append(
        {
            "id": row["id"],
            "name": row["name"],
            "wins": wins,
            "losses": losses,
            "matches": total,
            "winPct": win_pct,
        }
    )

# 2) Aggregated links: winner -> loser with counts
cur.execute(
    """
    SELECT
        winner_id AS source,
        CASE
            WHEN winner_id = topWrestler_id THEN bottomWrestler_id
            ELSE topWrestler_id
        END AS target,
        COUNT(*) AS count
    FROM matches
    WHERE weightClass = ?
    GROUP BY source, target
    """,
    (WEIGHT_CLASS,),
)

links = []
for row in cur.fetchall():
    if row["source"] is None or row["target"] is None:
        continue
    links.append(
        {
            "source": row["source"],
            "target": row["target"],
            "count": row["count"],
        }
    )

graph = {"nodes": nodes, "links": links}

with open("graph.json", "w", encoding="utf-8") as f:
    json.dump(graph, f)

print(f"Exported {len(nodes)} nodes and {len(links)} links to graph.json")
