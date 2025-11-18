import argparse
import sqlite3
import json
from datetime import datetime
import networkx as nx

import math

DB_PATH = "data.db"


def _parse_cli_timestamp(value: str) -> datetime:
    sanitized = value.strip()
    if not sanitized:
        raise argparse.ArgumentTypeError("Date/time values cannot be empty.")
    normalized = sanitized.replace("z", "Z")
    try:
        return datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date/time '{value}'. Use YYYY-MM-DD or a full ISO-8601 timestamp."
        ) from exc


def _valid_timestamp(value: str) -> datetime:
    return _parse_cli_timestamp(value)


def _format_cli_timestamp(dt: datetime) -> str:
    return dt.isoformat()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export a wrestler match graph for a given weight class, optionally filtered by date range."
    )
    parser.add_argument(
        "-w",
        "--weight-class",
        dest="weight_classes",
        action="append",
        help="Specify a weight class to include. Repeat or provide comma-separated values. Use '*' or omit the flag to include all.",
    )
    parser.add_argument(
        "--start-date",
        type=_valid_timestamp,
        help="Only include matches on or after this timestamp (YYYY-MM-DD or ISO-8601)",
    )
    parser.add_argument(
        "--end-date",
        type=_valid_timestamp,
        help="Only include matches on or before this timestamp (YYYY-MM-DD or ISO-8601)",
    )
    return parser.parse_args()


def build_match_filters(weight_classes=None, start_date=None, end_date=None):
    filters = []
    params = []

    if weight_classes:
        placeholders = ",".join(["?"] * len(weight_classes))
        filters.append(f"m.weightClass IN ({placeholders})")
        params.extend(weight_classes)

    if start_date:
        filters.append("m.date >= ?")
        params.append(start_date)
    if end_date:
        filters.append("m.date <= ?")
        params.append(end_date)

    if not filters:
        filters.append("1=1")

    return " AND ".join(filters), params


def normalize_weight_classes(raw_values):
    if not raw_values:
        return None

    normalized = []
    for value in raw_values:
        if value is None:
            continue
        parts = [part.strip() for part in value.split(",") if part.strip()]
        if any(part == "*" for part in parts):
            return None
        normalized.extend(parts)

    return normalized or None


def win_pct_to_color(win_pct):
    """Convert win percentage to color: red (0%) to green (100%)"""
    hue = win_pct * 120
    h = hue / 60
    c = 1.0
    x = c * (1 - abs(h % 2 - 1))

    if h < 1:
        r, g, b = c, x, 0
    elif h < 2:
        r, g, b = x, c, 0
    elif h < 3:
        r, g, b = 0, c, x
    elif h < 4:
        r, g, b = 0, x, c
    elif h < 5:
        r, g, b = x, 0, c
    else:
        r, g, b = c, 0, x

    r, g, b = int(r * 255), int(g * 255), int(b * 255)
    return f"#{r:02x}{g:02x}{b:02x}"


def calculate_size(matches, stats_lookup, min_size=2, max_size=15):
    """Calculate node size based on total matches"""
    if not stats_lookup:
        return min_size

    all_matches = [s["matches"] for s in stats_lookup.values()]
    min_matches = min(all_matches) if all_matches else 1
    max_matches = max(all_matches) if all_matches else 1

    if max_matches == min_matches:
        return (min_size + max_size) / 2

    normalized = (matches - min_matches) / (max_matches - min_matches)
    return min_size + normalized * (max_size - min_size)


def main():
    args = parse_args()

    if args.start_date and args.end_date and args.start_date > args.end_date:
        raise SystemExit("start-date must be less than or equal to end-date")

    start_arg = _format_cli_timestamp(args.start_date) if args.start_date else None
    end_arg = _format_cli_timestamp(args.end_date) if args.end_date else None

    weight_classes = normalize_weight_classes(args.weight_classes)

    if weight_classes:
        print("Starting export for weight classes:", ", ".join(weight_classes))
    else:
        print("Starting export for all weight classes...")
    if start_arg or end_arg:
        print(
            "Applying date filter:",
            f"{start_arg or '-∞'} to {end_arg or '+∞'}",
        )
    print("Connecting to database...")

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    where_clause, where_params = build_match_filters(
        weight_classes, start_arg, end_arg
    )

    # 1) Nodes = wrestlers in this weight with aggregated win/loss info
    print("Fetching wrestler data...")
    cur.execute(
        f"""
        SELECT
            w.id,
            w.name,
            SUM(CASE WHEN m.winner_id = w.id THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN m.winner_id != w.id THEN 1 ELSE 0 END) AS losses
        FROM wrestlers w
        JOIN matches m ON (m.topWrestler_id = w.id OR m.bottomWrestler_id = w.id)
        WHERE {where_clause}
        GROUP BY w.id, w.name
        """,
        where_params,
    )

    wrestler_stats = {}
    for row in cur.fetchall():
        wins = row["wins"] or 0
        losses = row["losses"] or 0
        total = wins + losses
        win_pct = wins / total if total > 0 else 0
        wrestler_stats[row["id"]] = {
            "name": row["name"],
            "wins": wins,
            "losses": losses,
            "matches": total,
            "winPct": win_pct,
        }

    print(f"Found {len(wrestler_stats)} wrestlers")

    # 2) Aggregated links: winner -> loser with counts
    print("Fetching match edges...")
    cur.execute(
        f"""
        SELECT
            winner_id AS source,
            CASE
                WHEN winner_id = topWrestler_id THEN bottomWrestler_id
                ELSE topWrestler_id
            END AS target,
            COUNT(*) AS count
        FROM matches m
        WHERE {where_clause}
        GROUP BY source, target
        """,
        where_params,
    )

    edges_data = []
    for row in cur.fetchall():
        if row["source"] is None or row["target"] is None:
            continue
        edges_data.append(
            {
                "source": row["source"],
                "target": row["target"],
                "count": row["count"],
            }
        )

    print(f"Found {len(edges_data)} edges")

    # 3) Build NetworkX graph for layout calculation
    print("Building NetworkX graph...")
    G = nx.DiGraph()

    # Add nodes with their IDs
    for wrestler_id in wrestler_stats.keys():
        G.add_node(wrestler_id)

    # Add edges
    for edge in edges_data:
        G.add_edge(edge["source"], edge["target"], weight=edge["count"])

    print(f"Graph has {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")

    # 4) Calculate spring layout positions
    print("Calculating spring layout (this may take a moment)...")
    pos = nx.spring_layout(G, k=100 / math.sqrt(G.number_of_nodes()), iterations=50, seed=42, method="force",)
    print("Layout calculation complete!")

    print("Building output nodes with attributes...")
    nodes = []
    for wrestler_id, stats in wrestler_stats.items():
        x, y = pos[wrestler_id]
        color = win_pct_to_color(stats["winPct"])
        size = calculate_size(stats["matches"], wrestler_stats)

        nodes.append({
            "id": wrestler_id,
            "attributes": {
                "label": stats["name"],
                "x": x,
                "y": y,
                "color": color,
                "size": size,
                "wins": stats["wins"],
                "losses": stats["losses"],
            }
        })

    # 8) Build output edges with new format
    print("Building output edges...")
    edges = []
    for edge_data in edges_data:
        source = edge_data["source"]
        target = edge_data["target"]
        key = f"{source}>{target}"

        edges.append({
            "key": key,
            "source": source,
            "target": target,
            "attributes": {
                "type": "arrow"
            }
        })

    # 9) Create final graph structure
    print("Creating final graph structure...")
    graph = {"nodes": nodes, "edges": edges}

    print("Writing to graph.json...")

    with open("graph.json", "w", encoding="utf-8") as f:
        json.dump(graph, f, indent=2)

    print(f"\n✓ Successfully exported {len(nodes)} nodes and {len(edges)} edges to graph.json")
    print(f"Layout: spring_layout with k=2, iterations=50")
    print(f"Node colors: red (low win %) to green (high win %)")
    print(f"Node sizes: based on total matches played")


if __name__ == "__main__":
    main()
