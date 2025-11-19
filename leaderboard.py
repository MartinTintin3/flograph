#!/usr/bin/env python3
"""Generate conservative (rating - 2 * RD) leaderboards for every weight class."""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Any, Dict, List, Optional

DB_PATH = "data.db"
DEFAULT_OUTPUT = Path("build/leaderboards.json")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create per-weight leaderboards ranked by the conservative score "
            "rating - 2 * RD using the persisted ratings table."
        )
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Where to write the leaderboard JSON (default: {DEFAULT_OUTPUT}).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Maximum wrestlers to keep per weight class (default: 25; use 0 for all).",
    )
    parser.add_argument(
        "--min-last-updated",
        dest="min_last_updated",
        type=_parse_cli_date,
        help="Only include wrestlers whose last_updated is on or after this YYYY-MM-DD date.",
    )
    return parser.parse_args()


def _parse_cli_date(value: str) -> date:
    sanitized = value.strip()
    if not sanitized:
        raise argparse.ArgumentTypeError("Dates cannot be empty.")
    try:
        return datetime.strptime(sanitized, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}'. Use YYYY-MM-DD."
        ) from exc


def fetch_ratings(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            r.wrestler_id,
            r.weight_class,
            r.rating,
            r.rd,
            r.volatility,
            r.last_updated,
            w.name
        FROM ratings r
        LEFT JOIN wrestlers w ON w.id = r.wrestler_id
        ORDER BY r.weight_class ASC
        """
    )
    return cursor.fetchall()


def conservative_score(row: sqlite3.Row) -> float:
    rating = float(row["rating"] or 0)
    rd = float(row["rd"] or 0)
    return rating - (2.0 * rd)


def format_weight_key(weight: str) -> float:
    try:
        return float(weight)
    except (ValueError, TypeError):
        return math.inf


def build_leaderboards(
    rows: List[sqlite3.Row],
    limit: int,
    min_last_updated: Optional[date],
) -> Dict[str, List[Dict[str, Any]]]:
    weights: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        weight_class = row["weight_class"]
        if weight_class is None:
            continue
        last_updated_raw = row["last_updated"]
        if min_last_updated is not None:
            if not last_updated_raw:
                continue
            try:
                row_date = datetime.strptime(last_updated_raw, "%Y-%m-%d").date()
            except ValueError:
                continue
            if row_date < min_last_updated:
                continue
        score = conservative_score(row)
        weights[weight_class].append(
            {
                "wrestler_id": row["wrestler_id"],
                "name": row["name"] or "",
                "rating": round(float(row["rating"] or 0), 3),
                "rd": round(float(row["rd"] or 0), 3),
                "volatility": round(float(row["volatility"] or 0), 6),
                "conservative_rating": round(score, 3),
                "last_updated": last_updated_raw,
            }
        )

    ordered_weights = dict(
        sorted(weights.items(), key=lambda item: (format_weight_key(item[0]), item[0]))
    )

    for weight_class, wrestlers in ordered_weights.items():
        wrestlers.sort(key=lambda entry: entry["conservative_rating"], reverse=True)
        if limit > 0:
            ordered_weights[weight_class] = wrestlers[:limit]

    return ordered_weights


def write_output(output_path: Path, payload: Dict[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def main() -> None:
    args = parse_args()
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = fetch_ratings(conn)
    finally:
        conn.close()

    if not rows:
        raise SystemExit("No ratings found. Run rating.py first to populate the ratings table.")

    leaderboards = build_leaderboards(rows, args.limit, args.min_last_updated)
    payload: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "method": "rating - 2 * RD",
        "limit_per_weight": args.limit,
        "min_last_updated": args.min_last_updated.isoformat() if args.min_last_updated else None,
        "weight_classes": leaderboards,
    }
    write_output(args.output, payload)
    total_entries = sum(len(entries) for entries in leaderboards.values())
    print(
        f"Saved {len(leaderboards)} weight-class leaderboards "
        f"({total_entries} wrestler-weight rows) -> {args.output}"
    )


if __name__ == "__main__":
    main()
