#!/usr/bin/env python3
"""Compute Glicko-2 ratings for every numbered weight class."""

from __future__ import annotations

import argparse
import json
import math
import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import DefaultDict, Dict, Iterable, List, Optional, Sequence, Tuple

DB_PATH = "data.db"
RATING_SCALE = 173.7178
DEFAULT_RATING = 1500.0
DEFAULT_RD = 350.0
DEFAULT_VOLATILITY = 0.06
MAX_RD = 350.0
DEFAULT_TAU = 0.5
EPSILON = 1e-6

date_regex = re.compile(r"\d+(?:\.\d+)?")


@dataclass(frozen=True)
class RawMatch:
    period_key: date
    weight_class: str
    winner_id: str
    loser_id: str
    occurred_at: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Replay historical matches with Glicko-2, generating ratings per "
            "wrestler/weight."
        )
    )
    parser.add_argument(
        "--tau",
        dest="taus",
        type=float,
        action="append",
        help=(
            "Volatility constraint(s) to evaluate. Repeat the flag to compare "
            "multiple values in a single run. Default: 0.5"
        ),
    )
    parser.add_argument(
        "--persist-tau",
        dest="persist_tau",
        type=float,
        help=(
            "Persist the results for this tau into the ratings table. The "
            "value must also appear in --tau (or be the default)."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default="build",
        help="Directory where JSON snapshots should be written. Default: build",
    )
    parser.add_argument(
        "--start-date",
        type=_valid_timestamp,
        help="Only include matches on or after this timestamp (YYYY-MM-DD or ISO).",
    )
    parser.add_argument(
        "--end-date",
        type=_valid_timestamp,
        help="Only include matches on or before this timestamp (YYYY-MM-DD or ISO).",
    )
    return parser.parse_args()


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


def parse_db_timestamp(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    sanitized = raw.strip()
    if not sanitized:
        return None
    normalized = sanitized.replace("z", "Z")
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def month_floor(value: datetime) -> date:
    return date(value.year, value.month, 1)


def add_month(value: date) -> date:
    year = value.year
    month = value.month + 1
    if month == 13:
        month = 1
        year += 1
    return date(year, month, 1)


def normalize_weight_label(raw_label: Optional[str]) -> Optional[str]:
    if not raw_label:
        return None
    tokens = date_regex.findall(raw_label)
    for token in tokens:
        digits_only = token.replace(".", "")
        if len(digits_only) < 2:
            continue
        if "." in token:
            cleaned = token.lstrip("0")
            if cleaned.startswith("."):
                cleaned = "0" + cleaned
            return cleaned or "0"
        return str(int(token))
    return None


def fetch_wrestler_names(conn: sqlite3.Connection) -> Dict[str, str]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM wrestlers")
    rows = cur.fetchall()
    return {row["id"]: row["name"] or "" for row in rows}


def fetch_matches(
    conn: sqlite3.Connection,
    start_date: Optional[datetime],
    end_date: Optional[datetime],
) -> List[RawMatch]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    filters = [
        "winner_id IS NOT NULL",
        "topWrestler_id IS NOT NULL",
        "bottomWrestler_id IS NOT NULL",
        "weightClass IS NOT NULL",
        "date IS NOT NULL",
    ]
    params: List[str] = []
    if start_date:
        filters.append("date >= ?")
        params.append(start_date.isoformat())
    if end_date:
        filters.append("date <= ?")
        params.append(end_date.isoformat())

    where_clause = " AND ".join(filters)
    cur.execute(
        f"""
        SELECT id, date, weightClass, winner_id, topWrestler_id, bottomWrestler_id
        FROM matches
        WHERE {where_clause}
        ORDER BY date ASC
        """,
        params,
    )

    raw_matches: List[RawMatch] = []
    skipped = 0
    for row in cur.fetchall():
        winner_id = row["winner_id"]
        top_id = row["topWrestler_id"]
        bottom_id = row["bottomWrestler_id"]
        if winner_id is None or top_id is None or bottom_id is None:
            skipped += 1
            continue

        if winner_id == top_id:
            loser_id = bottom_id
        elif winner_id == bottom_id:
            loser_id = top_id
        else:
            # Winner mismatch; skip the row to keep data clean.
            skipped += 1
            continue

        if loser_id is None or loser_id == winner_id:
            skipped += 1
            continue

        normalized_weight = normalize_weight_label(row["weightClass"])
        if not normalized_weight:
            skipped += 1
            continue

        match_ts = parse_db_timestamp(row["date"])
        if match_ts is None:
            skipped += 1
            continue

        raw_matches.append(
            RawMatch(
                period_key=month_floor(match_ts),
                weight_class=normalized_weight,
                winner_id=winner_id,
                loser_id=loser_id,
                occurred_at=match_ts.isoformat(),
            )
        )

    if skipped:
        print(f"Skipped {skipped} matches due to missing or invalid data.")

    return raw_matches


def build_period_index(matches: Sequence[RawMatch]) -> Tuple[List[date], Dict[date, int]]:
    if not matches:
        return [], {}
    first = min(match.period_key for match in matches)
    last = max(match.period_key for match in matches)
    periods: List[date] = []
    current = date(first.year, first.month, 1)
    while current <= last:
        periods.append(current)
        current = add_month(current)
    lookup = {period: idx for idx, period in enumerate(periods)}
    return periods, lookup


def group_matches_by_period(
    matches: Sequence[RawMatch],
    lookup: Dict[date, int],
) -> Dict[int, DefaultDict[str, List[RawMatch]]]:
    grouped: Dict[int, DefaultDict[str, List[RawMatch]]] = {}
    for match in matches:
        period_idx = lookup[match.period_key]
        period_bucket = grouped.setdefault(period_idx, defaultdict(list))
        period_bucket[match.weight_class].append(match)
    return grouped


def ensure_state(states: Dict[str, Dict[str, float]], wrestler_id: str, period_idx: int) -> Dict[str, float]:
    if wrestler_id not in states:
        states[wrestler_id] = {
            "rating": DEFAULT_RATING,
            "rd": DEFAULT_RD,
            "volatility": DEFAULT_VOLATILITY,
            "last_period_index": period_idx,
            "last_competed_period": None,
            "matches_played": 0,
        }
    return states[wrestler_id]


def apply_inactivity(state: Dict[str, float], target_period: int) -> None:
    last_period = int(state.get("last_period_index", 0))
    if target_period <= last_period:
        return
    delta = target_period - last_period
    phi = state["rd"] / RATING_SCALE
    sigma = state["volatility"]
    phi = math.sqrt(phi * phi + delta * sigma * sigma)
    state["rd"] = min(phi * RATING_SCALE, MAX_RD)
    state["last_period_index"] = target_period


def build_snapshot(state: Dict[str, float]) -> Dict[str, float]:
    return {
        "rating": state["rating"],
        "rd": state["rd"],
        "volatility": state["volatility"],
        "mu": (state["rating"] - DEFAULT_RATING) / RATING_SCALE,
        "phi": state["rd"] / RATING_SCALE,
    }


def glicko_g(phi: float) -> float:
    return 1.0 / math.sqrt(1.0 + (3.0 * phi * phi) / (math.pi * math.pi))


def glicko_E(mu: float, mu_j: float, phi_j: float) -> float:
    return 1.0 / (1.0 + math.exp(-glicko_g(phi_j) * (mu - mu_j)))


def update_volatility(phi: float, sigma: float, delta: float, v: float, tau: float) -> float:
    a = math.log(sigma * sigma)

    def f(x: float) -> float:
        exp_x = math.exp(x)
        numerator = exp_x * (delta * delta - phi * phi - v - exp_x)
        denominator = 2.0 * (phi * phi + v + exp_x) ** 2
        return (numerator / denominator) - ((x - a) / (tau * tau))

    A = a
    if delta * delta > phi * phi + v:
        B = math.log(delta * delta - phi * phi - v)
    else:
        k = 1
        while f(a - k * tau) < 0:
            k += 1
        B = a - k * tau

    fA = f(A)
    fB = f(B)

    while abs(B - A) > EPSILON:
        C = A + (A - B) * fA / (fB - fA)
        fC = f(C)
        if fC * fB < 0:
            A = B
            fA = fB
        else:
            fA = fA / 2.0
        B = C
        fB = fC

    return math.exp(A / 2.0)


def update_player(
    snapshot: Dict[str, float],
    pairings: List[Tuple[str, float]],
    opponents: Dict[str, Dict[str, float]],
    tau: float,
) -> Dict[str, float]:
    if not pairings:
        return {
            "rating": snapshot["rating"],
            "rd": snapshot["rd"],
            "volatility": snapshot["volatility"],
        }

    mu = snapshot["mu"]
    phi = snapshot["phi"]
    sigma = snapshot["volatility"]

    v_inv = 0.0
    delta_sum = 0.0
    for opponent_id, score in pairings:
        opp = opponents[opponent_id]
        g = glicko_g(opp["phi"])
        E_val = glicko_E(mu, opp["mu"], opp["phi"])
        v_inv += (g ** 2) * E_val * (1.0 - E_val)
        delta_sum += g * (score - E_val)

    if v_inv == 0:
        return {
            "rating": snapshot["rating"],
            "rd": snapshot["rd"],
            "volatility": snapshot["volatility"],
        }

    v = 1.0 / v_inv
    delta = v * delta_sum
    new_sigma = update_volatility(phi, sigma, delta, v, tau)
    phi_star = math.sqrt(phi * phi + new_sigma * new_sigma)
    phi_prime = 1.0 / math.sqrt((1.0 / (phi_star * phi_star)) + (1.0 / v))
    mu_prime = mu + (phi_prime * phi_prime) * delta_sum

    rating = mu_prime * RATING_SCALE + DEFAULT_RATING
    rd = phi_prime * RATING_SCALE

    return {
        "rating": rating,
        "rd": min(rd, MAX_RD),
        "volatility": new_sigma,
    }


def format_period_label(period_idx: Optional[int], periods: Sequence[date]) -> Optional[str]:
    if period_idx is None or period_idx < 0 or period_idx >= len(periods):
        return None
    period_date = periods[period_idx]
    return period_date.strftime("%Y-%m")


def period_label_to_date(label: Optional[str]) -> Optional[str]:
    if not label:
        return None
    try:
        year, month = label.split("-", 1)
        return f"{int(year):04d}-{int(month):02d}-01"
    except ValueError:
        return None


def run_glicko(
    tau: float,
    grouped_matches: Dict[int, DefaultDict[str, List[RawMatch]]],
    periods: Sequence[date],
) -> Dict[str, Dict[str, Dict[str, float]]]:
    states: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
    total_periods = len(periods)
    for period_idx in range(total_periods):
        weight_groups = grouped_matches.get(period_idx)
        if not weight_groups:
            continue
        for weight_class, matches in weight_groups.items():
            weight_states = states[weight_class]
            per_player: DefaultDict[str, List[Tuple[str, float]]] = defaultdict(list)
            for match in matches:
                winner_state = ensure_state(weight_states, match.winner_id, period_idx)
                loser_state = ensure_state(weight_states, match.loser_id, period_idx)
                per_player[match.winner_id].append((match.loser_id, 1.0))
                per_player[match.loser_id].append((match.winner_id, 0.0))

            snapshots: Dict[str, Dict[str, float]] = {}
            for wrestler_id in per_player.keys():
                state = ensure_state(weight_states, wrestler_id, period_idx)
                apply_inactivity(state, period_idx)
                snapshots[wrestler_id] = build_snapshot(state)

            updates: Dict[str, Dict[str, float]] = {}
            for wrestler_id, pairings in per_player.items():
                updates[wrestler_id] = update_player(snapshots[wrestler_id], pairings, snapshots, tau)

            for wrestler_id, updated in updates.items():
                state = weight_states[wrestler_id]
                state["rating"] = updated["rating"]
                state["rd"] = updated["rd"]
                state["volatility"] = updated["volatility"]
                state["last_period_index"] = period_idx
                state["last_competed_period"] = period_idx
                state["matches_played"] += len(per_player[wrestler_id])

    final_idx = total_periods - 1
    for weight_states in states.values():
        for state in weight_states.values():
            apply_inactivity(state, final_idx)
    return states


def build_result_payload(
    tau: float,
    states: Dict[str, Dict[str, Dict[str, float]]],
    periods: Sequence[date],
    wrestler_names: Dict[str, str],
) -> Dict[str, object]:
    weights_payload: Dict[str, List[Dict[str, object]]] = {}
    for weight, wrestlers in states.items():
        entries: List[Dict[str, object]] = []
        for wrestler_id, state in wrestlers.items():
            entries.append(
                {
                    "wrestler_id": wrestler_id,
                    "name": wrestler_names.get(wrestler_id, ""),
                    "rating": round(state["rating"], 3),
                    "rd": round(state["rd"], 3),
                    "volatility": round(state["volatility"], 6),
                    "matches": int(state["matches_played"]),
                    "last_active_period": format_period_label(
                        state.get("last_competed_period"), periods
                    ),
                }
            )
        entries.sort(key=lambda item: item["rating"], reverse=True)
        weights_payload[weight] = entries

    sorted_weights = sorted(
        weights_payload.items(),
        key=lambda item: (float(item[0]) if _is_float(item[0]) else math.inf, item[0]),
    )

    ordered_payload = {weight: entries for weight, entries in sorted_weights}
    return {
        "tau": tau,
        "generated_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "period_start": periods[0].isoformat() if periods else None,
        "period_end": periods[-1].isoformat() if periods else None,
        "total_periods": len(periods),
        "weight_classes": ordered_payload,
    }


def _is_float(value: str) -> bool:
    try:
        float(value)
        return True
    except ValueError:
        return False


def persist_ratings(
    conn: sqlite3.Connection,
    payload: Dict[str, object],
) -> None:
    ensure_ratings_table(conn)

    weight_classes: Dict[str, List[Dict[str, object]]] = payload["weight_classes"]  # type: ignore[index]
    default_last_updated = payload["period_end"] or datetime.utcnow().date().isoformat()  # type: ignore[assignment]
    cursor = conn.cursor()
    cursor.execute("DELETE FROM ratings")

    rows: List[Tuple[str, str, float, float, float, str]] = []
    for weight, wrestlers in weight_classes.items():
        for wrestler in wrestlers:
            last_updated = period_label_to_date(wrestler.get("last_active_period")) or default_last_updated
            rows.append(
                (
                    wrestler["wrestler_id"],
                    weight,
                    wrestler["rating"],
                    wrestler["rd"],
                    wrestler["volatility"],
                    last_updated,
                )
            )

    cursor.executemany(
        """
        INSERT INTO ratings (wrestler_id, weight_class, rating, rd, volatility, last_updated)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()


def ensure_ratings_table(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS ratings (
            wrestler_id TEXT NOT NULL,
            weight_class TEXT NOT NULL,
            rating REAL,
            rd REAL,
            volatility REAL,
            last_updated TEXT,
            PRIMARY KEY (wrestler_id, weight_class)
        )
        """
    )
    conn.commit()


def write_payload(output_dir: Path, payload: Dict[str, object]) -> Path:
    tau = payload["tau"]
    file_name = f"glicko2_tau-{tau:.3f}.json".replace("/", "-")
    output_path = output_dir / file_name
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    return output_path


def main() -> None:
    args = parse_args()
    if args.start_date and args.end_date and args.start_date > args.end_date:
        raise SystemExit("start-date must be less than or equal to end-date")

    tau_values = args.taus or [DEFAULT_TAU]
    tau_values = sorted({round(value, 6) for value in tau_values})

    if args.persist_tau is not None and args.persist_tau not in tau_values:
        tau_values.append(args.persist_tau)
        tau_values = sorted({round(value, 6) for value in tau_values})

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    wrestler_names = fetch_wrestler_names(conn)
    raw_matches = fetch_matches(conn, args.start_date, args.end_date)
    if not raw_matches:
        raise SystemExit("No matches available for the provided filters.")

    periods, lookup = build_period_index(raw_matches)
    grouped_matches = group_matches_by_period(raw_matches, lookup)

    persisted_tau = args.persist_tau
    persisted_payload: Optional[Dict[str, object]] = None

    for tau in tau_values:
        states = run_glicko(tau, grouped_matches, periods)
        payload = build_result_payload(tau, states, periods, wrestler_names)
        output_path = write_payload(output_dir, payload)
        wrestler_count = sum(len(entries) for entries in payload["weight_classes"].values())
        print(
            f"[tau={tau:.3f}] {len(payload['weight_classes'])} weight classes, "
            f"{wrestler_count} wrestler-weight ratings -> {output_path}"
        )
        if persisted_tau is not None and math.isclose(tau, persisted_tau, rel_tol=0, abs_tol=1e-9):
            persisted_payload = payload

    if persisted_tau is not None and persisted_payload is not None:
        persist_ratings(conn, persisted_payload)
        print(
            f"Persisted tau={persisted_tau:.3f} results into the ratings table "
            f"({len(persisted_payload['weight_classes'])} weight classes)."
        )

    conn.close()


if __name__ == "__main__":
    main()
