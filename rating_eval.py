#!/usr/bin/env python3
"""Evaluate Glicko-2 parameter sets on held-out matches."""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

from rating import (
    DEFAULT_RD,
    DEFAULT_RATING,
    RATING_SCALE,
    RawMatch,
    build_period_index,
    fetch_matches,
    glicko_E,
    group_matches_by_period,
    run_glicko,
)

DB_PATH = "data.db"
EPSILON = 1e-12
DEFAULT_TAU = 0.5


@dataclass
class EvaluationResult:
    tau: float
    matches: int
    log_loss: float
    brier: float
    accuracy: float

    def as_dict(self) -> Dict[str, float]:
        return {
            "tau": self.tau,
            "matches": self.matches,
            "log_loss": self.log_loss,
            "brier": self.brier,
            "accuracy": self.accuracy,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare multiple Glicko-2 tau values by scoring held-out matches."
    )
    parser.add_argument(
        "--tau",
        dest="taus",
        type=float,
        action="append",
        help="Tau values to evaluate (repeat flag); defaults to 0.5 if omitted.",
    )
    parser.add_argument(
        "--train-end",
        type=_valid_timestamp,
        required=True,
        help="Inclusive upper bound (YYYY-MM-DD or ISO) for matches used to fit ratings.",
    )
    parser.add_argument(
        "--eval-start",
        type=_valid_timestamp,
        help="Inclusive lower bound for evaluation matches; defaults to train-end + 1 day.",
    )
    parser.add_argument(
        "--eval-end",
        type=_valid_timestamp,
        help="Optional inclusive upper bound for evaluation matches.",
    )
    parser.add_argument(
        "--start-date",
        type=_valid_timestamp,
        help="Optional global lower bound on matches considered (train + eval).",
    )
    parser.add_argument(
        "--end-date",
        type=_valid_timestamp,
        help="Optional global upper bound on matches considered (train + eval).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to write the evaluation summary JSON.",
    )
    return parser.parse_args()


def _valid_timestamp(value: str) -> datetime:
    sanitized = value.strip()
    if not sanitized:
        raise argparse.ArgumentTypeError("Date/time values cannot be empty.")
    normalized = sanitized.replace("z", "Z")
    try:
        dt = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date/time '{value}'. Use YYYY-MM-DD or full ISO-8601."
        ) from exc
    return dt


def _strip_timezone(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def ensure_eval_start(train_end: datetime, eval_start: Optional[datetime]) -> datetime:
    if eval_start and eval_start <= train_end:
        raise SystemExit("eval-start must be after train-end.")
    if eval_start:
        return eval_start
    return train_end + timedelta(seconds=1)


def clamp_probability(value: float) -> float:
    return max(min(value, 1 - EPSILON), EPSILON)


def partition_matches(
    matches: Sequence[RawMatch],
    train_end: datetime,
    eval_start: datetime,
    eval_end: Optional[datetime],
) -> tuple[list[RawMatch], list[RawMatch]]:
    train: List[RawMatch] = []
    eval_matches: List[RawMatch] = []
    for match in matches:
        match_dt = _strip_timezone(datetime.fromisoformat(match.occurred_at))
        if match_dt <= train_end:
            train.append(match)
        elif match_dt >= eval_start and (eval_end is None or match_dt <= eval_end):
            eval_matches.append(match)
    return train, eval_matches


def build_states(
    tau: float,
    train_matches: Sequence[RawMatch],
) -> Dict[str, Dict[str, Dict[str, float]]]:
    if not train_matches:
        return {}
    periods, lookup = build_period_index(train_matches)
    if not periods:
        return {}
    grouped = group_matches_by_period(train_matches, lookup)
    return run_glicko(tau, grouped, periods)


def get_state(
    weight_states: Dict[str, Dict[str, Dict[str, float]]],
    weight_class: str,
    wrestler_id: str,
) -> Dict[str, float]:
    return weight_states.get(weight_class, {}).get(
        wrestler_id,
        {"rating": DEFAULT_RATING, "rd": DEFAULT_RD},
    )


def probability_of_victory(
    winner_state: Dict[str, float],
    loser_state: Dict[str, float],
) -> float:
    mu = (winner_state["rating"] - DEFAULT_RATING) / RATING_SCALE
    mu_opp = (loser_state["rating"] - DEFAULT_RATING) / RATING_SCALE
    phi_opp = loser_state["rd"] / RATING_SCALE
    return glicko_E(mu, mu_opp, phi_opp)


def evaluate_matches(
    weight_states: Dict[str, Dict[str, Dict[str, float]]],
    eval_matches: Sequence[RawMatch],
) -> EvaluationResult:
    total = 0
    log_loss = 0.0
    brier = 0.0
    accuracy = 0.0

    for match in eval_matches:
        weight_class = match.weight_class
        if not weight_class:
            continue
        winner_state = get_state(weight_states, weight_class, match.winner_id)
        loser_state = get_state(weight_states, weight_class, match.loser_id)
        prob = clamp_probability(probability_of_victory(winner_state, loser_state))
        log_loss += -math.log(prob)
        brier += (1.0 - prob) ** 2
        accuracy += 1.0 if prob >= 0.5 else 0.0
        total += 1

    if total == 0:
        return EvaluationResult(tau=math.nan, matches=0, log_loss=math.inf, brier=math.inf, accuracy=0.0)

    return EvaluationResult(
        tau=0.0,
        matches=total,
        log_loss=log_loss / total,
        brier=brier / total,
        accuracy=accuracy / total,
    )


def main() -> None:
    args = parse_args()
    taus = sorted({round(value, 6) for value in (args.taus or [DEFAULT_TAU])})
    train_end = _strip_timezone(args.train_end)
    eval_start = ensure_eval_start(train_end, _strip_timezone(args.eval_start) if args.eval_start else None)
    eval_end = _strip_timezone(args.eval_end) if args.eval_end else None
    if eval_end and eval_end <= train_end:
        raise SystemExit("eval-end must be after train-end.")

    conn = sqlite3.connect(DB_PATH)
    try:
        matches = fetch_matches(conn, args.start_date, args.end_date)
    finally:
        conn.close()

    if not matches:
        raise SystemExit("No matches found for the provided filters.")

    train_matches, eval_matches = partition_matches(matches, train_end, eval_start, eval_end)
    if not train_matches:
        raise SystemExit("No training matches fall on/before train-end.")
    if not eval_matches:
        raise SystemExit("No evaluation matches fall inside the requested range.")

    results: List[EvaluationResult] = []
    for tau in taus:
        states = build_states(tau, train_matches)
        result = evaluate_matches(states, eval_matches)
        result.tau = tau
        results.append(result)
        print(
            f"tau={tau:.3f} -> matches={result.matches}, log_loss={result.log_loss:.4f}, "
            f"brier={result.brier:.4f}, accuracy={result.accuracy:.3f}"
        )

    if args.output:
        payload = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "train_end": train_end.isoformat(),
            "eval_start": eval_start.isoformat(),
            "eval_end": eval_end.isoformat() if eval_end else None,
            "taus": taus,
            "results": [res.as_dict() for res in results],
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
        print(f"Saved evaluation summary to {args.output}")


if __name__ == "__main__":
    main()
