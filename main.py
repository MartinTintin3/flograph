import argparse
from datetime import datetime

import download_util
import db

DEFAULT_SEED = "064ad7f4-8d16-4dd2-94b1-1dd1c45c3832"

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


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Crawl FloArena wrestler graph")
	parser.add_argument("--seed", default=DEFAULT_SEED, help="Seed wrestler identityPersonId")
	parser.add_argument("--depth", type=int, default=3, help="Maximum crawl depth")
	parser.add_argument(
		"--reset",
		action="store_true",
		help="Clear persisted crawler state before starting",
	)
	parser.add_argument(
		"--start-date",
		type=_valid_timestamp,
		help="Only keep matches on or after this timestamp (YYYY-MM-DD or ISO-8601)",
	)
	parser.add_argument(
		"--end-date",
		type=_valid_timestamp,
		help="Only keep matches on or before this timestamp (YYYY-MM-DD or ISO-8601)",
	)
	parser.add_argument(
		"--weight-class",
		dest="weight_classes",
		action="append",
		default=[],
		help="Limit crawl to the provided weight classes (repeat flag for multiple)",
	)
	return parser.parse_args()


def main() -> None:
	args = parse_args()
	if args.start_date and args.end_date and args.start_date > args.end_date:
		raise SystemExit("start-date must be less than or equal to end-date")
	start_arg = _format_cli_timestamp(args.start_date) if args.start_date else None
	end_arg = _format_cli_timestamp(args.end_date) if args.end_date else None
	db.initialize_database()
	download_util.crawl(
		seed_id=args.seed,
		depth=args.depth,
		reset=args.reset,
		allowed_weights=set(args.weight_classes) if args.weight_classes else None,
		start_date=start_arg,
		end_date=end_arg,
	)


if __name__ == "__main__":
	main()
