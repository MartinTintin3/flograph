import argparse

import download_util
import db

DEFAULT_SEED = "064ad7f4-8d16-4dd2-94b1-1dd1c45c3832"


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
		"--weight-class",
		dest="weight_classes",
		action="append",
		default=[],
		help="Limit crawl to the provided weight classes (repeat flag for multiple)",
	)
	return parser.parse_args()


def main() -> None:
	args = parse_args()
	db.initialize_database()
	download_util.crawl(
		seed_id=args.seed,
		depth=args.depth,
		reset=args.reset,
		allowed_weights=set(args.weight_classes) if args.weight_classes else None,
	)


if __name__ == "__main__":
	main()
