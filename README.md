# Flograph data export

`export.py` builds a `graph.json` file from the local SQLite database (`data.db`) so the visualization in `public/` or `visualizer/` can render matchups.

## Usage

From the repo root:

```bash
# export every match (all weights)
python export.py

# export specific weights across a date range
python export.py -w 138 -w 145 --start-date 2024-11-01 --end-date 2024-12-31

# shorthand for "all weights"
python export.py --weight-class "*"
```

Flags:

- `--weight-class` / `-w`: repeat or pass comma-separated values to include specific weight classes. Omit the flag (or pass `*`) to include every recorded weight class.
- `--start-date`: optional inclusive lower bound (`YYYY-MM-DD`).
- `--end-date`: optional inclusive upper bound (`YYYY-MM-DD`).

When a date range is supplied, only matches whose `matches.date` value falls inside the range are included. Wrestler win/loss summaries, edge weights, and the rendered graph layout all reflect the filtered dataset.

The generated `graph.json` is written to the repository root; serve `public/` or `visualizer/` with any static file server to explore it.

## Monthly Glicko-2 ratings

`rating.py` replays every historical match using the Glicko-2 system. Weight classes are normalized to the first numeric substring (letter-only weights such as `HH` are ignored), rating periods are monthly, and inactive wrestlers automatically gain rating deviation over gaps with no matches.

```bash
# compare two tau values and persist the 0.5 run into the ratings table
python rating.py --tau 0.5 --tau 0.8 --persist-tau 0.5

# limit the replay window
python rating.py --start-date 2023-01-01 --end-date 2024-12-31
```

Each tau run produces `build/glicko2_tau-<value>.json` containing every wrestler-weight rating (rating, deviation, volatility, match count, last active month). Passing `--persist-tau <value>` overwrites the `ratings` table with the corresponding snapshot so downstream tooling can read the latest run.

### Conservative leaderboards

After populating the `ratings` table, run:

```bash
python leaderboard.py --limit 25
```

This writes `build/leaderboards.json`, grouping every weight class and ranking wrestlers by the conservative estimate `rating - 2 * RD`. Use `--limit 0` to keep the full list or `--output <path>` to send the leaderboard somewhere else. Pass `--min-last-updated YYYY-MM-DD` to drop wrestlers who have not competed at that weight since the given date (e.g., focus on current-season activity).
Each entry's `last_updated` reflects the first day of the most recent month that wrestler logged a match at that weight.

### Parameter evaluation

`rating_eval.py` replays the historical data up to a cutoff date, then scores later matches to see how well each tau value predicts the outcomes. Metrics include log loss, Brier score, and simple accuracy.

```bash
python rating_eval.py \
	--train-end 2023-12-31 \
	--eval-start 2024-01-01 \
	--eval-end 2024-12-31 \
	--tau 0.3 --tau 0.5 --tau 0.8 \
	--output build/rating_eval_2024.json
```

The script prints a summary table and, if `--output` is provided, saves the full evaluation payload. When `--eval-start` is omitted it defaults to the instant after `--train-end`, so you can simply pass a split date to compare multiple tau values automatically.
