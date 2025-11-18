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
