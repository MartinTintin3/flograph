# Sigma.js wrestling demo

This package renders the directed matchup network exported by `export.py`. It is built with [Vite](https://vitejs.dev/) and [`@react-sigma/core`](https://sim51.github.io/react-sigma/), providing an end-to-end reference for visualizing the `graph.json` file that ships with the main `flograph` project.

## Dataset

1. Run `python export.py` from the repo root (add any `--start-date`, `--end-date`, or `--weight-class` filters you need).
2. Copy or symlink the resulting `graph.json` into `sigma.js/packages/demo/public/graph.json`.
3. Start the dev server (see below) and reload the page. The viewer falls back to the small `graph.sample.json` that lives in `public/` when no export is present, so you always have something to look at.

Nodes inherit the layout, color, and size computed in `export.py`, so what you see is exactly what the data pipeline produced.

## Available Scripts

In the project directory, you can run:

### `pnpm start`

Runs the app in the development mode.\
Open [localhost:3000](http://localhost:3000) to view it in the browser.

The page will reload if you make edits.\
You will also see any lint errors in the console.

### `pnpm run build`

Builds the app for production to the `build` folder.\
It correctly bundles React in production mode and optimizes the build for the best performance.
