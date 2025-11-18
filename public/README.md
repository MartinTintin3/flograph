# Wrestling Graph + Reagraph

This project turns `graph.json` into an interactive WebGL network graph powered by [reagraph](https://github.com/reaviz/reagraph) and plain React loaded from CDNs. No build tooling is required—open the page through any static web server and the client-side code handles the rest.

## Quick start

Serve the `public` directory so the browser can fetch `graph.json`:

```bash
cd /home/martin/flograph/public
python3 -m http.server 4173
```

Then open <http://localhost:4173> in a modern browser.

## Data format

`graph.json` must contain two arrays:

```json
{
  "nodes": [
    { "id": "string", "name": "string", "wins": 0, "losses": 0, "alias": "optional", "winPct": 0.75 }
  ],
  "links": [
    { "source": "node-id", "target": "node-id", "count": 3 }
  ]
}
```

- `winPct` is optional; it is derived automatically from wins and losses when omitted.
- Additional properties are preserved inside the `data` payload each node/edge exposes through the info panel.

If `graph.json` is missing or invalid, the app automatically falls back to the sample dataset defined inline in `index.html`, and shows a banner explaining the issue.

## Customizing the view

- Hover or tap nodes to inspect the record and alias.
- Drag nodes to pin them in place; drag the background to pan.
- Use the mouse wheel or trackpad scroll to zoom.
- Click **Reset view** to fit all nodes back into frame using the Reagraph camera controls.

The React code lives entirely in `index.html`, making it easy to further tweak layouts, colors, or add new UI without touching a build pipeline.
