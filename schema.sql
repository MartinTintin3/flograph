CREATE TABLE IF NOT EXISTS matches (
	id TEXT PRIMARY KEY,
	topWrestler_id TEXT,
	bottomWrestler_id TEXT,
	winner_id TEXT,
	weightClass TEXT,
	event_id TEXT,
	date TEXT,
	result TEXT,
	winType TEXT,
	FOREIGN KEY(topWrestler_id) REFERENCES wrestlers(id),
	FOREIGN KEY(bottomWrestler_id) REFERENCES wrestlers(id),
	FOREIGN KEY(winner_id) REFERENCES wrestlers(id),
	FOREIGN KEY(event_id) REFERENCES events(id)
);

CREATE TABLE IF NOT EXISTS wrestlers (
	id TEXT PRIMARY KEY,
	name TEXT,
	team_id TEXT,
	FOREIGN KEY(team_id) REFERENCES teams(id)
);

CREATE TABLE IF NOT EXISTS events (
	id TEXT PRIMARY KEY,
	name TEXT,
	date TEXT,
	location TEXT
);

CREATE TABLE IF NOT EXISTS teams (
	id TEXT PRIMARY KEY,
	name TEXT
);

CREATE TABLE IF NOT EXISTS fetched (
	id TEXT PRIMARY KEY,
	date TEXT
);

CREATE TABLE IF NOT EXISTS crawl_queue (
	wrestler_id TEXT PRIMARY KEY,
	depth INTEGER NOT NULL,
	enqueued_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS crawl_seen (
	wrestler_id TEXT PRIMARY KEY,
	depth INTEGER NOT NULL,
	discovered_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
	processed_at TEXT
);

CREATE TABLE IF NOT EXISTS crawler_state (
	id INTEGER PRIMARY KEY CHECK (id = 1),
	seed_id TEXT NOT NULL,
	depth_limit INTEGER NOT NULL,
	created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TEXT
);