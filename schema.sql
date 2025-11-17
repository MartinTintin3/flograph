CREATE TABLE IF NOT EXISTS matches (
	id TEXT PRIMARY KEY,
	topWrestler_id TEXT,
	bottomWrestler_id TEXT,
	winner_id TEXT,
	weightClass TEXT,
	event_id TEXT,
	matchDate TEXT,
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