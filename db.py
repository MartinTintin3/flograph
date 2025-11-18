import sqlite3
from typing import Optional, List, Tuple, Dict
from datetime import datetime

DATABASE_PATH = "data.db"

def get_connection():
    """Get a connection to the database."""
    return sqlite3.connect(DATABASE_PATH)


# ==================== WRESTLERS CRUD ====================

def mark_fetch(id: str) -> None:
    """Mark a fetch record with the current date. Inserts a new record or updates if it already exists."""
    conn = get_connection()
    cursor = conn.cursor()

    current_date = datetime.now().strftime("%Y-%m-%d")

    # Use UPSERT pattern: try to insert, or update if it already exists
    cursor.execute(
        """INSERT INTO fetched (id, date) VALUES (?, ?)
           ON CONFLICT(id) DO UPDATE SET date = excluded.date""",
        (id, current_date)
    )

    conn.commit()
    conn.close()


def get_last_fetch_date(id: str) -> Optional[str]:
    """Get the last fetch date for a given id, or return None if not found."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT date FROM fetched WHERE id = ?", (id,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None


def create_wrestler(id: int, name: str, team_id: str) -> None:
    """Create a new wrestler with the given ID. Does nothing if ID already exists."""
    conn = get_connection()
    cursor = conn.cursor()

    # Check if wrestler with this ID already exists
    cursor.execute("SELECT id FROM wrestlers WHERE id = ?", (id,))
    if cursor.fetchone() is not None:
        conn.close()
        return

    cursor.execute(
        "INSERT INTO wrestlers (id, name, team_id) VALUES (?, ?, ?)",
        (id, name, team_id)
    )
    conn.commit()
    conn.close()


def get_wrestler(id: int) -> Optional[Tuple]:
    """Get a wrestler by ID."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM wrestlers WHERE id = ?", (id,))
    result = cursor.fetchone()
    conn.close()
    return result


def get_all_wrestlers() -> List[Tuple]:
    """Get all wrestlers."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM wrestlers")
    results = cursor.fetchall()
    conn.close()
    return results


def update_wrestler(id: int, name: Optional[str] = None, team_id: Optional[str] = None) -> None:
    """Update a wrestler by ID. Only updates provided fields."""
    conn = get_connection()
    cursor = conn.cursor()

    updates = []
    params = []

    if name is not None:
        updates.append("name = ?")
        params.append(name)
    if team_id is not None:
        updates.append("team_id = ?")
        params.append(team_id)

    if updates:
        params.append(id)
        query = f"UPDATE wrestlers SET {', '.join(updates)} WHERE id = ?"
        cursor.execute(query, params)
        conn.commit()

    conn.close()


def delete_wrestler(id: int) -> None:
    """Delete a wrestler by ID."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM wrestlers WHERE id = ?", (id,))
    conn.commit()
    conn.close()


# ==================== EVENTS CRUD ====================

def create_event(id: int, name: str, date: str, location: str) -> None:
    """Create a new event with the given ID. Does nothing if ID already exists."""
    conn = get_connection()
    cursor = conn.cursor()

    # Check if event with this ID already exists
    cursor.execute("SELECT id FROM events WHERE id = ?", (id,))
    if cursor.fetchone() is not None:
        conn.close()
        return

    cursor.execute(
        "INSERT INTO events (id, name, date, location) VALUES (?, ?, ?, ?)",
        (id, name, date, location)
    )
    conn.commit()
    conn.close()


def get_event(id: int) -> Optional[Tuple]:
    """Get an event by ID."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM events WHERE id = ?", (id,))
    result = cursor.fetchone()
    conn.close()
    return result


def get_all_events() -> List[Tuple]:
    """Get all events."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM events")
    results = cursor.fetchall()
    conn.close()
    return results


def update_event(id: int, name: Optional[str] = None, date: Optional[str] = None,
                location: Optional[str] = None) -> None:
    """Update an event by ID. Only updates provided fields."""
    conn = get_connection()
    cursor = conn.cursor()

    updates = []
    params = []

    if name is not None:
        updates.append("name = ?")
        params.append(name)
    if date is not None:
        updates.append("date = ?")
        params.append(date)
    if location is not None:
        updates.append("location = ?")
        params.append(location)

    if updates:
        params.append(id)
        query = f"UPDATE events SET {', '.join(updates)} WHERE id = ?"
        cursor.execute(query, params)
        conn.commit()

    conn.close()


def delete_event(id: int) -> None:
    """Delete an event by ID."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM events WHERE id = ?", (id,))
    conn.commit()
    conn.close()


# ==================== TEAMS CRUD ====================

def create_team(id: int, name: str) -> None:
    """Create a new team with the given ID. Does nothing if ID already exists."""
    conn = get_connection()
    cursor = conn.cursor()

    # Check if team with this ID already exists
    cursor.execute("SELECT id FROM teams WHERE id = ?", (id,))
    if cursor.fetchone() is not None:
        conn.close()
        return
    
    if (id is None) or (name is None):
        conn.close()
        return

    cursor.execute(
        "INSERT INTO teams (id, name) VALUES (?, ?)",
        (id, name)
    )
    conn.commit()
    conn.close()


def get_team(id: int) -> Optional[Tuple]:
    """Get a team by ID."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM teams WHERE id = ?", (id,))
    result = cursor.fetchone()
    conn.close()
    return result


def get_all_teams() -> List[Tuple]:
    """Get all teams."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM teams")
    results = cursor.fetchall()
    conn.close()
    return results


def update_team(id: int, name: Optional[str] = None) -> None:
    """Update a team by ID. Only updates provided fields."""
    conn = get_connection()
    cursor = conn.cursor()

    updates = []
    params = []

    if name is not None:
        updates.append("name = ?")
        params.append(name)

    if updates:
        params.append(id)
        query = f"UPDATE teams SET {', '.join(updates)} WHERE id = ?"
        cursor.execute(query, params)
        conn.commit()

    conn.close()


def delete_team(id: int) -> None:
    """Delete a team by ID."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM teams WHERE id = ?", (id,))
    conn.commit()
    conn.close()


def get_wrestlers_by_team(team_id: int) -> List[Tuple]:
    """Get all wrestlers for a specific team."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM wrestlers WHERE team_id = ?", (team_id,))
    results = cursor.fetchall()
    conn.close()
    return results

# ==================== MATCHES CRUD ====================

def create_match(id: int, topWrestler_id: int, bottomWrestler_id: int,
                winner_id: int, weightClass: str, event_id: int, date: str, result: str, winType: str) -> None:
    """Create a new match with the given ID. Does nothing if ID already exists."""
    conn = get_connection()
    cursor = conn.cursor()

    if topWrestler_id is None or bottomWrestler_id is None:
        conn.close()
        return

    # Check if match with this ID already exists
    cursor.execute("SELECT id FROM matches WHERE id = ?", (id,))
    if cursor.fetchone() is not None:
        conn.close()
        return

    cursor.execute(
        """INSERT INTO matches (id, topWrestler_id, bottomWrestler_id, winner_id,
           weightClass, event_id, date, result, winType)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (id, topWrestler_id, bottomWrestler_id, winner_id, weightClass,
         event_id, date, result, winType)
    )
    conn.commit()
    conn.close()


def get_match(id: int) -> Optional[Tuple]:
    """Get a match by ID."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM matches WHERE id = ?", (id,))
    result = cursor.fetchone()
    conn.close()
    return result


def get_all_matches() -> List[Tuple]:
    """Get all matches."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM matches")
    results = cursor.fetchall()
    conn.close()
    return results


def get_matches_by_wrestler(wrestler_id: int) -> List[Tuple]:
    """Get all matches involving a specific wrestler."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """SELECT * FROM matches
           WHERE topWrestler_id = ? OR bottomWrestler_id = ?""",
        (wrestler_id, wrestler_id)
    )
    results = cursor.fetchall()
    conn.close()
    return results


def get_matches_by_event(event_id: int) -> List[Tuple]:
    """Get all matches for a specific event."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM matches WHERE event_id = ?", (event_id,))
    results = cursor.fetchall()
    conn.close()
    return results


def delete_match(id: int) -> None:
    """Delete a match by ID."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM matches WHERE id = ?", (id,))
    conn.commit()
    conn.close()


# ==================== UTILITY FUNCTIONS ====================

def initialize_database():
    """Initialize the database using schema.sql."""
    conn = get_connection()
    cursor = conn.cursor()

    with open('schema.sql', 'r') as f:
        schema = f.read()
        cursor.executescript(schema)

    conn.commit()
    conn.close()


# ==================== CRAWLER STATE ====================

def upsert_crawler_state(seed_id: str, depth_limit: int) -> None:
    """Persist the crawler configuration (seed + depth)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO crawler_state (id, seed_id, depth_limit)
               VALUES (1, ?, ?)
               ON CONFLICT(id) DO UPDATE
               SET seed_id = excluded.seed_id,
                   depth_limit = excluded.depth_limit,
                   updated_at = CURRENT_TIMESTAMP""",
        (seed_id, depth_limit),
    )
    conn.commit()
    conn.close()


def get_crawler_state() -> Optional[Dict[str, int]]:
    """Return the stored crawler configuration, if any."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT seed_id, depth_limit FROM crawler_state WHERE id = 1")
    row = cursor.fetchone()
    conn.close()
    if row is None:
        return None
    return {"seed_id": row[0], "depth_limit": row[1]}


def clear_crawler_state() -> None:
    """Remove crawler metadata and frontier tables."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM crawler_state")
    cursor.execute("DELETE FROM crawl_queue")
    cursor.execute("DELETE FROM crawl_seen")
    conn.commit()
    conn.close()


# ==================== CRAWL FRONTIER HELPERS ====================

def enqueue_wrestler(wrestler_id: str, depth: int) -> None:
    """Add or update a wrestler in the crawl queue."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO crawl_queue (wrestler_id, depth)
               VALUES (?, ?)
               ON CONFLICT(wrestler_id) DO UPDATE
               SET depth = excluded.depth,
                   enqueued_at = CURRENT_TIMESTAMP""",
        (wrestler_id, depth),
    )
    conn.commit()
    conn.close()


def remove_from_queue(wrestler_id: str) -> None:
    """Remove a wrestler from the crawl queue."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM crawl_queue WHERE wrestler_id = ?", (wrestler_id,))
    conn.commit()
    conn.close()


def clear_queue() -> None:
    """Remove all entries from the crawl queue."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM crawl_queue")
    conn.commit()
    conn.close()


def get_queue_items() -> List[Tuple[str, int]]:
    """Return all queue entries ordered by enqueue time."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM crawl_queue WHERE wrestler_id IS NULL OR wrestler_id = ''")
    conn.commit()
    cursor.execute(
        "SELECT wrestler_id, depth FROM crawl_queue ORDER BY enqueued_at ASC"
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


def get_queue_count() -> int:
    """Return the number of outstanding queue entries."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM crawl_queue")
    count = cursor.fetchone()[0]
    conn.close()
    return count


def record_seen_wrestler(wrestler_id: str, depth: int) -> None:
    """Track that a wrestler has been discovered at a specific depth."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO crawl_seen (wrestler_id, depth)
               VALUES (?, ?)
               ON CONFLICT(wrestler_id) DO UPDATE
               SET depth = MIN(depth, excluded.depth)""",
        (wrestler_id, depth),
    )
    conn.commit()
    conn.close()


def get_seen_wrestlers() -> List[Tuple[str, int]]:
    """Return all seen wrestlers with their depth."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT wrestler_id, depth FROM crawl_seen")
    rows = cursor.fetchall()
    conn.close()
    return rows


def mark_wrestler_processed(wrestler_id: str) -> None:
    """Set processed_at for a wrestler once crawling is complete."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE crawl_seen SET processed_at = CURRENT_TIMESTAMP WHERE wrestler_id = ?",
        (wrestler_id,),
    )
    conn.commit()
    conn.close()


def get_processed_wrestlers() -> List[str]:
    """Return wrestler IDs that have already been processed."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT wrestler_id FROM crawl_seen WHERE processed_at IS NOT NULL")
    rows = cursor.fetchall()
    conn.close()
    return [row[0] for row in rows]


def get_unprocessed_wrestlers(max_depth: Optional[int] = None) -> List[Tuple[str, int]]:
    """Return wrestlers that have been discovered but not processed yet."""
    conn = get_connection()
    cursor = conn.cursor()
    query = "SELECT wrestler_id, depth FROM crawl_seen WHERE processed_at IS NULL"
    params: List[int] = []
    if max_depth is not None:
        query += " AND depth <= ?"
        params.append(max_depth)
    query += " ORDER BY depth ASC"
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    return rows


def get_all_fetched_ids() -> List[str]:
    """Return IDs that have already been fully processed."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM fetched")
    rows = cursor.fetchall()
    conn.close()
    return [row[0] for row in rows]


def clear_frontier_tables() -> None:
    """Helper to drop queue/seen entries without touching metadata."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM crawl_queue")
    cursor.execute("DELETE FROM crawl_seen")
    conn.commit()
    conn.close()
