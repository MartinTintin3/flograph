import sqlite3
from typing import Optional, List, Tuple

DATABASE_PATH = "data.db"

def get_connection():
    """Get a connection to the database."""
    return sqlite3.connect(DATABASE_PATH)


# ==================== WRESTLERS CRUD ====================

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
                winner_id: int, weightClass: str, event_id: int, matchDate: str, result: str, winType: str) -> None:
    """Create a new match with the given ID. Does nothing if ID already exists."""
    conn = get_connection()
    cursor = conn.cursor()

    # Check if match with this ID already exists
    cursor.execute("SELECT id FROM matches WHERE id = ?", (id,))
    if cursor.fetchone() is not None:
        conn.close()
        return

    cursor.execute(
        """INSERT INTO matches (id, topWrestler_id, bottomWrestler_id, winner_id,
           weightClass, event_id, matchDate, result, winType)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (id, topWrestler_id, bottomWrestler_id, winner_id, weightClass,
         event_id, matchDate, result, winType)
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
