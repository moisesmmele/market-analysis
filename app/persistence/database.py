from typing import Any
import sqlite3
import json

from app.entities import Session, Listing
from app import config

# Can be refactored into a Repository and Driver if needed
# Atm it's fine to handle both in a single class
# Since we are using st.cache, which acts as a singleton/resource locator,
# And the database is sqlite, there is no need to close conns

class Database:
    conn: sqlite3.Connection

    def __init__(self):
        # check_same_thread allows to cache the conn and use it across
        # streamlit runs (aka page hits)
        self.conn = sqlite3.connect(config.database.file, check_same_thread=False)
        # fetch statements returns Row object (can read via index or kv/dict)
        self.conn.row_factory = sqlite3.Row
        self.provision()

    def provision(self) -> None:
        """execute the schema.sql file to ensure proper config and table creation"""

        with open(config.database.schema, 'r') as f:
            schema_sql = f.read()

        self.conn.executescript(schema_sql).close()
        self.conn.commit()

    def get_index(self) -> dict[int, str]:
        """Returns an index dict, since get_session is recursive (fetches all listings)"""

        # language=sql
        subquery = "(SELECT COUNT(*) FROM listings WHERE listings.session_id = sessions.id) as listings_count"
        query = f"SELECT id, title, {subquery} FROM sessions ORDER BY id DESC"

        rows = (self.conn.cursor()).execute(query).fetchall()
        return {row['id']: {"title": row['title'], "listings": row["listings_count"]} for row in rows} if rows else {}

    def save_session(self, session: Session) -> int:
        """Saves a new session and its listings, only commit after every listing is saved"""

        sql = "INSERT INTO sessions (title, description, datetime_start, datetime_finish, meta) VALUES (?, ?, ?, ?, ?)"
        params = (session.title, session.description, session.start_time, session.finish_time, json.dumps(session.meta))

        cursor = self.conn.cursor()
        session_id: int = cursor.execute(sql, params).lastrowid
        cursor.close()

        self.save_listings(session.raw_listings)

        # Commit transaction
        self.conn.commit()

        return session_id

    def get_session(self, session_id) -> Session | None:
        """Retrieves a session with its listings from the database"""

        sql = "SELECT * FROM sessions WHERE id = ? ORDER BY datetime_start DESC"
        params = (session_id,)

        cursor = self.conn.cursor()
        row = cursor.execute(sql, params).fetchone()
        cursor.close()

        if row is None:
            return None

        session = Session.from_row(dict(row))
        session.listings = {i: l for i, l in self.get_listings(session_id).items()}

        return session

    def get_listings(self, session_id) -> dict[int, Listing]:
        """retrieves listings for a given session"""

        sql = "SELECT * FROM listings WHERE session_id = ?"
        cursor = self.conn.cursor()
        cursor.execute(sql, (session_id,))
        rows = cursor.fetchall()
        cursor.close()
        return {int(row["id"]): Listing.from_row(row) for row in rows} if rows else {}

    def get_one_listing(self, listing_id) -> Listing | None:
        """retrieves a listing"""
        sql = "SELECT * FROM listings WHERE id = ?"
        cursor = self.conn.cursor()
        cursor.execute(sql, (listing_id,))
        row = cursor.fetchone()
        if not row:
            return None
        cursor.close()
        return Listing.from_row(row)

    # only called within save_session, no commit required
    def save_listings(self, listings: list[dict[str, str]]) -> int:
        """Saves a listing to the database. Commit is done by save_session"""
        sql = "INSERT INTO LISTINGS (session_id, raw_data) VALUES (?, ?)"
        data: list[tuple[str, str]] = [(d.get('session_id'), d.get('raw_data')) for d in listings]
        cursor = self.conn.cursor()
        listing_id = cursor.executemany(sql, data).lastrowid
        cursor.close()

        return listing_id

    def _query(self, query: str) -> dict[Any, Any]:
        cursor = self.conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return {index: {**row} for index, row in enumerate(rows)}