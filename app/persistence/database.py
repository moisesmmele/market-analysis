from datetime import datetime
from typing import Any
import sqlite3
import json

from app.entities import Session, Listing
from enums import SubsessionStatus
from entities import Subsession
from app import config

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
        with open(config.database.schema, 'r') as f:
            schema_sql = f.read()
        self.conn.executescript(schema_sql).close()
        self.conn.commit()

    def get_index(self) -> dict[int, dict]:
        query = f"""
                SELECT  
                        sessions.id                 AS session_id,
                        sessions.id                 AS session_id,
                        sessions.title              AS session_title,
                        sessions.description        AS session_description,
                        sessions.provider           AS session_provider,
                        sessions.platform           AS session_platform,
                        sessions.date               AS date,
                        subsessions.id              AS subsession_id,
                        subsessions.search_term     AS search_term,
                        (SELECT COUNT(*) 
                            FROM listings 
                            WHERE listings.subsession_id = subsessions.id)                
                                                    AS listings_count
                
                FROM sessions
                    LEFT JOIN subsessions 
                    ON subsessions.session_id = sessions.id
                
                ORDER BY sessions.id DESC
                """

        rows = (self.conn.cursor()).execute(query).fetchall()

        results = {}
        for row in rows:
            session_id = row['session_id']

            if session_id not in results:
                results[session_id] = {
                    "title": row['session_title'],
                    "description": row['session_description'],
                    "provider": row['session_provider'],
                    "platform": row['session_platform'],
                    "date": datetime.fromisoformat(row['date']).date().isoformat(),
                    "subsessions": [],
                    "subsession_count": 0,
                    "listings_count": 0
                }

            if row['subsession_id'] is not None:
                results[session_id]["subsessions"].append({
                    "search_term": row['search_term'],
                    "listings": row['listings_count']
                })

        for session_id in results:
            results[session_id]["subsession_count"] = len(results[session_id]["subsessions"])
            for subsession in results[session_id]["subsessions"]:
                results[session_id]["listings_count"] += subsession["listings"]

        return results

    def get_pending(self, session_id: int) -> dict[int, Subsession]:
        status = SubsessionStatus.pending
        query = "SELECT * FROM subsessions WHERE session_id = ? AND status = ?"
        params = (session_id, status)
        cursor = self.conn.cursor()
        rows = cursor.execute(query, params).fetchall()
        return {row['id']: Subsession.from_row(dict(**row)) for row in rows} if rows else {}

    def get_last_session_id(self) -> int|None:
        sql = "SELECT MAX(id) as last FROM sessions"
        cursor = self.conn.cursor()
        row = cursor.execute(sql).fetchone()
        cursor.close()
        return row['last'] if row['last'] else None

    def get_session(self, session_id) -> Session | None:
        sql = "SELECT * FROM sessions WHERE id = ? ORDER BY date DESC"
        params = (session_id,)

        cursor = self.conn.cursor()
        row = cursor.execute(sql, params).fetchone()
        cursor.close()

        if row is None:
            return None

        return Session.from_row(dict(row), self.get_subsessions(session_id))


    def save_session(self, session: Session) -> Session:
        sql = """
              INSERT INTO sessions 
                  (title, description, provider, platform, date, meta) 
              VALUES (?, ?, ?, ?, ?, ?)
              """
        params = (session.title, session.description, session.provider,
                  session.platform, session.date,
                  json.dumps(session.meta))

        cursor = self.conn.cursor()
        session_id: int = cursor.execute(sql, params).lastrowid
        cursor.close()

        for index in session.subsessions:
            subs_id = self.save_subsession(session_id, session.subsessions[index])
            session.subsessions[index].id = subs_id

        # Commit transaction
        self.conn.commit()
        session.id = session_id
        return session

    def get_subsessions(self, session_id: int) -> dict[int, Subsession]:
        query = "SELECT * FROM subsessions WHERE session_id = ?"
        params = (session_id,)
        cursor = self.conn.cursor()
        rows = cursor.execute(query, params).fetchall()
        cursor.close()
        subsessions = {}
        for row in rows:
            subsession = Subsession.from_row(row)
            subsession.listings = self.get_listings(subsession.id)
            subsessions[row['id']] = subsession

        return subsessions

    def save_subsession(self, session_id: int, subsession: Subsession) -> int:
        """Saves a subsession"""
        sql = "INSERT INTO subsessions (session_id, search_term, start_date, status) VALUES (?, ?, ?, ?)"
        params = (session_id, subsession.search_term, subsession.start_date, subsession.status)
        cursor = self.conn.cursor()
        subsession_id = cursor.execute(sql, params).lastrowid
        cursor.close()
        return subsession_id

    def update_subsession(self, subsession: Subsession) -> int:
        self.save_listings(subsession.listings)
        stmt = "UPDATE subsessions SET status = ?, finish_date = ? WHERE id = ?"
        params = (subsession.status, subsession.finish_date, subsession.id)
        cursor = self.conn.cursor()
        cursor.execute(stmt, params)
        self.conn.commit()
        cursor.close()
        return subsession.id

    def get_listings(self, subsession_id) -> dict[int, Listing]:
        sql = "SELECT * FROM listings WHERE subsession_id = ?"
        cursor = self.conn.cursor()
        cursor.execute(sql, (subsession_id,))
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

    def save_listings(self, listings: dict[int, Listing]) -> int:
        """Saves a listing to the database. Commit is done by save_session"""
        sql = "INSERT INTO LISTINGS (subsession_id, raw_data) VALUES (?, ?)"
        data: list[tuple[int, str]] = [
            (listing.subsession_id, listing.raw_data) for listing in listings.values()]
        cursor = self.conn.cursor()
        listing_id = cursor.executemany(sql, data).lastrowid
        cursor.close()
        return listing_id

    def get_duplicates(self, session_id: int):
        sql = "SELECT * FROM known_duplicates WHERE session_id = ?"
        cursor = self.conn.cursor()
        rows = cursor.execute(sql, (session_id,)).fetchall()
        return [(row['id_a'], row['id_b']) for row in rows] if rows else []

    def save_duplicates(self, session_id: int, duplicates: list[tuple[int, int]]) -> None:
        sql = "INSERT INTO known_duplicates (session_id, id_a, id_b) VALUES (?, ?, ?)"
        values = [(session_id, duplicate[0], duplicate[1]) for duplicate in duplicates]
        print(values)
        cursor = self.conn.cursor()
        cursor.executemany(sql, values)
        self.conn.commit()
        cursor.close()


    def _query(self, query: str) -> dict[Any, Any]:
        cursor = self.conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return {index: {**row} for index, row in enumerate(rows)}