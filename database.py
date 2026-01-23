import Session
import Listing
import sqlite3
import config
from typing import list
# Persistence layer using SQLite3 and Pandas

class Database:
    conn: sqlite3.Connection
    
    def __init__(self):
        self.conn = sqlite3.connect(config.database.file)
        self.provision()
    
    def provision(self) -> None:
        """Read and execute the schema file"""
        with open(config.database.schema, 'r') as f:
            schema_sql = f.read()
        
        cursor = self.conn.cursor()
        cursor.executescript(schema_sql)
        self.conn.commit()

    def save_session(self, session: Session) -> int:

        sql = "INSERT INTO scrape_session (title, datetime_start, datetime_finish, meta) VALUES (?, ?, ?, ?)"
        
        cursor = self.conn.cursor()
        cursor.execute(sql, (session.title, session.start_time, session.finish_time, session.metaToJson()))
        self.conn.commit()
        return cursor.lastrowid

    def get_all_sessions(self) -> list[Session]:
        """Retrieves a list of all scrape sessions."""
        sql = "SELECT id, datetime_start, datetime_finish, meta FROM scrape_session ORDER BY datetime_start DESC"
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return [Session(**row) for row in cursor.fetchall()]

    def get_one_session(self, session_id: int) -> Session:
        """Retrieves a specific scrape session."""
        sql = "SELECT id, datetime_start, datetime_finish, meta FROM scrape_session WHERE id = ?"
        cursor = self.conn.cursor()
        cursor.execute(sql, (session_id))
        return Session(**cursor.fetchone())

    def save_listing(self, listing: Listing) -> int:
        sql = "INSERT INTO scrape_data (scrape_session_id, location, company, job_level, title, date_posted, raw_data) VALUES (?, ?, ?, ?, ?, ?, ?)"
        cursor = self.conn.cursor()
        cursor.execute(sql, (listing.session_id, listing.location, listing.company, listing.job_level, listing.title, listing.date_posted, listing.raw_data))
        self.conn.commit()
        return cursor.lastrowid

    def get_session_listings(self, session: Session) -> list[Listing]:
        """Retrieves dataframe for a specific session."""
        sql = "SELECT * FROM listings WHERE session_id = ?"
        cursor = self.conn.cursor()
        cursor.execute(sql, (session.id))
        return [Listing(**row) for row in cursor.fetchall()]