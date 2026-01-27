from datetime import datetime
from session import Session
from listing import Listing
from config import config
import sqlite3
import json

class Database:
    conn: sqlite3.Connection
    
    def __init__(self):
        self.conn = sqlite3.connect(config.database.file)
        self.conn.row_factory = sqlite3.Row
        self.provision()
    
    def provision(self) -> None:
        with open(config.database.schema, 'r') as f:
            schema_sql = f.read()
        
        cursor = self.conn.cursor()
        cursor.executescript(schema_sql)
        self.conn.commit()

    def get_index(self) -> dict[int, str]:
        sql = "SELECT * FROM sessions ORDER BY id DESC"
        cursor = self.conn.cursor()
        cursor.execute(sql)
        index: dict[int, str] = {row['id']: row['title'] for row in cursor.fetchall()}
        return index

    def save_session(self, session: Session) -> int:
        session_sql = "INSERT INTO sessions (title, description, datetime_start, datetime_finish, meta) VALUES (?, ?, ?, ?, ?)"
        cursor = self.conn.cursor()
        cursor.execute(session_sql, (session.title, session.description,
                                                session.start_time, session.finish_time,
                                                json.dumps(session.meta)))
        session_id: int = cursor.lastrowid
        for listing in session.listings:
            self.save_listing(session_id, listing)
        self.conn.commit()
        return session_id

    def get_session(self, session_id) -> Session:
        sql = "SELECT * FROM sessions WHERE id = ? ORDER BY datetime_start DESC"
        cursor = self.conn.cursor()
        row = cursor.execute(sql,(session_id,)).fetchone()
        session = Session(row['title'])
        session.description = row['description']
        session.id = int(row['id'])
        session.start_time = datetime.fromisoformat(row['datetime_start'])
        session.finish_time = datetime.fromisoformat(row['datetime_finish'])
        session.meta = json.loads(row['meta'])
        session.listings = self.get_listings(session)
        return session

    def get_listings(self, session: Session) -> list[Listing]:
        sql = "SELECT * FROM listings WHERE session_id = ?"
        cursor = self.conn.cursor()
        cursor.execute(sql, (session.id,))
        rows = cursor.fetchall()
        listings: list[Listing] = []
        for row in rows:
            listing = Listing()
            listing.id = int(row['id'])
            listing.session_id = session.id
            listing.location = row['location']
            listing.company = row['company']
            listing.job_level = row['job_level']
            listing.title = row['title']
            listing.date_posted = row['date_posted']
            listing.raw_data = row['raw_data']
            listings.append(listing)
        return listings

    # only called within save_session, no commit required
    def save_listing(self, session_id: int, listing: Listing) -> int:
        sql = ("INSERT INTO listings "
               "(session_id, location, company, job_level, title, date_posted, raw_data) "
               "VALUES (?, ?, ?, ?, ?, ?, ?)")
        cursor = self.conn.cursor()
        cursor.execute(
            sql,
            (session_id,
             listing.location,
             listing.company,
             listing.job_level,
             listing.title,
             listing.date_posted,
             listing.raw_data))
        return cursor.lastrowid