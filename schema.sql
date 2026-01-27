-- SQLite3 Schema

PRAGMA foreign_keys = ON; -- Enable foreign key constraints for sqlite3

CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT, -- human-friendly session title
    description TEXT,
    datetime_start TEXT, -- managed by scraper; ISO 8601 format
    datetime_finish TEXT, -- managed by scraper; ISO 8601 format
    meta TEXT -- JSON Metadata (search params, etc)
);

CREATE TABLE IF NOT EXISTS listings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER,
    location TEXT,
    company TEXT, 
    job_level TEXT,
    title TEXT,
    date_posted TEXT,
    raw_data TEXT, -- Raw JSON data
    FOREIGN KEY (session_id) REFERENCES sessions (id) ON DELETE CASCADE
);
