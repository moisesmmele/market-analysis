-- SQLite3 Schema

PRAGMA foreign_keys = ON; -- Enable foreign key constraints for sqlite3

CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT, -- human-friendly session title
    description TEXT,
    provider TEXT,
    platform TEXT,
    date TEXT, -- managed by scraper; ISO 8601 format
    meta TEXT -- JSON Metadata (search params, etc)
);

CREATE TABLE IF NOT EXISTS subsessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER,
    search_term TEXT,
    start_date TEXT,
    finish_date TEXT,
    status TEXT,
    FOREIGN KEY (session_id) REFERENCES sessions (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS listings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subsession_id INTEGER,
    raw_data TEXT, -- Raw JSON data
    FOREIGN KEY (subsession_id) REFERENCES subsessions (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS known_duplicates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER,
    id_a INTEGER,
    id_b INTEGER,
    FOREIGN KEY (session_id) REFERENCES sessions (id) ON DELETE CASCADE
)