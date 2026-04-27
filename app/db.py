import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone

from .config import DATA_DIR, DB_PATH


SCHEMA = """
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    username TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    totp_secret TEXT,
    totp_enabled INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS webdav_config (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    base_url TEXT NOT NULL,
    username TEXT NOT NULL,
    password TEXT NOT NULL,
    remote_dir TEXT NOT NULL DEFAULT '/backups'
);

CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    source_path TEXT NOT NULL,
    interval_days INTEGER NOT NULL,
    retention_count INTEGER NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    next_run_at TEXT,
    last_run_at TEXT,
    last_status TEXT,
    last_message TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    message TEXT,
    archive_name TEXT,
    FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
);
"""


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def init_db():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with connect() as conn:
        conn.executescript(SCHEMA)
        _migrate_webdav_config(conn)


def _migrate_webdav_config(conn):
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'webdav_config'"
    ).fetchone()
    if not row:
        return
    table_sql = row["sql"] or ""
    if "CHECK (id = 1)" not in table_sql and "CHECK(id = 1)" not in table_sql:
        return

    existing = conn.execute(
        "SELECT base_url, username, password, remote_dir FROM webdav_config ORDER BY id"
    ).fetchall()
    backup_name = "webdav_config_legacy"
    suffix = 1
    while conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (backup_name,)
    ).fetchone():
        suffix += 1
        backup_name = f"webdav_config_legacy_{suffix}"

    conn.execute(f"ALTER TABLE webdav_config RENAME TO {backup_name}")
    conn.execute(
        """
        CREATE TABLE webdav_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            base_url TEXT NOT NULL,
            username TEXT NOT NULL,
            password TEXT NOT NULL,
            remote_dir TEXT NOT NULL DEFAULT '/backups'
        )
        """
    )
    for cfg in existing:
        conn.execute(
            "INSERT INTO webdav_config(base_url, username, password, remote_dir) VALUES(?, ?, ?, ?)",
            (cfg["base_url"], cfg["username"], cfg["password"], cfg["remote_dir"]),
        )


@contextmanager
def connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        yield conn
        conn.commit()
    finally:
        conn.close()


def get_setting(key, default=None):
    with connect() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else default


def set_setting(key, value):
    with connect() as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
