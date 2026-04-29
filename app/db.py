import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone

from .config import DATA_DIR, DB_PATH, SOURCE_ROOT
from .schedule import next_run_from_cron


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

CREATE TABLE IF NOT EXISTS nodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    hostname TEXT,
    source_root TEXT NOT NULL DEFAULT '/backup-source',
    token_hash TEXT,
    status TEXT NOT NULL DEFAULT 'offline',
    enabled INTEGER NOT NULL DEFAULT 1,
    mode TEXT NOT NULL DEFAULT 'agent',
    version TEXT,
    last_seen_at TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    node_id INTEGER,
    destination_id INTEGER,
    source_path TEXT NOT NULL,
    interval_days INTEGER NOT NULL DEFAULT 1,
    cron_expr TEXT NOT NULL DEFAULT '0 2 * * *',
    retention_count INTEGER NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    next_run_at TEXT,
    last_run_at TEXT,
    last_status TEXT,
    last_message TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(node_id) REFERENCES nodes(id),
    FOREIGN KEY(destination_id) REFERENCES webdav_config(id)
);

CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    node_id INTEGER,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    message TEXT,
    progress_current INTEGER NOT NULL DEFAULT 0,
    progress_total INTEGER NOT NULL DEFAULT 0,
    progress_label TEXT,
    archive_name TEXT,
    FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE,
    FOREIGN KEY(node_id) REFERENCES nodes(id)
);

CREATE TABLE IF NOT EXISTS agent_commands (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id INTEGER NOT NULL,
    run_id INTEGER,
    type TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    claimed_at TEXT,
    finished_at TEXT,
    error TEXT,
    result TEXT,
    FOREIGN KEY(node_id) REFERENCES nodes(id),
    FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE
);
"""


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def init_db():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with connect() as conn:
        conn.executescript(SCHEMA)
        _migrate_webdav_config(conn)
        _migrate_jobs_destination(conn)
        _migrate_nodes(conn)
        _migrate_jobs_cron(conn)
        _migrate_run_progress(conn)
        _migrate_runs_node(conn)
        _migrate_agent_commands(conn)
        _refresh_job_next_runs(conn)


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


def _migrate_jobs_destination(conn):
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()}
    if "destination_id" in columns:
        return
    conn.execute("ALTER TABLE jobs ADD COLUMN destination_id INTEGER")
    default_destination = conn.execute("SELECT id FROM webdav_config ORDER BY id LIMIT 1").fetchone()
    if default_destination:
        conn.execute("UPDATE jobs SET destination_id = ? WHERE destination_id IS NULL", (default_destination["id"],))


def _migrate_nodes(conn):
    node = conn.execute("SELECT id FROM nodes WHERE mode = 'local' ORDER BY id LIMIT 1").fetchone()
    if not node:
        conn.execute(
            "INSERT INTO nodes(name, hostname, source_root, status, enabled, mode, version, last_seen_at, created_at) "
            "VALUES('local', 'local', ?, 'online', 1, 'local', NULL, ?, ?)",
            (SOURCE_ROOT.as_posix(), utc_now_iso(), utc_now_iso()),
        )
    local_id = conn.execute("SELECT id FROM nodes WHERE mode = 'local' ORDER BY id LIMIT 1").fetchone()["id"]
    conn.execute("UPDATE nodes SET source_root = ?, status = 'online' WHERE id = ?", (SOURCE_ROOT.as_posix(), local_id))
    job_columns = {row["name"] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()}
    if "node_id" not in job_columns:
        conn.execute("ALTER TABLE jobs ADD COLUMN node_id INTEGER")
    conn.execute("UPDATE jobs SET node_id = ? WHERE node_id IS NULL", (local_id,))


def _migrate_jobs_cron(conn):
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()}
    if "cron_expr" in columns:
        return
    conn.execute("ALTER TABLE jobs ADD COLUMN cron_expr TEXT NOT NULL DEFAULT '0 2 * * *'")
    for job in conn.execute("SELECT id, interval_days FROM jobs").fetchall():
        interval_days = max(1, int(job["interval_days"] or 1))
        cron_expr = "0 2 * * *" if interval_days == 1 else f"0 2 */{interval_days} * *"
        conn.execute("UPDATE jobs SET cron_expr = ? WHERE id = ?", (cron_expr, job["id"]))


def _refresh_job_next_runs(conn):
    for job in conn.execute("SELECT id, cron_expr FROM jobs WHERE enabled = 1").fetchall():
        try:
            next_run = next_run_from_cron(job["cron_expr"])
        except Exception:
            continue
        conn.execute(
            "UPDATE jobs SET next_run_at = ? WHERE id = ?",
            (next_run, job["id"]),
        )


def _migrate_run_progress(conn):
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(runs)").fetchall()}
    if "progress_current" not in columns:
        conn.execute("ALTER TABLE runs ADD COLUMN progress_current INTEGER NOT NULL DEFAULT 0")
    if "progress_total" not in columns:
        conn.execute("ALTER TABLE runs ADD COLUMN progress_total INTEGER NOT NULL DEFAULT 0")
    if "progress_label" not in columns:
        conn.execute("ALTER TABLE runs ADD COLUMN progress_label TEXT")
    if "archive_name" not in columns:
        conn.execute("ALTER TABLE runs ADD COLUMN archive_name TEXT")


def _migrate_runs_node(conn):
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(runs)").fetchall()}
    if "node_id" not in columns:
        conn.execute("ALTER TABLE runs ADD COLUMN node_id INTEGER")
    conn.execute(
        "UPDATE runs SET node_id = (SELECT node_id FROM jobs WHERE jobs.id = runs.job_id) WHERE node_id IS NULL"
    )


def _migrate_agent_commands(conn):
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(agent_commands)").fetchall()}
    if "result" not in columns:
        conn.execute("ALTER TABLE agent_commands ADD COLUMN result TEXT")


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
