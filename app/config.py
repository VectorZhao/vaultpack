import os
from pathlib import Path
from datetime import timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


DATA_DIR = Path(os.environ.get("BACKUP_DATA_DIR", "/data"))
SOURCE_ROOT = Path(os.environ.get("BACKUP_SOURCE_ROOT", "/backup-source")).resolve()
WORK_DIR = Path(os.environ.get("BACKUP_WORK_DIR", "/tmp/backup-work"))
SECRET_KEY = os.environ.get("BACKUP_SECRET_KEY", "dev-change-me")
VAULTPACK_ROLE = os.environ.get("VAULTPACK_ROLE", "panel").strip().lower() or "panel"
PANEL_URL = os.environ.get("PANEL_URL", "").rstrip("/")
AGENT_ENROLL_TOKEN = os.environ.get("AGENT_ENROLL_TOKEN", "")
AGENT_TOKEN = os.environ.get("AGENT_TOKEN", "")
AGENT_NAME = os.environ.get("AGENT_NAME", "")
AGENT_POLL_INTERVAL = max(2, int(os.environ.get("AGENT_POLL_INTERVAL", "10")))
APP_VERSION = os.environ.get("VAULTPACK_VERSION", "2.1.4")
ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")
TIMEZONE_NAME = os.environ.get("BACKUP_TIMEZONE") or os.environ.get("TZ") or "Asia/Shanghai"

try:
    APP_TIMEZONE = ZoneInfo(TIMEZONE_NAME)
except ZoneInfoNotFoundError:
    TIMEZONE_NAME = "UTC"
    APP_TIMEZONE = timezone.utc

DB_PATH = DATA_DIR / "backup.db"
