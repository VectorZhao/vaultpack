import os
from pathlib import Path
from datetime import timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


DATA_DIR = Path(os.environ.get("BACKUP_DATA_DIR", "/data"))
SOURCE_ROOT = Path(os.environ.get("BACKUP_SOURCE_ROOT", "/backup-source")).resolve()
WORK_DIR = Path(os.environ.get("BACKUP_WORK_DIR", "/tmp/backup-work"))
SECRET_KEY = os.environ.get("BACKUP_SECRET_KEY", "dev-change-me")
TIMEZONE_NAME = os.environ.get("BACKUP_TIMEZONE") or os.environ.get("TZ") or "Asia/Shanghai"

try:
    APP_TIMEZONE = ZoneInfo(TIMEZONE_NAME)
except ZoneInfoNotFoundError:
    TIMEZONE_NAME = "UTC"
    APP_TIMEZONE = timezone.utc

DB_PATH = DATA_DIR / "backup.db"
