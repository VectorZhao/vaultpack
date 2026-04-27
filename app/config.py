import os
from pathlib import Path


DATA_DIR = Path(os.environ.get("BACKUP_DATA_DIR", "/data"))
SOURCE_ROOT = Path(os.environ.get("BACKUP_SOURCE_ROOT", "/backup-source")).resolve()
WORK_DIR = Path(os.environ.get("BACKUP_WORK_DIR", "/tmp/backup-work"))
SECRET_KEY = os.environ.get("BACKUP_SECRET_KEY", "dev-change-me")

DB_PATH = DATA_DIR / "backup.db"
