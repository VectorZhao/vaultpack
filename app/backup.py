import json
import shutil
import tarfile
from datetime import datetime, timezone
from pathlib import Path

from apscheduler.triggers.cron import CronTrigger

from .config import SOURCE_ROOT, WORK_DIR
from .db import connect, utc_now_iso
from .webdav import WebDAVClient, WebDAVConfig


def safe_source_path(relative_path):
    candidate = (SOURCE_ROOT / relative_path.strip("/")).resolve()
    if candidate != SOURCE_ROOT and SOURCE_ROOT not in candidate.parents:
        raise ValueError("目录超出允许的挂载根目录")
    if not candidate.exists() or not candidate.is_dir():
        raise ValueError("目录不存在或不是文件夹")
    return candidate


def parse_source_paths(value):
    if not value:
        return ["."]
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            paths = [str(item).strip() or "." for item in parsed]
        else:
            paths = [str(parsed).strip() or "."]
    except json.JSONDecodeError:
        paths = [value.strip() or "."]
    return normalize_source_paths(paths)


def normalize_source_paths(paths):
    normalized = []
    seen = set()
    for path in paths:
        path = "." if path in ("", ".") else path.strip("/")
        safe_source_path(path)
        if path == ".":
            return ["."]
        if path not in seen:
            seen.add(path)
            normalized.append(path)
    normalized.sort()
    return normalized


def serialize_source_paths(paths):
    return json.dumps(normalize_source_paths(paths), ensure_ascii=False)


def format_source_paths(value):
    return ", ".join(parse_source_paths(value))


def list_source_dirs(relative_path=None):
    if relative_path is None:
        return {
            "mode": "mounts",
            "current": None,
            "current_label": "挂载目录",
            "parent": None,
            "entries": [
                {
                    "name": SOURCE_ROOT.name or SOURCE_ROOT.as_posix(),
                    "label": f"{SOURCE_ROOT.name or SOURCE_ROOT.as_posix()} ({SOURCE_ROOT.as_posix()})",
                    "path": ".",
                }
            ],
        }

    root = safe_source_path(relative_path or ".")
    entries = []
    for child in sorted(root.iterdir(), key=lambda p: p.name.lower()):
        if child.is_dir() and not child.is_symlink():
            rel = child.relative_to(SOURCE_ROOT).as_posix()
            entries.append({"name": child.name, "label": child.name, "path": rel})
    parent = None
    if root != SOURCE_ROOT:
        parent = root.parent.relative_to(SOURCE_ROOT).as_posix()
        if parent == ".":
            parent = ""
    current = root.relative_to(SOURCE_ROOT).as_posix()
    current = "" if current == "." else current
    return {
        "mode": "dirs",
        "current": current,
        "current_label": current or "挂载目录",
        "parent": parent,
        "entries": entries,
    }


def due_jobs():
    now = utc_now_iso()
    with connect() as conn:
        return conn.execute(
            "SELECT * FROM jobs WHERE enabled = 1 AND (next_run_at IS NULL OR next_run_at <= ?) ORDER BY id",
            (now,),
        ).fetchall()


def run_job(job_id):
    with connect() as conn:
        job = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        cfg = None
        if job and job["destination_id"]:
            cfg = conn.execute("SELECT * FROM webdav_config WHERE id = ?", (job["destination_id"],)).fetchone()
        if job and not job["destination_id"]:
            cfg = conn.execute("SELECT * FROM webdav_config ORDER BY id LIMIT 1").fetchone()
    if not job:
        return
    if not cfg:
        _mark_job(job_id, "failed", "任务选择的存储目的地不存在", None)
        return

    started_at = utc_now_iso()
    with connect() as conn:
        run_id = conn.execute(
            "INSERT INTO runs(job_id, started_at, status) VALUES(?, ?, ?)",
            (job_id, started_at, "running"),
        ).lastrowid

    archive_path = None
    archive_name = None
    try:
        source_paths = parse_source_paths(job["source_path"])
        archive_name = _archive_name(job["id"], job["name"])
        archive_path = WORK_DIR / archive_name
        WORK_DIR.mkdir(parents=True, exist_ok=True)
        _make_archive(source_paths, archive_path)

        client = WebDAVClient(WebDAVConfig(cfg["base_url"], cfg["username"], cfg["password"], cfg["remote_dir"]))
        client.upload_file(archive_path, archive_name)
        _apply_retention(client, job["id"], job["retention_count"])

        message = f"已上传 {archive_name}"
        _finish_run(run_id, "success", message, archive_name)
        _mark_job(job_id, "success", message, next_run_from_cron(job["cron_expr"]))
    except Exception as exc:
        message = str(exc)
        _finish_run(run_id, "failed", message, archive_name)
        _mark_job(job_id, "failed", message, next_run_from_cron(job["cron_expr"]))
    finally:
        if archive_path and archive_path.exists():
            archive_path.unlink()


def run_due_jobs():
    for job in due_jobs():
        run_job(job["id"])


def _archive_name(job_id, name):
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    slug = "".join(ch if ch.isalnum() or ch in ("-", "_") else "-" for ch in name).strip("-") or "backup"
    return f"job-{job_id}-{slug}-{stamp}.tar.gz"


def _make_archive(source_paths, archive_path):
    temp_path = archive_path.with_suffix(".tmp")
    if temp_path.exists():
        temp_path.unlink()
    with tarfile.open(temp_path, "w:gz") as tar:
        for source_path in source_paths:
            source = safe_source_path(source_path)
            arcname = SOURCE_ROOT.name if source_path == "." else source_path
            tar.add(source, arcname=arcname, recursive=True)
    shutil.move(temp_path, archive_path)


def _apply_retention(client, job_id, retention_count):
    prefix = f"job-{job_id}-"
    backups = sorted(name for name in client.list_files() if name.startswith(prefix) and name.endswith(".tar.gz"))
    overflow = len(backups) - retention_count
    for name in backups[:max(0, overflow)]:
        client.delete(name)


def next_run_from_cron(cron_expr, base_time=None):
    base_time = base_time or datetime.now(timezone.utc)
    trigger = CronTrigger.from_crontab(cron_expr, timezone=timezone.utc)
    next_run = trigger.get_next_fire_time(None, base_time)
    if not next_run:
        raise ValueError("cron 表达式无法计算下次运行时间")
    return next_run.replace(microsecond=0).isoformat()


def _finish_run(run_id, status, message, archive_name):
    with connect() as conn:
        conn.execute(
            "UPDATE runs SET finished_at = ?, status = ?, message = ?, archive_name = ? WHERE id = ?",
            (utc_now_iso(), status, message, archive_name, run_id),
        )


def _mark_job(job_id, status, message, next_run_at):
    with connect() as conn:
        conn.execute(
            "UPDATE jobs SET last_run_at = ?, last_status = ?, last_message = ?, next_run_at = ? WHERE id = ?",
            (utc_now_iso(), status, message, next_run_at, job_id),
        )
