import json
import shutil
import tarfile
import time
from datetime import datetime, timezone
from pathlib import Path

from .config import SOURCE_ROOT, WORK_DIR
from .db import connect, utc_now_iso
from .schedule import next_run_from_cron
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


def run_job(job_id, run_id=None):
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
        run_id = run_id or _create_run(job_id, "任务选择的存储目的地不存在")
        _finish_run(run_id, "failed", "任务选择的存储目的地不存在", None)
        _mark_job(job_id, "failed", "任务选择的存储目的地不存在", None)
        return

    run_id = run_id or _create_run(job_id, "正在准备备份...")

    archive_path = None
    archive_name = None
    try:
        _update_run_progress(run_id, 0, 0, "正在扫描文件...")
        source_paths = parse_source_paths(job["source_path"])
        archive_name = _archive_name(job["id"], job["name"])
        archive_path = WORK_DIR / archive_name
        WORK_DIR.mkdir(parents=True, exist_ok=True)
        _make_archive(source_paths, archive_path, run_id)

        client = WebDAVClient(WebDAVConfig(cfg["base_url"], cfg["username"], cfg["password"], cfg["remote_dir"]))
        archive_size = archive_path.stat().st_size
        _update_run_progress(run_id, 0, archive_size, f"正在上传：剩余 {format_bytes(archive_size)}")
        upload_state = {"last_update": 0}
        client.upload_file(
            archive_path,
            archive_name,
            progress_callback=lambda sent, total: _update_upload_progress(run_id, sent, total, upload_state),
        )
        _update_run_progress(run_id, archive_size, archive_size, "正在清理旧版本...")
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


def create_pending_run(job_id):
    return _create_run(job_id, "等待开始备份...")


def _create_run(job_id, label):
    with connect() as conn:
        return conn.execute(
            "INSERT INTO runs(job_id, started_at, status, message, progress_label) VALUES(?, ?, ?, ?, ?)",
            (job_id, utc_now_iso(), "running", label, label),
        ).lastrowid


def _archive_name(job_id, name):
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    slug = "".join(ch if ch.isalnum() or ch in ("-", "_") else "-" for ch in name).strip("-") or "backup"
    return f"job-{job_id}-{slug}-{stamp}.tar.gz"


def _make_archive(source_paths, archive_path, run_id):
    temp_path = archive_path.with_suffix(".tmp")
    if temp_path.exists():
        temp_path.unlink()
    entries, total_files, total_bytes = _collect_archive_entries(source_paths)
    total_bytes = max(total_bytes, 1)
    files_done = 0
    bytes_done = 0
    last_update = 0
    _update_run_progress(
        run_id,
        bytes_done,
        total_bytes,
        f"正在压缩：剩余 {total_files} 个文件（{format_bytes(total_bytes)}）",
    )
    with tarfile.open(temp_path, "w:gz") as tar:
        for entry, arcname, size, is_dir in entries:
            tar.add(entry, arcname=arcname, recursive=False)
            if is_dir:
                continue
            files_done += 1
            bytes_done += size
            now = time.monotonic()
            if files_done == total_files or now - last_update >= 0.5:
                remaining_files = max(total_files - files_done, 0)
                remaining_bytes = max(total_bytes - bytes_done, 0)
                _update_run_progress(
                    run_id,
                    min(bytes_done, total_bytes),
                    total_bytes,
                    f"正在压缩：剩余 {remaining_files} 个文件（{format_bytes(remaining_bytes)}）",
                )
                last_update = now
    if total_files == 0:
        _update_run_progress(run_id, total_bytes, total_bytes, "压缩完成，准备上传...")
    shutil.move(temp_path, archive_path)


def _apply_retention(client, job_id, retention_count):
    prefix = f"job-{job_id}-"
    backups = sorted(name for name in client.list_files() if name.startswith(prefix) and name.endswith(".tar.gz"))
    overflow = len(backups) - retention_count
    for name in backups[:max(0, overflow)]:
        client.delete(name)


def _collect_archive_entries(source_paths):
    entries = []
    total_files = 0
    total_bytes = 0
    for source_path in source_paths:
        source = safe_source_path(source_path)
        arcroot = SOURCE_ROOT.name if source_path == "." else source_path
        entries.append((source, arcroot, 0, True))
        for root, dirnames, filenames in source.walk():
            dirnames[:] = sorted(name for name in dirnames if not (root / name).is_symlink())
            for dirname in dirnames:
                directory = root / dirname
                arcname = directory.relative_to(source).as_posix()
                arcname = f"{arcroot}/{arcname}" if arcname else arcroot
                entries.append((directory, arcname, 0, True))
            for filename in sorted(filenames):
                file_path = root / filename
                if file_path.is_symlink() or not file_path.is_file():
                    continue
                size = file_path.stat().st_size
                arcname = file_path.relative_to(source).as_posix()
                arcname = f"{arcroot}/{arcname}" if arcname else arcroot
                entries.append((file_path, arcname, size, False))
                total_files += 1
                total_bytes += size
    return entries, total_files, total_bytes


def _update_upload_progress(run_id, sent, total, state):
    now = time.monotonic()
    if sent < total and now - state["last_update"] < 0.5:
        return
    state["last_update"] = now
    remaining = max(total - sent, 0)
    _update_run_progress(run_id, sent, max(total, 1), f"正在上传：剩余 {format_bytes(remaining)}")


def _update_run_progress(run_id, current, total, label):
    with connect() as conn:
        conn.execute(
            "UPDATE runs SET progress_current = ?, progress_total = ?, progress_label = ?, message = ? WHERE id = ?",
            (int(current), int(total), label, label, run_id),
        )


def format_bytes(size):
    size = float(size or 0)
    units = ("B", "KB", "MB", "GB", "TB")
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.2f} {unit}"
        size /= 1024


def _finish_run(run_id, status, message, archive_name):
    with connect() as conn:
        conn.execute(
            "UPDATE runs SET finished_at = ?, status = ?, message = ?, progress_label = ?, archive_name = ? WHERE id = ?",
            (utc_now_iso(), status, message, message, archive_name, run_id),
        )


def _mark_job(job_id, status, message, next_run_at):
    with connect() as conn:
        conn.execute(
            "UPDATE jobs SET last_run_at = ?, last_status = ?, last_message = ?, next_run_at = ? WHERE id = ?",
            (utc_now_iso(), status, message, next_run_at, job_id),
        )
