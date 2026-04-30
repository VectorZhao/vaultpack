import json
import re
import shutil
import tarfile
import time
from datetime import datetime
from pathlib import Path

from .config import APP_TIMEZONE, SOURCE_ROOT, WORK_DIR
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
            "SELECT jobs.*, nodes.mode AS node_mode FROM jobs "
            "LEFT JOIN nodes ON nodes.id = jobs.node_id "
            "WHERE jobs.enabled = 1 AND (jobs.next_run_at IS NULL OR jobs.next_run_at <= ?) ORDER BY jobs.id",
            (now,),
        ).fetchall()


def run_job(job_id, run_id=None):
    with connect() as conn:
        job = conn.execute(
            "SELECT jobs.*, nodes.name AS node_name FROM jobs LEFT JOIN nodes ON nodes.id = jobs.node_id WHERE jobs.id = ?",
            (job_id,),
        ).fetchone()
        cfg = None
        if job and job["destination_id"]:
            cfg = conn.execute("SELECT * FROM webdav_config WHERE id = ?", (job["destination_id"],)).fetchone()
        if job and not job["destination_id"]:
            cfg = conn.execute("SELECT * FROM webdav_config ORDER BY id LIMIT 1").fetchone()
    if not job:
        return
    if not cfg:
        run_id = run_id or _create_run(job_id, "任务选择的存储目的地不存在", job["node_id"] if job else None)
        _finish_run(run_id, "failed", "任务选择的存储目的地不存在", None)
        _mark_job(job_id, "failed", "任务选择的存储目的地不存在", None)
        return

    run_id = run_id or _create_run(job_id, "正在准备备份...", job["node_id"])
    result = run_backup_payload(
        dict(job),
        dict(cfg),
        lambda current, total, label: _update_run_progress(run_id, current, total, label),
    )
    _finish_run(run_id, result["status"], result["message"], result.get("archive_name"))
    _mark_job(job_id, result["status"], result["message"], next_run_from_cron(job["cron_expr"]))


def run_backup_payload(job, cfg, progress_callback=None):
    def progress(current, total, label):
        if progress_callback:
            progress_callback(current, total, label)

    archive_path = None
    archive_name = None
    try:
        progress(0, 0, "正在扫描文件...")
        source_paths = parse_source_paths(job["source_path"])
        archive_name = _archive_name(job["id"], job.get("node_name"))
        archive_path = WORK_DIR / archive_name
        WORK_DIR.mkdir(parents=True, exist_ok=True)
        _make_archive(source_paths, archive_path, progress)

        client = WebDAVClient(WebDAVConfig(cfg["base_url"], cfg["username"], cfg["password"], cfg["remote_dir"]))
        archive_size = archive_path.stat().st_size
        progress(0, archive_size, f"正在上传：剩余 {format_bytes(archive_size)}")
        upload_state = {"last_update": 0}
        client.upload_file(
            archive_path,
            archive_name,
            progress_callback=lambda sent, total: _upload_progress(sent, total, upload_state, progress),
        )
        progress(archive_size, archive_size, "正在清理旧版本...")
        _apply_retention(client, job["id"], int(job["retention_count"]))
        return {
            "status": "success",
            "message": f"已上传 {archive_name}",
            "archive_name": archive_name,
            "progress_total": archive_size,
        }
    except Exception as exc:
        return {"status": "failed", "message": str(exc), "archive_name": archive_name}
    finally:
        if archive_path and archive_path.exists():
            archive_path.unlink()


def run_due_jobs():
    for job in due_jobs():
        if job["node_mode"] == "local":
            run_job(job["id"])
        else:
            enqueue_agent_run(job["id"])


def create_pending_run(job_id):
    with connect() as conn:
        job = conn.execute("SELECT node_id FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return _create_run(job_id, "等待开始备份...", job["node_id"] if job else None)


def _create_run(job_id, label, node_id=None):
    with connect() as conn:
        return _create_run_in_conn(conn, job_id, label, node_id)


def _create_run_in_conn(conn, job_id, label, node_id=None):
    return conn.execute(
        "INSERT INTO runs(job_id, node_id, started_at, status, message, progress_label) VALUES(?, ?, ?, ?, ?, ?)",
        (job_id, node_id, utc_now_iso(), "running", label, label),
    ).lastrowid


def enqueue_agent_run(job_id, run_id=None):
    with connect() as conn:
        job = conn.execute(
            "SELECT jobs.*, nodes.mode AS node_mode FROM jobs LEFT JOIN nodes ON nodes.id = jobs.node_id WHERE jobs.id = ?",
            (job_id,),
        ).fetchone()
    if not job:
        return None
    if job["node_mode"] == "local":
        return run_job(job_id, run_id)
    with connect() as conn:
        job = conn.execute(
            "SELECT jobs.*, nodes.mode AS node_mode FROM jobs LEFT JOIN nodes ON nodes.id = jobs.node_id WHERE jobs.id = ?",
            (job_id,),
        ).fetchone()
        cfg = conn.execute("SELECT * FROM webdav_config WHERE id = ?", (job["destination_id"],)).fetchone()
        node = conn.execute("SELECT * FROM nodes WHERE id = ?", (job["node_id"],)).fetchone()
        if not cfg or not node or not node["enabled"]:
            run_id = run_id or _create_run_in_conn(conn, job_id, "节点或存储目的地不可用", job["node_id"])
            conn.execute(
                "UPDATE runs SET finished_at = ?, status = ?, message = ?, progress_label = ?, archive_name = ? WHERE id = ?",
                (utc_now_iso(), "failed", "节点或存储目的地不可用", "节点或存储目的地不可用", None, run_id),
            )
            conn.execute(
                "UPDATE jobs SET last_run_at = ?, last_status = ?, last_message = ?, next_run_at = ? WHERE id = ?",
                (utc_now_iso(), "failed", "节点或存储目的地不可用", next_run_from_cron(job["cron_expr"]), job_id),
            )
            return run_id
        running = conn.execute(
            "SELECT id FROM runs WHERE job_id = ? AND status = 'running' LIMIT 1",
            (job_id,),
        ).fetchone()
        if running and not run_id:
            return running["id"]
        run_id = run_id or _create_run_in_conn(conn, job_id, "等待节点领取任务...", job["node_id"])
        payload = {
            "run_id": run_id,
            "job": {
                "id": job["id"],
                "name": job["name"],
                "node_name": node["name"],
                "source_path": job["source_path"],
                "retention_count": job["retention_count"],
                "cron_expr": job["cron_expr"],
            },
            "destination": {
                "type": "webdav",
                "base_url": cfg["base_url"],
                "username": cfg["username"],
                "password": cfg["password"],
                "remote_dir": cfg["remote_dir"],
            },
        }
        conn.execute(
            "INSERT INTO agent_commands(node_id, run_id, type, payload, status, created_at) VALUES(?, ?, ?, ?, ?, ?)",
            (job["node_id"], run_id, "run_backup", json.dumps(payload, ensure_ascii=False), "pending", utc_now_iso()),
        )
        conn.execute(
            "UPDATE runs SET message = ?, progress_label = ? WHERE id = ?",
            ("等待节点领取任务...", "等待节点领取任务...", run_id),
        )
        return run_id


def _archive_name(job_id, node_name):
    stamp = datetime.now(APP_TIMEZONE).strftime("%Y%m%d-%H%M")
    node_slug = _filename_slug(node_name or "local")
    return f"{node_slug}-j{job_id}-{stamp}.tar.gz"


def _filename_slug(value):
    slug = re.sub(r"[^A-Za-z0-9_-]+", "-", str(value or "").strip().lower())
    return slug.strip("-") or "local"


def _make_archive(source_paths, archive_path, progress_callback):
    temp_path = archive_path.with_suffix(".tmp")
    if temp_path.exists():
        temp_path.unlink()
    entries, total_files, total_bytes = _collect_archive_entries(source_paths)
    total_bytes = max(total_bytes, 1)
    files_done = 0
    bytes_done = 0
    last_update = 0
    progress_callback(
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
                progress_callback(
                    min(bytes_done, total_bytes),
                    total_bytes,
                    f"正在压缩：剩余 {remaining_files} 个文件（{format_bytes(remaining_bytes)}）",
                )
                last_update = now
    if total_files == 0:
        progress_callback(total_bytes, total_bytes, "压缩完成，准备上传...")
    shutil.move(temp_path, archive_path)


def _apply_retention(client, job_id, retention_count):
    legacy_prefix = f"job-{job_id}-"
    new_pattern = re.compile(rf"^[A-Za-z0-9_-]+-j{re.escape(str(job_id))}-\d{{8}}-\d{{4}}\.tar\.gz$")
    backups = sorted(
        (
            name
            for name in client.list_files()
            if name.endswith(".tar.gz") and (name.startswith(legacy_prefix) or new_pattern.match(name))
        ),
        key=_retention_sort_key,
    )
    overflow = len(backups) - retention_count
    for name in backups[:max(0, overflow)]:
        client.delete(name)


def _retention_sort_key(name):
    new_match = re.search(r"-(\d{8})-(\d{4})\.tar\.gz$", name)
    if new_match:
        return f"{new_match.group(1)}{new_match.group(2)}00"
    legacy_match = re.search(r"-(\d{8})T(\d{6})Z\.tar\.gz$", name)
    if legacy_match:
        return f"{legacy_match.group(1)}{legacy_match.group(2)}"
    return name


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


def _upload_progress(sent, total, state, progress_callback):
    now = time.monotonic()
    if sent < total and now - state["last_update"] < 0.5:
        return
    state["last_update"] = now
    remaining = max(total - sent, 0)
    progress_callback(sent, max(total, 1), f"正在上传：剩余 {format_bytes(remaining)}")


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
