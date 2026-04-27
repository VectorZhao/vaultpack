import base64
from datetime import datetime, timedelta, timezone
from io import BytesIO

import pyotp
import qrcode
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, abort, flash, jsonify, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from .backup import (
    format_source_paths,
    list_source_dirs,
    normalize_source_paths,
    parse_source_paths,
    run_due_jobs,
    run_job,
    serialize_source_paths,
)
from .config import SECRET_KEY, SOURCE_ROOT
from .db import connect, init_db, utc_now_iso
from .webdav import WebDAVClient, WebDAVConfig


def create_app():
    init_db()
    app = Flask(__name__)
    app.secret_key = SECRET_KEY
    app.permanent_session_lifetime = timedelta(days=7)
    app.jinja_env.filters["time"] = format_time
    app.jinja_env.filters["source_paths"] = format_source_paths

    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(run_due_jobs, "interval", minutes=1, id="run_due_jobs", replace_existing=True)
    scheduler.start()

    @app.before_request
    def require_login():
        if request.endpoint in {"login", "login_post", "setup", "setup_post", "static"}:
            return
        if not _has_user() and request.endpoint != "setup":
            return redirect(url_for("setup"))
        if not session.get("user_id"):
            return redirect(url_for("login"))

    @app.get("/setup")
    def setup():
        if _has_user():
            return redirect(url_for("index"))
        return render_template("setup.html")

    @app.post("/setup")
    def setup_post():
        if _has_user():
            return redirect(url_for("index"))
        username = request.form.get("username", "admin").strip() or "admin"
        password = request.form.get("password", "")
        if len(password) < 8:
            flash("密码至少需要 8 位。", "error")
            return redirect(url_for("setup"))
        with connect() as conn:
            conn.execute(
                "INSERT INTO users(id, username, password_hash) VALUES(1, ?, ?)",
                (username, generate_password_hash(password)),
            )
        flash("管理员账号已创建，请登录。", "success")
        return redirect(url_for("login"))

    @app.get("/login")
    def login():
        if not _has_user():
            return redirect(url_for("setup"))
        return render_template("login.html")

    @app.post("/login")
    def login_post():
        user = _get_user()
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        token = request.form.get("token", "").replace(" ", "")
        if not user or username != user["username"] or not check_password_hash(user["password_hash"], password):
            flash("账号或密码错误。", "error")
            return redirect(url_for("login"))
        if user["totp_enabled"]:
            if not token or not pyotp.TOTP(user["totp_secret"]).verify(token, valid_window=1):
                flash("二次验证码错误。", "error")
                return redirect(url_for("login"))
        session.permanent = True
        session["user_id"] = 1
        return redirect(url_for("index"))

    @app.post("/logout")
    def logout():
        session.clear()
        return redirect(url_for("login"))

    @app.get("/")
    def index():
        with connect() as conn:
            jobs = conn.execute("SELECT * FROM jobs ORDER BY id DESC").fetchall()
            cfg = conn.execute("SELECT * FROM webdav_config WHERE id = 1").fetchone()
            runs = conn.execute(
                "SELECT runs.*, jobs.name AS job_name FROM runs JOIN jobs ON jobs.id = runs.job_id "
                "ORDER BY runs.id DESC LIMIT 20"
            ).fetchall()
        return render_template("index.html", jobs=jobs, cfg=cfg, runs=runs, source_root=SOURCE_ROOT)

    @app.get("/webdav")
    def webdav():
        with connect() as conn:
            cfg = conn.execute("SELECT * FROM webdav_config WHERE id = 1").fetchone()
        return render_template("webdav.html", cfg=cfg)

    @app.post("/webdav")
    def webdav_post():
        base_url = request.form.get("base_url", "").strip()
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        remote_dir = request.form.get("remote_dir", "/backups").strip() or "/backups"
        with connect() as conn:
            existing = conn.execute("SELECT * FROM webdav_config WHERE id = 1").fetchone()
        if not (base_url and username):
            flash("WebDAV 地址和账号都需要填写。", "error")
            return redirect(url_for("webdav"))
        if not password and not existing:
            flash("首次配置 WebDAV 时需要填写密码。", "error")
            return redirect(url_for("webdav"))
        if not password and existing:
            password = existing["password"]
        with connect() as conn:
            conn.execute(
                "INSERT INTO webdav_config(id, base_url, username, password, remote_dir) VALUES(1, ?, ?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET base_url = excluded.base_url, username = excluded.username, "
                "password = excluded.password, remote_dir = excluded.remote_dir",
                (base_url, username, password, remote_dir),
            )
        flash("WebDAV 配置已保存。", "success")
        return redirect(url_for("webdav"))

    @app.post("/webdav/test")
    def webdav_test():
        cfg = _get_webdav_config()
        if not cfg:
            flash("请先保存 WebDAV 配置。", "error")
            return redirect(url_for("webdav"))
        try:
            WebDAVClient(cfg).test()
            flash("WebDAV 连接测试成功。", "success")
        except Exception as exc:
            flash(f"WebDAV 连接失败：{exc}", "error")
        return redirect(url_for("webdav"))

    @app.get("/jobs/new")
    def job_new():
        browser = list_source_dirs(None)
        return render_template("job_form.html", job=None, browser=browser, selected_paths=["."])

    @app.get("/api/source-dirs")
    def api_source_dirs():
        path = request.args.get("path")
        try:
            return jsonify(list_source_dirs(path))
        except Exception as exc:
            return jsonify({"error": str(exc)}), 400

    @app.post("/jobs")
    def job_create():
        values = _job_values()
        if isinstance(values, str):
            flash(values, "error")
            return redirect(url_for("job_new", path=request.form.get("source_path", "")))
        with connect() as conn:
            conn.execute(
                "INSERT INTO jobs(name, source_path, interval_days, retention_count, enabled, next_run_at, created_at) "
                "VALUES(?, ?, ?, ?, ?, ?, ?)",
                (*values, request.form.get("enabled") == "on", utc_now_iso(), utc_now_iso()),
            )
        flash("备份任务已创建。", "success")
        return redirect(url_for("index"))

    @app.get("/jobs/<int:job_id>/edit")
    def job_edit(job_id):
        job = _get_job(job_id)
        if not job:
            abort(404)
        browser = list_source_dirs(None)
        return render_template("job_form.html", job=job, browser=browser, selected_paths=parse_source_paths(job["source_path"]))

    @app.post("/jobs/<int:job_id>")
    def job_update(job_id):
        values = _job_values()
        if isinstance(values, str):
            flash(values, "error")
            return redirect(url_for("job_edit", job_id=job_id, path=request.form.get("source_path", "")))
        with connect() as conn:
            conn.execute(
                "UPDATE jobs SET name = ?, source_path = ?, interval_days = ?, retention_count = ?, "
                "enabled = ? WHERE id = ?",
                (*values, request.form.get("enabled") == "on", job_id),
            )
        flash("备份任务已更新。", "success")
        return redirect(url_for("index"))

    @app.post("/jobs/<int:job_id>/run")
    def job_run(job_id):
        run_job(job_id)
        flash("手动备份已执行，请查看运行记录。", "success")
        return redirect(url_for("index"))

    @app.post("/jobs/<int:job_id>/delete")
    def job_delete(job_id):
        with connect() as conn:
            conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        flash("备份任务已删除。", "success")
        return redirect(url_for("index"))

    @app.get("/account")
    def account():
        user = _get_user()
        secret = user["totp_secret"] or pyotp.random_base32()
        uri = pyotp.TOTP(secret).provisioning_uri(name=user["username"], issuer_name="WebDAV Backup")
        qr_data = _qr_data_uri(uri)
        return render_template("account.html", user=user, secret=secret, qr_data=qr_data)

    @app.post("/account/password")
    def account_password():
        user = _get_user()
        current = request.form.get("current_password", "")
        new_password = request.form.get("new_password", "")
        if not check_password_hash(user["password_hash"], current):
            flash("当前密码错误。", "error")
        elif len(new_password) < 8:
            flash("新密码至少需要 8 位。", "error")
        else:
            with connect() as conn:
                conn.execute("UPDATE users SET password_hash = ? WHERE id = 1", (generate_password_hash(new_password),))
            flash("密码已更新。", "success")
        return redirect(url_for("account"))

    @app.post("/account/totp/enable")
    def account_totp_enable():
        secret = request.form.get("secret", "")
        token = request.form.get("token", "").replace(" ", "")
        if not pyotp.TOTP(secret).verify(token, valid_window=1):
            flash("验证码错误，未启用二次验证。", "error")
        else:
            with connect() as conn:
                conn.execute("UPDATE users SET totp_secret = ?, totp_enabled = 1 WHERE id = 1", (secret,))
            flash("二次验证已启用。", "success")
        return redirect(url_for("account"))

    @app.post("/account/totp/disable")
    def account_totp_disable():
        token = request.form.get("token", "").replace(" ", "")
        user = _get_user()
        if user["totp_enabled"] and pyotp.TOTP(user["totp_secret"]).verify(token, valid_window=1):
            with connect() as conn:
                conn.execute("UPDATE users SET totp_enabled = 0 WHERE id = 1")
            flash("二次验证已关闭。", "success")
        else:
            flash("验证码错误，未关闭二次验证。", "error")
        return redirect(url_for("account"))

    return app


def _has_user():
    return _get_user() is not None


def _get_user():
    with connect() as conn:
        return conn.execute("SELECT * FROM users WHERE id = 1").fetchone()


def _get_job(job_id):
    with connect() as conn:
        return conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()


def _get_webdav_config():
    with connect() as conn:
        row = conn.execute("SELECT * FROM webdav_config WHERE id = 1").fetchone()
    if not row:
        return None
    return WebDAVConfig(row["base_url"], row["username"], row["password"], row["remote_dir"])


def _job_values():
    try:
        name = request.form.get("name", "").strip()
        source_paths = normalize_source_paths(parse_source_paths(request.form.get("source_path", "")))
        interval_days = int(request.form.get("interval_days", "7"))
        retention_count = int(request.form.get("retention_count", "5"))
    except Exception as exc:
        return str(exc)
    if not name:
        return "任务名称不能为空。"
    if not source_paths:
        return "至少需要选择一个备份目录。"
    if interval_days < 1:
        return "备份间隔至少为 1 天。"
    if retention_count < 1:
        return "保留版本至少为 1 个。"
    return name, serialize_source_paths(source_paths), interval_days, retention_count


def _qr_data_uri(uri):
    image = qrcode.make(uri)
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    return f"data:image/png;base64,{encoded}"


def format_time(value):
    if not value:
        return "-"
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo:
            dt = dt.astimezone()
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return value
