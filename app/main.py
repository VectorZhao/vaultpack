import base64
from datetime import datetime, timedelta, timezone
from io import BytesIO
from threading import Thread

import pyotp
import qrcode
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, abort, flash, jsonify, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from .backup import (
    create_pending_run,
    format_source_paths,
    list_source_dirs,
    next_run_from_cron,
    normalize_source_paths,
    parse_source_paths,
    run_due_jobs,
    run_job,
    serialize_source_paths,
)
from .config import APP_TIMEZONE, SECRET_KEY, SOURCE_ROOT, TIMEZONE_NAME
from .db import connect, init_db, utc_now_iso
from .webdav import WebDAVClient, WebDAVConfig


def create_app():
    init_db()
    app = Flask(__name__)
    app.secret_key = SECRET_KEY
    app.permanent_session_lifetime = timedelta(days=7)
    app.jinja_env.filters["time"] = format_time
    app.jinja_env.filters["source_paths"] = format_source_paths
    app.jinja_env.filters["progress_percent"] = progress_percent

    @app.context_processor
    def inject_app_context():
        return {"backup_timezone": TIMEZONE_NAME}

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
            jobs = conn.execute(
                "SELECT jobs.*, webdav_config.base_url AS destination_base_url, "
                "webdav_config.remote_dir AS destination_remote_dir "
                "FROM jobs LEFT JOIN webdav_config ON webdav_config.id = jobs.destination_id "
                "ORDER BY jobs.id DESC"
            ).fetchall()
            cfg = conn.execute("SELECT * FROM webdav_config ORDER BY id LIMIT 1").fetchone()
            runs = conn.execute(
                "SELECT runs.*, jobs.name AS job_name FROM runs JOIN jobs ON jobs.id = runs.job_id "
                "ORDER BY runs.id DESC LIMIT 20"
            ).fetchall()
            running_runs = conn.execute(
                "SELECT runs.*, jobs.name AS job_name FROM runs JOIN jobs ON jobs.id = runs.job_id "
                "WHERE runs.status = 'running' ORDER BY runs.started_at DESC"
            ).fetchall()
        running_by_job = {run["job_id"]: run for run in running_runs}
        active_run = running_runs[0] if running_runs else None
        return render_template(
            "index.html",
            jobs=jobs,
            cfg=cfg,
            runs=runs,
            running_by_job=running_by_job,
            active_run=active_run,
            source_root=SOURCE_ROOT,
        )

    @app.get("/webdav")
    def webdav():
        cfg = _get_webdav_row()
        if not cfg:
            return redirect(url_for("webdav_new"))
        return redirect(url_for("webdav_detail", config_id=cfg["id"]))

    @app.get("/webdav/<int:config_id>")
    def webdav_detail(config_id):
        with connect() as conn:
            cfg = conn.execute("SELECT * FROM webdav_config WHERE id = ?", (config_id,)).fetchone()
        if not cfg:
            abort(404)
        return render_template("webdav.html", cfg=cfg, form_title="管理 WebDAV 目的地", is_new=False)

    @app.get("/destinations/new/webdav")
    def webdav_new():
        return render_template("webdav.html", cfg=None, form_title="新建 WebDAV 目的地", is_new=True)

    @app.get("/destinations")
    def destinations():
        with connect() as conn:
            configs = conn.execute("SELECT * FROM webdav_config ORDER BY id").fetchall()
        return render_template("destinations.html", configs=configs)

    @app.get("/destinations/new")
    def destination_new():
        return render_template("destination_new.html")

    @app.get("/restore")
    def restore():
        return render_template("placeholder.html", title="恢复", message="恢复功能将在后续版本加入。")

    @app.get("/about")
    def about():
        return render_template("about.html")

    @app.post("/webdav/<int:config_id>")
    def webdav_post(config_id):
        result = _save_webdav_config(config_id)
        if isinstance(result, str):
            flash(result, "error")
            return redirect(url_for("webdav_detail", config_id=config_id))
        flash("WebDAV 配置已保存。", "success")
        return redirect(url_for("webdav_detail", config_id=config_id))

    @app.post("/webdav/<int:config_id>/delete")
    def webdav_delete(config_id):
        with connect() as conn:
            cfg = conn.execute("SELECT * FROM webdav_config WHERE id = ?", (config_id,)).fetchone()
            if not cfg:
                abort(404)
            used_by = conn.execute(
                "SELECT COUNT(*) AS count FROM jobs WHERE destination_id = ?",
                (config_id,),
            ).fetchone()["count"]
            if used_by:
                flash(f"WebDAV #{config_id} 正在被 {used_by} 个备份任务使用，不能删除。", "error")
            else:
                conn.execute("DELETE FROM webdav_config WHERE id = ?", (config_id,))
                flash(f"WebDAV #{config_id} 已删除。", "success")
        return redirect(url_for("destinations"))

    @app.post("/destinations/new/webdav")
    def webdav_new_post():
        result = _webdav_config_from_form(require_password=True)
        if isinstance(result, str):
            flash(result, "error")
            return redirect(url_for("webdav_new"))
        try:
            WebDAVClient(result).test()
        except Exception as exc:
            flash(f"WebDAV 连接测试失败，未保存：{exc}", "error")
            return redirect(url_for("webdav_new"))
        _insert_webdav_config(result)
        flash("WebDAV 目的地已保存，连接测试成功。", "success")
        return redirect(url_for("destinations"))

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

    @app.post("/webdav/<int:config_id>/test")
    def webdav_test_config(config_id):
        cfg = _get_webdav_config(config_id)
        if not cfg:
            abort(404)
        try:
            WebDAVClient(cfg).test()
            flash("WebDAV 连接测试成功。", "success")
        except Exception as exc:
            flash(f"WebDAV 连接失败：{exc}", "error")
        return redirect(url_for("webdav_detail", config_id=config_id))

    @app.get("/jobs/new")
    def job_new():
        browser = list_source_dirs(None)
        destinations = _get_webdav_rows()
        return render_template("job_form.html", job=None, browser=browser, selected_paths=["."], destinations=destinations)

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
                "INSERT INTO jobs(name, destination_id, source_path, interval_days, cron_expr, retention_count, enabled, next_run_at, created_at) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    values[0],
                    values[1],
                    values[2],
                    1,
                    values[3],
                    values[4],
                    request.form.get("enabled") == "on",
                    next_run_from_cron(values[3]),
                    utc_now_iso(),
                ),
            )
        flash("备份任务已创建。", "success")
        return redirect(url_for("index"))

    @app.get("/jobs/<int:job_id>/edit")
    def job_edit(job_id):
        job = _get_job(job_id)
        if not job:
            abort(404)
        browser = list_source_dirs(None)
        destinations = _get_webdav_rows()
        return render_template(
            "job_form.html",
            job=job,
            browser=browser,
            selected_paths=parse_source_paths(job["source_path"]),
            destinations=destinations,
        )

    @app.post("/jobs/<int:job_id>")
    def job_update(job_id):
        values = _job_values()
        if isinstance(values, str):
            flash(values, "error")
            return redirect(url_for("job_edit", job_id=job_id, path=request.form.get("source_path", "")))
        with connect() as conn:
            conn.execute(
                "UPDATE jobs SET name = ?, destination_id = ?, source_path = ?, cron_expr = ?, retention_count = ?, "
                "enabled = ?, next_run_at = ? WHERE id = ?",
                (*values, request.form.get("enabled") == "on", next_run_from_cron(values[3]), job_id),
            )
        flash("备份任务已更新。", "success")
        return redirect(url_for("index"))

    @app.post("/jobs/<int:job_id>/run")
    def job_run(job_id):
        if not _get_job(job_id):
            abort(404)
        with connect() as conn:
            running = conn.execute(
                "SELECT 1 FROM runs WHERE job_id = ? AND status = 'running' LIMIT 1",
                (job_id,),
            ).fetchone()
        if running:
            flash("这个任务已经在运行中。", "error")
        else:
            run_id = create_pending_run(job_id)
            Thread(target=run_job, args=(job_id, run_id), daemon=True).start()
            flash("手动备份已开始，请查看进度。", "success")
        return redirect(url_for("index"))

    @app.post("/jobs/<int:job_id>/delete")
    def job_delete(job_id):
        with connect() as conn:
            job = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if not job:
                abort(404)
            running = conn.execute(
                "SELECT 1 FROM runs WHERE job_id = ? AND status = 'running' LIMIT 1",
                (job_id,),
            ).fetchone()
            if running:
                flash("这个任务正在运行中，不能删除。", "error")
            else:
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

    @app.post("/account/username")
    def account_username():
        user = _get_user()
        username = request.form.get("username", "").strip()
        current = request.form.get("current_password", "")
        if not username:
            flash("用户名不能为空。", "error")
        elif len(username) > 64:
            flash("用户名不能超过 64 个字符。", "error")
        elif not check_password_hash(user["password_hash"], current):
            flash("当前密码错误，用户名未更新。", "error")
        else:
            with connect() as conn:
                conn.execute("UPDATE users SET username = ? WHERE id = 1", (username,))
            flash("用户名已更新。", "success")
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


def _get_webdav_row(config_id=None):
    with connect() as conn:
        if config_id is None:
            return conn.execute("SELECT * FROM webdav_config ORDER BY id LIMIT 1").fetchone()
        return conn.execute("SELECT * FROM webdav_config WHERE id = ?", (config_id,)).fetchone()


def _get_webdav_rows():
    with connect() as conn:
        return conn.execute("SELECT * FROM webdav_config ORDER BY id").fetchall()


def _get_webdav_config(config_id=None):
    row = _get_webdav_row(config_id)
    if not row:
        return None
    return _webdav_config_from_row(row)


def _webdav_config_from_row(row):
    return WebDAVConfig(row["base_url"], row["username"], row["password"], row["remote_dir"])


def _webdav_config_from_form(require_password=False, existing=None):
    base_url = request.form.get("base_url", "").strip()
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "")
    remote_dir = request.form.get("remote_dir", "").strip()
    if not (base_url and username and remote_dir):
        return "WebDAV 地址、账号和远端备份目录都需要填写。"
    if not password and (require_password or not existing):
        return "首次配置 WebDAV 时需要填写密码。"
    if not password and existing:
        password = existing["password"]
    return WebDAVConfig(base_url, username, password, remote_dir)


def _save_webdav_config(config_id):
    existing = _get_webdav_row(config_id)
    if not existing:
        abort(404)
    config = _webdav_config_from_form(existing=existing)
    if isinstance(config, str):
        return config
    with connect() as conn:
        conn.execute(
            "UPDATE webdav_config SET base_url = ?, username = ?, password = ?, remote_dir = ? WHERE id = ?",
            (config.base_url, config.username, config.password, config.remote_dir, config_id),
        )
    return config


def _insert_webdav_config(config):
    with connect() as conn:
        conn.execute(
            "INSERT INTO webdav_config(base_url, username, password, remote_dir) VALUES(?, ?, ?, ?)",
            (config.base_url, config.username, config.password, config.remote_dir),
        )


def _job_values():
    try:
        name = request.form.get("name", "").strip()
        source_paths = normalize_source_paths(parse_source_paths(request.form.get("source_path", "")))
        cron_expr = " ".join(request.form.get("cron_expr", "").split())
        retention_count = int(request.form.get("retention_count", "5"))
    except Exception as exc:
        return str(exc)
    try:
        destination_id = int(request.form.get("destination_id", "0"))
    except ValueError:
        return "请选择有效的存储目的地。"
    if not name:
        return "任务名称不能为空。"
    if not _get_webdav_row(destination_id):
        return "请选择有效的存储目的地。"
    if not source_paths:
        return "至少需要选择一个备份目录。"
    if not cron_expr:
        return "备份时间不能为空，请填写 cron 表达式。"
    try:
        next_run_from_cron(cron_expr)
    except Exception as exc:
        return f"cron 表达式无效：{exc}"
    if retention_count < 1:
        return "保留版本至少为 1 个。"
    return name, destination_id, serialize_source_paths(source_paths), cron_expr, retention_count


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
            dt = dt.astimezone(APP_TIMEZONE)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return value


def progress_percent(run):
    if not run or not run["progress_total"]:
        return 0
    return max(0, min(100, round((run["progress_current"] / run["progress_total"]) * 100)))
