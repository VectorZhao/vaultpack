import base64
import json
import os
import secrets
import socket
import time
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
    enqueue_agent_run,
    format_bytes,
    format_source_paths,
    list_source_dirs,
    next_run_from_cron,
    normalize_source_paths,
    parse_source_paths,
    run_due_jobs,
    run_job,
)
from .config import ADMIN_PASSWORD, ADMIN_USERNAME, APP_TIMEZONE, APP_VERSION, SECRET_KEY, SOURCE_ROOT, TIMEZONE_NAME
from .db import connect, get_setting, init_db, set_setting, utc_now_iso
from .webdav import WebDAVClient, WebDAVConfig


def create_app():
    init_db()
    _bootstrap_admin_from_env()
    app = Flask(__name__)
    app.secret_key = SECRET_KEY
    app.permanent_session_lifetime = timedelta(days=7)
    app.jinja_env.filters["time"] = format_time
    app.jinja_env.filters["source_paths"] = format_source_paths
    app.jinja_env.filters["source_count"] = format_source_count
    app.jinja_env.filters["source_chips"] = source_chips
    app.jinja_env.filters["progress_percent"] = progress_percent
    app.jinja_env.filters["duration"] = format_duration
    app.jinja_env.filters["bytes"] = format_size
    app.jinja_env.filters["cron_label"] = cron_label

    @app.context_processor
    def inject_app_context():
        return {"app_version": APP_VERSION, "backup_timezone": TIMEZONE_NAME, "topbar": _topbar_context()}

    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(run_due_jobs, "interval", minutes=1, id="run_due_jobs", replace_existing=True)
    scheduler.start()

    @app.before_request
    def require_login():
        if request.endpoint and request.endpoint.startswith("api_agent_"):
            return
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
        _mark_stale_nodes()
        with connect() as conn:
            all_jobs = _fetch_jobs_with_meta(conn)
            cfg = conn.execute("SELECT * FROM webdav_config ORDER BY id LIMIT 1").fetchone()
            configs = conn.execute("SELECT * FROM webdav_config ORDER BY id").fetchall()
            runs = conn.execute(
                "SELECT runs.*, jobs.name AS job_name, nodes.name AS node_name FROM runs "
                "JOIN jobs ON jobs.id = runs.job_id LEFT JOIN nodes ON nodes.id = runs.node_id "
                "ORDER BY runs.id DESC LIMIT 5"
            ).fetchall()
            running_runs = conn.execute(
                "SELECT runs.*, jobs.name AS job_name, nodes.name AS node_name FROM runs "
                "JOIN jobs ON jobs.id = runs.job_id LEFT JOIN nodes ON nodes.id = runs.node_id "
                "WHERE runs.status = 'running' ORDER BY runs.started_at DESC"
            ).fetchall()
        running_by_job = {run["job_id"]: run for run in running_runs}
        active_run = running_runs[0] if running_runs else None
        return render_template(
            "index.html",
            jobs=all_jobs[:3],
            cfg=cfg,
            runs=runs,
            running_by_job=running_by_job,
            active_run=active_run,
            dashboard=_dashboard_context(all_jobs, runs, running_runs, configs),
            configs=configs,
            source_root=SOURCE_ROOT,
        )

    @app.get("/jobs")
    def jobs():
        _mark_stale_nodes()
        with connect() as conn:
            jobs = _fetch_jobs_with_meta(conn)
            running_runs = conn.execute(
                "SELECT runs.*, jobs.name AS job_name, nodes.name AS node_name FROM runs "
                "JOIN jobs ON jobs.id = runs.job_id LEFT JOIN nodes ON nodes.id = runs.node_id "
                "WHERE runs.status = 'running' ORDER BY runs.started_at DESC"
            ).fetchall()
        return render_template("jobs.html", jobs=jobs, running_by_job={run["job_id"]: run for run in running_runs})

    @app.get("/runs")
    def runs():
        with connect() as conn:
            runs = conn.execute(
                "SELECT runs.*, jobs.name AS job_name, nodes.name AS node_name FROM runs "
                "JOIN jobs ON jobs.id = runs.job_id LEFT JOIN nodes ON nodes.id = runs.node_id "
                "ORDER BY runs.id DESC"
            ).fetchall()
        return render_template("runs.html", runs=runs)

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

    @app.get("/nodes")
    def nodes():
        _mark_stale_nodes()
        token = _enrollment_token()
        panel_url = request.host_url.rstrip("/")
        with connect() as conn:
            nodes = conn.execute("SELECT * FROM nodes ORDER BY mode DESC, id").fetchall()
        return render_template("nodes.html", nodes=nodes, enrollment_token=token, panel_url=panel_url)

    @app.post("/nodes/enrollment-token")
    def nodes_rotate_enrollment_token():
        token = secrets.token_urlsafe(32)
        set_setting("agent_enroll_token", token)
        flash("节点接入令牌已重新生成。", "success")
        return redirect(url_for("nodes"))

    @app.get("/destinations/new")
    def destination_new():
        return render_template("destination_new.html")

    @app.get("/restore")
    def restore():
        return render_template(
            "placeholder.html",
            title="恢复",
            message="恢复功能将在后续版本加入。当前可以在 WebDAV 目的地中查看已上传的备份包。",
            action_label="返回首页",
            action_url=url_for("index"),
        )

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
        destinations = _get_webdav_rows()
        nodes = _get_nodes()
        node_id = int(request.args.get("node_id") or (nodes[0]["id"] if nodes else 1))
        node = _get_node(node_id) or (nodes[0] if nodes else None)
        browser = _browser_for_node(node)
        return render_template(
            "job_form.html",
            job=None,
            browser=browser,
            selected_paths=["."],
            destinations=destinations,
            nodes=nodes,
            selected_node_id=node["id"] if node else None,
        )

    @app.get("/api/source-dirs")
    def api_source_dirs():
        path = request.args.get("path")
        try:
            node_id = int(request.args.get("node_id") or "1")
        except ValueError:
            return jsonify({"error": "节点无效"}), 400
        node = _get_node(node_id)
        if not node:
            return jsonify({"error": "节点不存在"}), 404
        try:
            if node["mode"] == "local":
                return jsonify(list_source_dirs(path))
            return jsonify(_request_agent_dirs(node, path))
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
                "INSERT INTO jobs(name, node_id, destination_id, source_path, interval_days, cron_expr, retention_count, enabled, next_run_at, created_at) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    values[0],
                    values[1],
                    values[2],
                    values[3],
                    1,
                    values[4],
                    values[5],
                    request.form.get("enabled") == "on",
                    next_run_from_cron(values[4]),
                    utc_now_iso(),
                ),
            )
        flash("备份任务已创建。", "success")
        return redirect(url_for("jobs"))

    @app.get("/jobs/<int:job_id>/edit")
    def job_edit(job_id):
        job = _get_job(job_id)
        if not job:
            abort(404)
        destinations = _get_webdav_rows()
        nodes = _get_nodes()
        try:
            requested_node_id = int(request.args.get("node_id") or job["node_id"])
        except ValueError:
            requested_node_id = job["node_id"]
        node = _get_node(requested_node_id) or _get_node(job["node_id"]) or (nodes[0] if nodes else None)
        browser = _browser_for_node(node)
        return render_template(
            "job_form.html",
            job=job,
            browser=browser,
            selected_paths=_display_source_paths(job["source_path"]),
            destinations=destinations,
            nodes=nodes,
            selected_node_id=node["id"] if node else job["node_id"],
        )

    @app.post("/jobs/<int:job_id>")
    def job_update(job_id):
        values = _job_values()
        if isinstance(values, str):
            flash(values, "error")
            return redirect(url_for("job_edit", job_id=job_id, path=request.form.get("source_path", "")))
        with connect() as conn:
            conn.execute(
                "UPDATE jobs SET name = ?, node_id = ?, destination_id = ?, source_path = ?, cron_expr = ?, retention_count = ?, "
                "enabled = ?, next_run_at = ? WHERE id = ?",
                (*values, request.form.get("enabled") == "on", next_run_from_cron(values[4]), job_id),
            )
        flash("备份任务已更新。", "success")
        return redirect(url_for("jobs"))

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
            job = _get_job(job_id)
            node = _get_node(job["node_id"]) if job else None
            if node and node["mode"] != "local":
                enqueue_agent_run(job_id, run_id)
                flash("手动备份已下发到节点，请查看运行记录。", "success")
            else:
                Thread(target=run_job, args=(job_id, run_id), daemon=True).start()
                flash("手动备份已开始，请查看进度。", "success")
        return _redirect_next()

    @app.post("/jobs/<int:job_id>/toggle")
    def job_toggle(job_id):
        with connect() as conn:
            job = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if not job:
                abort(404)
            running = conn.execute(
                "SELECT 1 FROM runs WHERE job_id = ? AND status = 'running' LIMIT 1",
                (job_id,),
            ).fetchone()
            if running and job["enabled"]:
                flash("这个任务正在运行中，不能停用。", "error")
                return _redirect_next()
            next_enabled = not bool(job["enabled"])
            next_run_at = next_run_from_cron(job["cron_expr"]) if next_enabled else None
            conn.execute(
                "UPDATE jobs SET enabled = ?, next_run_at = ? WHERE id = ?",
                (next_enabled, next_run_at, job_id),
            )
        flash("备份任务已启用。" if next_enabled else "备份任务已停用。", "success")
        return _redirect_next()

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
        return _redirect_next()

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

    @app.post("/api/agent/register")
    def api_agent_register():
        payload = request.get_json(silent=True) or {}
        if payload.get("enroll_token") != _enrollment_token():
            return jsonify({"error": "接入令牌无效"}), 401
        name = (payload.get("name") or payload.get("hostname") or "agent").strip()[:64]
        hostname = (payload.get("hostname") or name).strip()[:128]
        source_root = (payload.get("source_root") or "/backup-source").strip()[:255]
        version = (payload.get("version") or APP_VERSION).strip()[:64]
        token = secrets.token_urlsafe(32)
        with connect() as conn:
            duplicate = conn.execute(
                "SELECT 1 FROM nodes WHERE name = ? OR (hostname = ? AND source_root = ? AND mode = 'agent')",
                (name, hostname, source_root),
            ).fetchone()
            if duplicate:
                return jsonify({"error": "节点已存在，请删除旧节点或使用新的节点名称"}), 409
            node_id = conn.execute(
                "INSERT INTO nodes(name, hostname, source_root, token_hash, status, enabled, mode, version, last_seen_at, created_at) "
                "VALUES(?, ?, ?, ?, 'online', 1, 'agent', ?, ?, ?)",
                (name, hostname, source_root, generate_password_hash(token), version, utc_now_iso(), utc_now_iso()),
            ).lastrowid
        return jsonify({"node_id": node_id, "agent_token": token})

    @app.post("/api/agent/heartbeat")
    def api_agent_heartbeat():
        node = _agent_node_from_request()
        if not node:
            return jsonify({"error": "节点认证失败"}), 401
        payload = request.get_json(silent=True) or {}
        _update_node_seen(node["id"], payload)
        return jsonify({"ok": True})

    @app.post("/api/agent/poll")
    def api_agent_poll():
        node = _agent_node_from_request()
        if not node:
            return jsonify({"error": "节点认证失败"}), 401
        _update_node_seen(node["id"], request.get_json(silent=True) or {})
        command = None
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            with connect() as conn:
                command = conn.execute(
                    "SELECT * FROM agent_commands WHERE node_id = ? AND status = 'pending' ORDER BY id LIMIT 1",
                    (node["id"],),
                ).fetchone()
                if command:
                    conn.execute(
                        "UPDATE agent_commands SET status = 'claimed', claimed_at = ? WHERE id = ?",
                        (utc_now_iso(), command["id"]),
                    )
                    break
            time.sleep(1)
        if not command:
            return jsonify({"command": None})
        _update_node_seen(node["id"])
        return jsonify({
            "command": {
                "id": command["id"],
                "type": command["type"],
                "run_id": command["run_id"],
                "payload": json.loads(command["payload"]),
            }
        })

    @app.post("/api/agent/commands/<int:command_id>/finish")
    def api_agent_command_finish(command_id):
        node = _agent_node_from_request()
        if not node:
            return jsonify({"error": "节点认证失败"}), 401
        payload = request.get_json(silent=True) or {}
        status = "success" if payload.get("status") == "success" else "failed"
        with connect() as conn:
            command = conn.execute(
                "SELECT * FROM agent_commands WHERE id = ? AND node_id = ?",
                (command_id, node["id"]),
            ).fetchone()
            if not command:
                return jsonify({"error": "命令不存在"}), 404
            stored_payload = _redact_command_payload(command["payload"]) if status in {"success", "failed"} else command["payload"]
            conn.execute(
                "UPDATE agent_commands SET status = ?, finished_at = ?, error = ?, result = ?, payload = ? WHERE id = ?",
                (
                    status,
                    utc_now_iso(),
                    payload.get("error"),
                    json.dumps(payload.get("result"), ensure_ascii=False) if payload.get("result") is not None else None,
                    stored_payload,
                    command_id,
                ),
            )
        return jsonify({"ok": True})

    @app.post("/api/agent/runs/<int:run_id>/progress")
    def api_agent_run_progress(run_id):
        node = _agent_node_from_request()
        if not node:
            return jsonify({"error": "节点认证失败"}), 401
        payload = request.get_json(silent=True) or {}
        label = payload.get("label") or payload.get("message") or "正在运行..."
        with connect() as conn:
            run = conn.execute("SELECT * FROM runs WHERE id = ? AND node_id = ?", (run_id, node["id"])).fetchone()
            if not run:
                return jsonify({"error": "运行记录不存在"}), 404
            conn.execute(
                "UPDATE runs SET progress_current = ?, progress_total = ?, progress_label = ?, message = ? WHERE id = ?",
                (int(payload.get("current") or 0), int(payload.get("total") or 0), label, label, run_id),
            )
        return jsonify({"ok": True})

    @app.post("/api/agent/runs/<int:run_id>/finish")
    def api_agent_run_finish(run_id):
        node = _agent_node_from_request()
        if not node:
            return jsonify({"error": "节点认证失败"}), 401
        payload = request.get_json(silent=True) or {}
        status = "success" if payload.get("status") == "success" else "failed"
        message = payload.get("message") or ("备份成功" if status == "success" else "备份失败")
        with connect() as conn:
            run = conn.execute(
                "SELECT runs.*, jobs.cron_expr FROM runs JOIN jobs ON jobs.id = runs.job_id WHERE runs.id = ? AND runs.node_id = ?",
                (run_id, node["id"]),
            ).fetchone()
            if not run:
                return jsonify({"error": "运行记录不存在"}), 404
            conn.execute(
                "UPDATE runs SET finished_at = ?, status = ?, message = ?, progress_label = ?, archive_name = ? WHERE id = ?",
                (utc_now_iso(), status, message, message, payload.get("archive_name"), run_id),
            )
            conn.execute(
                "UPDATE jobs SET last_run_at = ?, last_status = ?, last_message = ?, next_run_at = ? WHERE id = ?",
                (utc_now_iso(), status, message, next_run_from_cron(run["cron_expr"]), run["job_id"]),
            )
        return jsonify({"ok": True})

    return app


def _has_user():
    return _get_user() is not None


def _bootstrap_admin_from_env():
    if _has_user() or not ADMIN_PASSWORD:
        return
    username = ADMIN_USERNAME.strip() or "admin"
    if len(ADMIN_PASSWORD) < 8:
        raise RuntimeError("ADMIN_PASSWORD must be at least 8 characters")
    with connect() as conn:
        conn.execute(
            "INSERT INTO users(id, username, password_hash) VALUES(1, ?, ?)",
            (username, generate_password_hash(ADMIN_PASSWORD)),
        )


def _get_user():
    with connect() as conn:
        return conn.execute("SELECT * FROM users WHERE id = 1").fetchone()


def _get_job(job_id):
    with connect() as conn:
        return conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()


def _fetch_jobs_with_meta(conn):
    return conn.execute(
        "SELECT jobs.*, webdav_config.base_url AS destination_base_url, "
        "webdav_config.remote_dir AS destination_remote_dir, nodes.name AS node_name, nodes.mode AS node_mode "
        "FROM jobs LEFT JOIN webdav_config ON webdav_config.id = jobs.destination_id "
        "LEFT JOIN nodes ON nodes.id = jobs.node_id "
        "ORDER BY jobs.id DESC"
    ).fetchall()


def _redirect_next(default_endpoint="index"):
    next_url = request.form.get("next", "").strip()
    if next_url.startswith("/") and not next_url.startswith("//"):
        return redirect(next_url)
    return redirect(url_for(default_endpoint))


def _get_node(node_id):
    with connect() as conn:
        return conn.execute("SELECT * FROM nodes WHERE id = ?", (node_id,)).fetchone()


def _get_nodes():
    with connect() as conn:
        return conn.execute("SELECT * FROM nodes WHERE enabled = 1 ORDER BY mode DESC, id").fetchall()


def _enrollment_token():
    token = get_setting("agent_enroll_token")
    if token:
        return token
    token = secrets.token_urlsafe(32)
    set_setting("agent_enroll_token", token)
    return token


def _agent_node_from_request():
    header = request.headers.get("Authorization", "")
    token = header.removeprefix("Bearer ").strip()
    if not token:
        return None
    with connect() as conn:
        nodes = conn.execute("SELECT * FROM nodes WHERE mode = 'agent' AND enabled = 1").fetchall()
    for node in nodes:
        if node["token_hash"] and check_password_hash(node["token_hash"], token):
            return node
    return None


def _update_node_seen(node_id, payload=None):
    payload = payload or {}
    with connect() as conn:
        conn.execute(
            "UPDATE nodes SET status = 'online', hostname = COALESCE(?, hostname), source_root = COALESCE(?, source_root), "
            "version = COALESCE(?, version), last_seen_at = ? WHERE id = ?",
            (
                payload.get("hostname"),
                payload.get("source_root"),
                payload.get("version"),
                utc_now_iso(),
                node_id,
            ),
        )


def _mark_stale_nodes():
    now = utc_now_iso()
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=2)).replace(microsecond=0).isoformat()
    with connect() as conn:
        conn.execute(
            "UPDATE nodes SET status = 'online', hostname = ?, source_root = ?, version = ?, last_seen_at = ? "
            "WHERE mode = 'local'",
            (socket.gethostname(), SOURCE_ROOT.as_posix(), APP_VERSION, now),
        )
        conn.execute(
            "UPDATE nodes SET status = 'offline' WHERE mode = 'agent' AND (last_seen_at IS NULL OR last_seen_at < ?)",
            (cutoff,),
        )


def _browser_for_node(node):
    if not node or node["mode"] == "local":
        return list_source_dirs(None)
    return {
        "mode": "mounts",
        "current": None,
        "current_label": "挂载目录",
        "parent": None,
        "entries": [
            {
                "name": node["name"],
                "label": f"{node['name']} ({node['source_root']})",
                "path": ".",
            }
        ],
    }


def _request_agent_dirs(node, path):
    payload = {"path": path}
    with connect() as conn:
        command_id = conn.execute(
            "INSERT INTO agent_commands(node_id, type, payload, status, created_at) VALUES(?, ?, ?, 'pending', ?)",
            (node["id"], "list_dirs", json.dumps(payload, ensure_ascii=False), utc_now_iso()),
        ).lastrowid
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        time.sleep(0.4)
        with connect() as conn:
            command = conn.execute("SELECT * FROM agent_commands WHERE id = ?", (command_id,)).fetchone()
        if command["status"] == "success":
            return json.loads(command["result"] or "{}")
        if command["status"] == "failed":
            raise RuntimeError(command["error"] or "节点读取目录失败")
    raise TimeoutError("节点响应超时，请确认 Agent 在线")


def _redact_command_payload(value):
    try:
        payload = json.loads(value or "{}")
    except json.JSONDecodeError:
        return value
    destination = payload.get("destination")
    if isinstance(destination, dict) and "password" in destination:
        destination["password"] = ""
    return json.dumps(payload, ensure_ascii=False)


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


def _topbar_context():
    topbar = {
        "title": _topbar_title(),
        "scheduler_label": "调度运行中",
        "next_label": "暂无计划任务",
        "next_time": "暂无计划",
        "node_name": os.environ.get("VAULTPACK_NODE_NAME") or socket.gethostname(),
        "active_run": None,
        "active_percent": 0,
    }
    if not session.get("user_id"):
        return topbar
    try:
        with connect() as conn:
            active_run = conn.execute(
                "SELECT runs.*, jobs.name AS job_name FROM runs JOIN jobs ON jobs.id = runs.job_id "
                "WHERE runs.status = 'running' ORDER BY runs.started_at DESC LIMIT 1"
            ).fetchone()
            next_job = conn.execute(
                "SELECT name, next_run_at FROM jobs "
                "WHERE enabled = 1 AND next_run_at IS NOT NULL "
                "ORDER BY next_run_at ASC LIMIT 1"
            ).fetchone()
    except Exception:
        return topbar

    if active_run:
        topbar["active_run"] = active_run
        topbar["active_percent"] = progress_percent(active_run)
        topbar["scheduler_label"] = "正在备份"
    if next_job:
        topbar["next_label"] = f"下次运行：{format_time(next_job['next_run_at'])} · {next_job['name']}"
        topbar["next_time"] = format_time(next_job["next_run_at"])
    return topbar


def _topbar_title():
    endpoint = request.endpoint or ""
    title_map = {
        "index": "备份控制台",
        "jobs": "备份任务",
        "job_new": "新建备份",
        "job_create": "新建备份",
        "job_edit": "编辑备份",
        "job_update": "编辑备份",
        "restore": "恢复",
        "destinations": "存储目的地",
        "nodes": "节点",
        "nodes_rotate_enrollment_token": "节点",
        "destination_new": "添加存储目的地",
        "webdav": "WebDAV 目的地",
        "webdav_detail": "管理 WebDAV",
        "webdav_new": "新建 WebDAV",
        "webdav_post": "管理 WebDAV",
        "webdav_new_post": "新建 WebDAV",
        "account": "设置",
        "account_username": "设置",
        "account_password": "设置",
        "account_totp_enable": "设置",
        "account_totp_disable": "设置",
        "about": "关于 vaultpack",
        "runs": "运行记录",
    }
    return title_map.get(endpoint, "备份控制台")


def _dashboard_context(jobs, runs, running_runs, configs):
    jobs = list(jobs)
    runs = list(runs)
    configs = list(configs)
    latest_run = runs[0] if runs else None
    next_job = min(
        (job for job in jobs if job["enabled"] and job["next_run_at"]),
        key=lambda job: job["next_run_at"],
        default=None,
    )
    abnormal_jobs = [
        job for job in jobs
        if job["last_status"] == "failed" or not job["destination_base_url"]
    ]
    enabled_count = sum(1 for job in jobs if job["enabled"])
    return {
        "total_jobs": len(jobs),
        "enabled_jobs": enabled_count,
        "disabled_jobs": len(jobs) - enabled_count,
        "latest_run": latest_run,
        "next_job": next_job,
        "next_distance": relative_time_until(next_job["next_run_at"]) if next_job else "暂无计划",
        "abnormal_jobs": len(abnormal_jobs),
        "running_jobs": len(running_runs),
        "destinations": len(configs),
        "healthy_destinations": len(configs),
        "health_percent": 100 if jobs and not abnormal_jobs else (0 if abnormal_jobs else 100),
    }


def _job_values():
    try:
        name = request.form.get("name", "").strip()
        cron_expr = " ".join(request.form.get("cron_expr", "").split())
        retention_count = int(request.form.get("retention_count", "5"))
    except Exception as exc:
        return str(exc)
    try:
        node_id = int(request.form.get("node_id", "0"))
    except ValueError:
        return "请选择有效的节点。"
    try:
        destination_id = int(request.form.get("destination_id", "0"))
    except ValueError:
        return "请选择有效的存储目的地。"
    node = _get_node(node_id)
    if not name:
        return "任务名称不能为空。"
    if not node or not node["enabled"]:
        return "请选择有效的节点。"
    if not _get_webdav_row(destination_id):
        return "请选择有效的存储目的地。"
    try:
        if node["mode"] == "local":
            source_paths = normalize_source_paths(parse_source_paths(request.form.get("source_path", "")))
        else:
            source_paths = _normalize_remote_source_paths(request.form.get("source_path", ""))
    except Exception as exc:
        return str(exc)
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
    return name, node_id, destination_id, json.dumps(source_paths, ensure_ascii=False), cron_expr, retention_count


def _normalize_remote_source_paths(value):
    if not value:
        return ["."]
    try:
        parsed = json.loads(value)
        paths = parsed if isinstance(parsed, list) else [parsed]
    except json.JSONDecodeError:
        paths = [value]
    normalized = []
    seen = set()
    for item in paths:
        path = str(item).strip()
        path = "." if path in ("", ".") else path.strip("/")
        if ".." in path.split("/"):
            raise ValueError("目录不能包含上级路径")
        if path == ".":
            return ["."]
        if path not in seen:
            normalized.append(path)
            seen.add(path)
    normalized.sort()
    return normalized


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


def format_date_time_short(value):
    if not value:
        return "-"
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo:
            dt = dt.astimezone(APP_TIMEZONE)
        return dt.strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return value


def relative_time_until(value):
    if not value:
        return "暂无计划"
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo:
            dt = dt.astimezone(APP_TIMEZONE)
        now = datetime.now(APP_TIMEZONE)
        seconds = int((dt - now).total_seconds())
    except ValueError:
        return "时间未知"
    if seconds <= 0:
        return "等待调度"
    minutes = seconds // 60
    hours = minutes // 60
    days = hours // 24
    if days:
        return f"距离 {days} 天 {hours % 24} 小时"
    if hours:
        return f"距离 {hours} 小时 {minutes % 60} 分钟"
    return f"距离 {max(minutes, 1)} 分钟"


def format_duration(started_at, finished_at=None):
    if not started_at:
        return "-"
    try:
        start = datetime.fromisoformat(started_at)
        end = datetime.fromisoformat(finished_at) if finished_at else datetime.now(timezone.utc)
        if start.tzinfo and end.tzinfo:
            end = end.astimezone(start.tzinfo)
        seconds = max(0, int((end - start).total_seconds()))
    except ValueError:
        return "-"
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def format_source_count(value):
    count = len(_display_source_paths(value))
    return f"已选择 {count} 个目录"


def source_chips(value, limit=3):
    paths = _display_source_paths(value)
    chips = paths[:limit]
    extra = max(0, len(paths) - len(chips))
    return {"chips": chips, "extra": extra, "total": len(paths)}


def _display_source_paths(value):
    if not value:
        return ["."]
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
        return [str(parsed)]
    except json.JSONDecodeError:
        return [value]


def format_size(value):
    return format_bytes(value or 0)


def cron_label(value):
    fields = (value or "").split()
    if len(fields) != 5:
        return value or "-"
    minute, hour, day, month, weekday = fields
    if not (minute.isdigit() and hour.isdigit()):
        return value
    time_label = f"{int(hour):02d}:{int(minute):02d}"
    if day == "*" and month == "*" and weekday == "*":
        return f"每天 {time_label}"
    if day.startswith("*/") and month == "*" and weekday == "*":
        interval = day[2:]
        if interval.isdigit():
            return f"每 {int(interval)} 天 {time_label}"
    if day == "*" and month == "*" and weekday.startswith("*/"):
        interval = weekday[2:]
        if interval.isdigit():
            return f"每 {int(interval)} 周 {time_label}"
    if day.isdigit() and month == "*" and weekday == "*":
        return f"每月 {int(day)} 日 {time_label}"
    return value


def progress_percent(run):
    if not run or not run["progress_total"]:
        return 0
    return max(0, min(100, round((run["progress_current"] / run["progress_total"]) * 100)))
