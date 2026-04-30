import socket
import time
from pathlib import Path

import requests

from .backup import list_source_dirs, run_backup_payload
from .config import (
    AGENT_ENROLL_TOKEN,
    AGENT_NAME,
    AGENT_POLL_INTERVAL,
    AGENT_TOKEN,
    APP_VERSION,
    DATA_DIR,
    PANEL_URL,
    SOURCE_ROOT,
)


TOKEN_PATH = DATA_DIR / "agent-token"


def main():
    if not PANEL_URL:
        raise RuntimeError("PANEL_URL is required when VAULTPACK_ROLE=agent")
    token = _load_token()
    if not token:
        token = _register()
        _save_token(token)
        print("Agent registered. Persist this token as AGENT_TOKEN if /data is not mounted:", token, flush=True)

    session = requests.Session()
    headers = {"Authorization": f"Bearer {token}"}
    heartbeat = {
        "name": _agent_name(),
        "hostname": socket.gethostname(),
        "source_root": SOURCE_ROOT.as_posix(),
        "version": APP_VERSION,
    }
    while True:
        try:
            session.post(f"{PANEL_URL}/api/agent/heartbeat", json=heartbeat, headers=headers, timeout=20).raise_for_status()
            response = session.post(f"{PANEL_URL}/api/agent/poll", json=heartbeat, headers=headers, timeout=60)
            response.raise_for_status()
            command = response.json().get("command")
            if command:
                _handle_command(session, headers, command)
        except Exception as exc:
            print(f"Agent loop error: {exc}", flush=True)
        time.sleep(AGENT_POLL_INTERVAL)


def _load_token():
    if AGENT_TOKEN:
        return AGENT_TOKEN
    if TOKEN_PATH.exists():
        return TOKEN_PATH.read_text().strip()
    return ""


def _save_token(token):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    TOKEN_PATH.write_text(token)
    TOKEN_PATH.chmod(0o600)


def _register():
    if not AGENT_ENROLL_TOKEN:
        raise RuntimeError("AGENT_ENROLL_TOKEN or AGENT_TOKEN is required when VAULTPACK_ROLE=agent")
    payload = {
        "enroll_token": AGENT_ENROLL_TOKEN,
        "name": _agent_name(),
        "hostname": socket.gethostname(),
        "source_root": SOURCE_ROOT.as_posix(),
        "version": APP_VERSION,
    }
    response = requests.post(f"{PANEL_URL}/api/agent/register", json=payload, timeout=30)
    response.raise_for_status()
    return response.json()["agent_token"]


def _agent_name():
    return AGENT_NAME.strip() or socket.gethostname()


def _handle_command(session, headers, command):
    command_id = command["id"]
    command_type = command["type"]
    payload = command.get("payload") or {}
    try:
        run_id = payload.get("run_id")
        if command_type == "list_dirs":
            result = list_source_dirs(payload.get("path"))
            _finish_command(session, headers, command_id, "success", result=result)
            return
        if command_type == "run_backup":
            run_id = payload["run_id"]
            payload["job"].setdefault("node_name", _agent_name())

            def progress(current, total, label):
                session.post(
                    f"{PANEL_URL}/api/agent/runs/{run_id}/progress",
                    json={"current": current, "total": total, "label": label},
                    headers=headers,
                    timeout=20,
                ).raise_for_status()

            result = run_backup_payload(payload["job"], payload["destination"], progress)
            session.post(
                f"{PANEL_URL}/api/agent/runs/{run_id}/finish",
                json=result,
                headers=headers,
                timeout=30,
            ).raise_for_status()
            _finish_command(session, headers, command_id, result["status"], result=result, error=None if result["status"] == "success" else result["message"])
            return
        _finish_command(session, headers, command_id, "failed", error=f"unknown command type: {command_type}")
    except Exception as exc:
        if payload.get("run_id"):
            try:
                session.post(
                    f"{PANEL_URL}/api/agent/runs/{payload['run_id']}/finish",
                    json={"status": "failed", "message": str(exc)},
                    headers=headers,
                    timeout=20,
                )
            except Exception:
                pass
        _finish_command(session, headers, command_id, "failed", error=str(exc))


def _finish_command(session, headers, command_id, status, result=None, error=None):
    session.post(
        f"{PANEL_URL}/api/agent/commands/{command_id}/finish",
        json={"status": status, "result": result, "error": error},
        headers=headers,
        timeout=20,
    ).raise_for_status()


if __name__ == "__main__":
    main()
