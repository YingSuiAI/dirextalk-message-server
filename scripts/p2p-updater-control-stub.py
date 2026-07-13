#!/usr/bin/env python3
"""Test-only updater desired-state controller for the P2P Compose regressions."""

from __future__ import annotations

import hmac
import json
import os
import secrets
import socketserver
import tempfile
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler
from pathlib import Path


CONTROL_PATH = "/_dirextalk/updater/v1/control/desired-state"
CONTROL_TOKEN_HEADER = "X-Dirextalk-Control-Token"
VALID_STATES = frozenset({"running", "upgrading", "maintenance", "deprovisioned"})
MAX_REQUEST_BYTES = 4096

SOCKET_PATH = Path(os.environ.get("DIREXTALK_UPDATER_SOCKET_PATH", "/run/dirextalk-updater/http.sock"))
TOKEN_PATH = Path(os.environ.get("DIREXTALK_UPDATER_CONTROL_TOKEN_PATH", "/etc/dirextalk-updater/control-token"))
STATE_PATH = Path(os.environ.get("DIREXTALK_UPDATER_STATE_PATH", "/var/lib/dirextalk-updater/desired-state.json"))
INSTANCE = os.environ.get("DIREXTALK_UPDATER_INSTANCE", "p2p").strip() or "p2p"


def atomic_write(path: Path, data: str, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def load_or_create_token() -> str:
    TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        token = TOKEN_PATH.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        token = f"{INSTANCE}-{secrets.token_urlsafe(32)}"
        atomic_write(TOKEN_PATH, token + "\n", 0o600)
    if not token:
        raise RuntimeError("updater control token is empty")
    return token


def load_or_initialize_state() -> None:
    try:
        current = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        persist_state("running")
        return
    if not isinstance(current, dict) or current.get("desired_state") not in VALID_STATES:
        raise RuntimeError("persisted updater desired state is invalid")


def persist_state(state: str) -> None:
    payload = {
        "desired_state": state,
        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    atomic_write(STATE_PATH, json.dumps(payload, separators=(",", ":")) + "\n", 0o600)


class ControlHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "DirextalkP2PUpdaterStub/1"

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        if self.path != CONTROL_PATH:
            self.respond(404, {"code": "not_found"})
            return
        supplied_token = self.headers.get(CONTROL_TOKEN_HEADER, "")
        if not hmac.compare_digest(supplied_token, self.server.control_token):
            self.respond(401, {"code": "control_token_invalid"})
            return
        try:
            content_length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            self.respond(400, {"code": "request_invalid"})
            return
        if content_length <= 0 or content_length > MAX_REQUEST_BYTES:
            self.respond(400, {"code": "request_invalid"})
            return
        try:
            request = json.loads(self.rfile.read(content_length))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self.respond(400, {"code": "request_invalid"})
            return
        state = request.get("desired_state") if isinstance(request, dict) else None
        if state not in VALID_STATES:
            self.respond(400, {"code": "desired_state_invalid"})
            return
        persist_state(state)
        self.respond(200, {"desired_state": state})

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        self.respond(405, {"code": "method_not_allowed"})

    def respond(self, status: int, payload: dict[str, str]) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, message: str, *args: object) -> None:
        # Unix-domain clients do not have an IP-style address tuple.
        print(message % args, flush=True)


class UnixControlServer(socketserver.UnixStreamServer):
    control_token: str


def main() -> None:
    control_token = load_or_create_token()
    load_or_initialize_state()
    SOCKET_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        SOCKET_PATH.unlink()
    except FileNotFoundError:
        pass
    server = UnixControlServer(str(SOCKET_PATH), ControlHandler)
    server.control_token = control_token
    os.chmod(SOCKET_PATH, 0o600)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        try:
            SOCKET_PATH.unlink()
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    main()
