#!/usr/bin/env python3
import argparse
import json
import os
import pathlib
import time
import urllib.error
import urllib.parse
import urllib.request


def request(method, url, body=None, token=""):
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=15) as response:
        raw = response.read()
    return json.loads(raw) if raw else {}


def action(base, kind, name, params, token=""):
    return request("POST", f"{base}/_p2p/{kind}", {"action": name, "params": params}, token)


def login(base, password, device):
    result = action(base, "query", "portal.auth", {"password": password, "device_id": device})
    token = result.get("access_token", "")
    if not token:
        raise RuntimeError("portal.auth did not return an access token")
    return token


def wait(base, version, allow_status_only=False):
    expected = version.removeprefix("v")
    last_error = None
    for _ in range(180):
        try:
            health = request("GET", f"{base}/_p2p/health")
            actual = str(health.get("version", "")).removeprefix("v")
            if health.get("status") == "ok" and (
                actual == expected
                or (allow_status_only and expected == "0.15.2" and actual == "")
            ):
                return
            last_error = RuntimeError(f"health version {actual!r}, expected {expected!r}")
        except (OSError, ValueError, urllib.error.HTTPError) as exc:
            last_error = exc
        time.sleep(1)
    raise RuntimeError(f"server did not become healthy: {last_error}")


def seed(base, bootstrap_path, state_path):
    bootstrap = json.loads(pathlib.Path(bootstrap_path).read_text(encoding="utf-8"))
    password = bootstrap.get("password", "")
    if not password:
        raise RuntimeError("bootstrap password is missing")
    token = login(base, password, "RELEASE-SOURCE")
    marker = "dirextalk-release-retained-profile"
    profile = action(base, "command", "profile.update", {"display_name": marker, "avatar_url": "mxc://localhost/release-retained"}, token)
    if profile.get("display_name") != marker:
        raise RuntimeError("profile.update did not persist the marker")
    room = request("POST", f"{base}/_matrix/client/v3/createRoom", {"name": "Release retained-data room", "preset": "private_chat"}, token)
    room_id = room.get("room_id", "")
    if not room_id:
        raise RuntimeError("Matrix createRoom did not return room_id")
    message = "dirextalk-release-retained-message"
    encoded_room = urllib.parse.quote(room_id, safe="")
    event = request("PUT", f"{base}/_matrix/client/v3/rooms/{encoded_room}/send/m.room.message/release-retained", {"msgtype": "m.text", "body": message}, token)
    if not event.get("event_id"):
        raise RuntimeError("Matrix send did not return event_id")
    state = {"password": password, "profile": marker, "room_id": room_id, "message": message}
    path = pathlib.Path(state_path)
    path.write_text(json.dumps(state, separators=(",", ":")), encoding="utf-8")
    os.chmod(path, 0o600)


def verify(base, state_path, version):
    state = json.loads(pathlib.Path(state_path).read_text(encoding="utf-8"))
    token = login(base, state["password"], "RELEASE-TARGET")
    profile = action(base, "query", "profile.get", {}, token)
    if profile.get("display_name") != state["profile"]:
        raise RuntimeError("profile marker did not survive upgrade")
    room = urllib.parse.quote(state["room_id"], safe="")
    messages = request("GET", f"{base}/_matrix/client/v3/rooms/{room}/messages?dir=b&limit=50", token=token)
    if not any(event.get("content", {}).get("body") == state["message"] for event in messages.get("chunk", [])):
        raise RuntimeError("Matrix message did not survive upgrade")
    wait(base, version)


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    wait_parser = subparsers.add_parser("wait")
    wait_parser.add_argument("--base", required=True)
    wait_parser.add_argument("--version", required=True)
    wait_parser.add_argument("--allow-status-only", action="store_true")
    seed_parser = subparsers.add_parser("seed")
    seed_parser.add_argument("--base", required=True)
    seed_parser.add_argument("--bootstrap", required=True)
    seed_parser.add_argument("--state", required=True)
    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("--base", required=True)
    verify_parser.add_argument("--state", required=True)
    verify_parser.add_argument("--version", required=True)
    args = parser.parse_args()
    if args.command == "wait":
        wait(args.base, args.version, args.allow_status_only)
    elif args.command == "seed":
        seed(args.base, args.bootstrap, args.state)
    else:
        verify(args.base, args.state, args.version)


if __name__ == "__main__":
    main()
