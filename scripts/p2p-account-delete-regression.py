#!/usr/bin/env python3
import argparse
import importlib.util
import os
import time
import urllib.error
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
THREE_NODE_PATH = ROOT / "scripts" / "p2p-three-node-regression.py"

spec = importlib.util.spec_from_file_location("p2p_three_node_regression", THREE_NODE_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError(f"cannot load {THREE_NODE_PATH}")
reg = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reg)


def portal_auth_succeeds(node: Any, password: str) -> bool:
    try:
        auth = reg.request_json(
            "POST",
            f"{node.base}/_p2p/query",
            {"action": "portal.auth", "params": {"password": password, "device_id": f"ACCOUNT-DELETE-{node.label}-OLD"}},
            "",
        )
    except reg.ApiError:
        return False
    except (ConnectionError, OSError, TimeoutError, urllib.error.URLError):
        return False
    return bool(auth.get("access_token"))


def reset_dual_stack(build: bool) -> None:
    reg.run_checked(["docker", "compose", "-f", "docker-compose.p2p-dual.yml", "down", "-v"])
    up = ["docker", "compose", "-f", "docker-compose.p2p-dual.yml", "up", "-d"]
    if build:
        up.append("--build")
    up.extend(["--force-recreate", "dendrite-a", "dendrite-b", "dendrite-c"])
    reg.run_checked(up)


def setup_nodes(suffix: int) -> tuple[Any, Any, Any]:
    nodes = [
        reg.Node("A", "http://127.0.0.1:18008", "direxio-p2p-dual-dendrite-a-1", f"{reg.PUBLIC_HOST}:18448", "", ""),
        reg.Node("B", "http://127.0.0.1:28008", "direxio-p2p-dual-dendrite-b-1", f"{reg.PUBLIC_HOST}:28448", "", ""),
        reg.Node("C", "http://127.0.0.1:38008", "direxio-p2p-dual-dendrite-c-1", f"{reg.PUBLIC_HOST}:38448", "", ""),
    ]
    for node in nodes:
        node.mxid = f"@owner:{node.server_name}"
        bootstrap = reg.read_bootstrap(node.container)
        node.password = bootstrap.get("password") or ""
        node.agent_token = bootstrap.get("agent_token") or ""
        reg.expect(bool(node.password), f"{node.label} bootstrap did not include password")
        reg.expect(bool(node.agent_token), f"{node.label} bootstrap did not include agent_token")
        reg.wait_until(
            f"{node.label} did not become healthy",
            lambda node=node: reg.request_json("GET", f"{node.base}/_p2p/health").get("status") == "ok",
            seconds=120,
        )
        reg.login(node)
        reg.update_profile(node, suffix)
    return nodes[0], nodes[1], nodes[2]


def run_account_delete_scenario(iteration: int) -> None:
    suffix = int(time.time() * 1000) + iteration
    a, b, c = setup_nodes(suffix)
    old_password = a.password

    direct_ab = reg.ensure_direct(a, b)
    direct_ac = reg.ensure_direct(a, c)

    a_group = reg.create_group(a, f"A Delete Owned Group {suffix}")
    reg.invite_and_join(a, b, a_group)
    reg.invite_and_join(a, c, a_group)

    a_channel = reg.create_channel(a, f"A Delete Owned Channel {suffix}", "private", "invite")
    reg.invite_channel_and_join(a, b, a_channel)
    reg.invite_channel_and_join(a, c, a_channel)

    b_channel = reg.create_channel(b, f"B Member Channel {suffix}", "private", "invite")
    reg.invite_channel_and_join(b, a, b_channel)
    reg.invite_channel_and_join(b, c, b_channel)

    c_group = reg.create_group(c, f"C Member Group {suffix}")
    reg.invite_and_join(c, a, c_group)
    reg.invite_and_join(c, b, c_group)

    reg.close_ws(a)
    deleted = reg.p2p(a, "command", "portal.account.delete", {"confirm": "delete_account"})
    reg.expect(deleted.get("status") == "deprovisioned", f"A account delete failed: {deleted!r}")
    reg.expect(int(deleted.get("contacts_left") or 0) >= 2, f"A account delete did not leave both contacts: {deleted!r}")
    reg.expect(int(deleted.get("groups_dissolved") or 0) >= 1, f"A account delete did not dissolve owned group: {deleted!r}")
    reg.expect(int(deleted.get("channels_dissolved") or 0) >= 1, f"A account delete did not dissolve owned channel: {deleted!r}")
    reg.expect(int(deleted.get("groups_left") or 0) >= 1, f"A account delete did not leave C group: {deleted!r}")
    reg.expect(int(deleted.get("channels_left") or 0) >= 1, f"A account delete did not leave B channel: {deleted!r}")

    reg.wait_until("B still shows deleted A direct contact", lambda: reg.contact_visible(b, a.mxid, direct_ab) is None, seconds=120)
    reg.wait_until("C still shows deleted A direct contact", lambda: reg.contact_visible(c, a.mxid, direct_ac) is None, seconds=120)
    reg.wait_until("old A password still logs in", lambda: not portal_auth_succeeds(a, old_password), seconds=120)

    for node in (b, c):
        reg.wait_until("A-owned group still visible after delete", lambda node=node: reg.room_removed_from_product_views(node, a_group, "group"), seconds=120)
        reg.wait_until(
            "A-owned channel still visible after delete",
            lambda node=node: reg.room_removed_from_product_views(node, a_channel.get("room_id") or "", "channel"),
            seconds=120,
        )

    reg.wait_until(
        "B channel still shows deleted A as joined",
        lambda: reg.joined_member(
            b,
            "channels.members",
            {"room_id": b_channel.get("room_id"), "channel_id": b_channel.get("channel_id")},
            a.mxid,
        )
        is None,
        seconds=120,
    )
    reg.wait_until("C still sees B-owned channel", lambda: reg.channel_visible(c, b_channel.get("room_id") or "") is not None, seconds=120)
    reg.wait_until(
        "C group still shows deleted A as joined",
        lambda: reg.joined_member(c, "groups.members", {"room_id": c_group}, a.mxid) is None,
        seconds=120,
    )
    reg.wait_until("B still sees C-owned group", lambda: reg.group_visible(b, c_group) is not None, seconds=120)

    reg.rebuild_node_with_empty_volumes(a, suffix + 1)
    reg.invite_channel_and_join(b, a, b_channel)
    reg.invite_and_join(c, a, c_group)
    reg.wait_until("rebuilt A missing B-owned channel", lambda: reg.channel_visible(a, b_channel.get("room_id") or "") is not None, seconds=120)
    reg.wait_until("rebuilt A missing C-owned group", lambda: reg.group_visible(a, c_group) is not None, seconds=120)


def main() -> int:
    os.environ.setdefault("P2P_DUAL_PUBLIC_HOST", reg.PUBLIC_HOST)
    parser = argparse.ArgumentParser(description="Run the focused three-node account deletion regression.")
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument("--no-build", action="store_true")
    args = parser.parse_args()

    for index in range(1, args.iterations + 1):
        reset_dual_stack(build=not args.no_build)
        run_account_delete_scenario(index)
        print(f"PASS account deletion focused regression iteration {index}")
    print("ACCOUNT_DELETE_REGRESSION_PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
