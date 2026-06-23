#!/usr/bin/env python3
import json
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Optional


WAIT_SECONDS = 60
PUBLIC_HOST = "host.docker.internal"


@dataclass
class Node:
    label: str
    base: str
    container: str
    server_name: str
    mxid: str
    password: str
    token: str = ""
    agent_token: str = ""
    name: str = ""
    avatar: str = ""


class ApiError(Exception):
    def __init__(self, status: int, body: Any, message: str):
        super().__init__(message)
        self.status = status
        self.body = body


def run(args: list[str]) -> str:
    return subprocess.check_output(args, text=True).strip()


def read_bootstrap(container: str) -> dict[str, Any]:
    raw = run(["docker", "exec", container, "cat", "/var/direxio-message-server/p2p/bootstrap.json"])
    return dict(json.loads(raw))


def request_json(method: str, url: str, body: Any = None, token: str = "") -> Any:
    data = None if body is None else json.dumps(body).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as res:
            raw = res.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            parsed = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            parsed = raw
        raise ApiError(exc.code, parsed, f"{method} {url} failed with {exc.code}") from exc


def p2p(node: Node, kind: str, action: str, params: Optional[dict[str, Any]] = None, *, token: Optional[str] = None) -> Any:
    bearer = node.token if token is None else token
    try:
        return request_json(
            "POST",
            f"{node.base}/_p2p/{kind}",
            {"action": action, "params": params or {}},
            bearer,
        )
    except ApiError as exc:
        if exc.status != 401 or token is not None:
            raise
        login(node)
        return request_json(
            "POST",
            f"{node.base}/_p2p/{kind}",
            {"action": action, "params": params or {}},
            node.token,
        )


def p2p_status(node: Node, kind: str, action: str, params: Optional[dict[str, Any]] = None, *, token: Optional[str] = None) -> tuple[int, Any]:
    try:
        return 200, p2p(node, kind, action, params, token=token)
    except ApiError as exc:
        return exc.status, exc.body


def mcp(node: Node, kind: str, action: str, params: Optional[dict[str, Any]] = None) -> Any:
    return p2p(node, kind, action, params, token=node.agent_token)


def matrix_send_text(node: Node, room_id: str, text: str) -> str:
    room_path = urllib.parse.quote(room_id, safe="")
    txn_id = f"three_node_{int(time.time() * 1000)}"
    body = {"msgtype": "m.text", "body": text}
    res = request_json(
        "PUT",
        f"{node.base}/_matrix/client/v3/rooms/{room_path}/send/m.room.message/{txn_id}",
        body,
        node.token,
    )
    return res.get("event_id", "")


def matrix_messages(node: Node, room_id: str) -> list[Any]:
    room_path = urllib.parse.quote(room_id, safe="")
    res = request_json(
        "GET",
        f"{node.base}/_matrix/client/v3/rooms/{room_path}/messages?dir=b&limit=30",
        token=node.token,
    )
    return list(res.get("chunk") or [])


def wait_until(label: str, fn, seconds: int = WAIT_SECONDS):
    deadline = time.monotonic() + seconds
    last_error: Optional[Exception] = None
    while time.monotonic() < deadline:
        try:
            value = fn()
            if value:
                return value
        except Exception as exc:  # keep polling federation/projection lag
            last_error = exc
        time.sleep(0.75)
    if last_error:
        raise AssertionError(f"{label}: {last_error}") from last_error
    raise AssertionError(label)


def expect(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def find_by(items: list[Any], **expected) -> Optional[Any]:
    for item in items:
        if all(item.get(key) == value for key, value in expected.items()):
            return item
    return None


def conversations(node: Node) -> list[dict[str, Any]]:
    res = p2p(node, "query", "conversations.list")
    return list(res.get("conversations") or [])


def conversation_for(node: Node, room_id: str) -> Optional[dict[str, Any]]:
    return find_by(conversations(node), matrix_room_id=room_id)


def assert_caps(
    conversation: dict[str, Any],
    *,
    send: bool,
    send_media: bool,
    call: bool,
    label: str,
    extra: Optional[dict[str, bool]] = None,
) -> None:
    caps = conversation.get("capabilities") or {}
    expected_caps = {
        "open": True,
        "send": send,
        "send_media": send_media,
        "call": call,
    }
    expected_caps.update(extra or {})
    for key, expected in expected_caps.items():
        expect(caps.get(key) is expected, f"{label} capability {key} expected {expected}, got {caps.get(key)!r}")


def login(node: Node) -> None:
    auth = p2p(
        node,
        "query",
        "portal.auth",
        {"password": node.password, "device_id": f"THREE-{node.label}"},
        token="",
    )
    node.token = auth.get("access_token") or ""
    expect(bool(node.token), f"{node.label} portal.auth did not return access_token")


def update_profile(node: Node, suffix: int) -> None:
    node.name = f"{node.label} Three {suffix}"
    node.avatar = f"mxc://{node.server_name}/three-{node.label.lower()}-{suffix}"
    profile = p2p(
        node,
        "command",
        "profile.update",
        {"display_name": node.name, "avatar_url": node.avatar},
    )
    expect(profile.get("display_name") == node.name, f"{node.label} profile.update did not persist display name")
    login(node)


def ensure_direct(a: Node, b: Node) -> str:
    contact = p2p(a, "command", "contacts.request", {"mxid": b.mxid, "display_name": b.name})
    room_id = contact.get("room_id") or ""
    expect(bool(room_id), "contacts.request did not return room_id")
    if contact.get("status") == "pending_outbound":
        wait_until(
            "B did not receive inbound contact request",
            lambda: find_by(
                list((p2p(b, "command", "sync.bootstrap").get("contacts") or [])),
                room_id=room_id,
                status="pending_inbound",
            ),
        )
        accepted = p2p(
            b,
            "command",
            "contacts.requests.accept",
            {"room_id": room_id, "peer_mxid": a.mxid, "display_name": a.name, "domain": a.server_name},
        )
        expect(accepted.get("status") == "accepted", "contacts.requests.accept did not accept")
    else:
        expect(contact.get("status") == "accepted", f"unexpected direct contact status {contact.get('status')!r}")

    wait_until(
        "A did not project accepted direct conversation",
        lambda: (conversation_for(a, room_id) or {}).get("relationship_status") == "accepted",
    )
    if not (conversation_for(b, room_id) or {}).get("relationship_status") == "accepted":
        restored = p2p(b, "command", "contacts.request", {"mxid": a.mxid, "display_name": a.name})
        expect(restored.get("status") == "accepted", "peer-side contacts.request did not repair accepted direct state")
        expect(restored.get("room_id") == room_id, "peer-side contacts.request did not reuse direct room")
    wait_until(
        "B did not project accepted direct conversation",
        lambda: (conversation_for(b, room_id) or {}).get("relationship_status") == "accepted",
    )
    assert_caps(conversation_for(a, room_id) or {}, send=True, send_media=True, call=True, label="A direct")
    assert_caps(conversation_for(b, room_id) or {}, send=True, send_media=True, call=True, label="B direct")
    return room_id


def verify_delete_readd(deleter: Node, peer: Node, room_id: str) -> None:
    deleted = p2p(deleter, "command", "contacts.delete", {"room_id": room_id})
    expect(deleted.get("status") == "ok", "contacts.delete did not return ok")
    restored = p2p(deleter, "command", "contacts.request", {"mxid": peer.mxid, "display_name": peer.name})
    expect(restored.get("status") == "accepted", "deleted contact re-request did not restore accepted state")
    expect(restored.get("room_id") == room_id, "deleted contact re-request did not reuse old direct room")
    wait_until(
        "restored direct conversation did not reopen",
        lambda: (conversation_for(deleter, room_id) or {}).get("relationship_status") == "accepted",
    )


def create_group(owner: Node, name: str) -> str:
    group = p2p(owner, "command", "groups.create", {"name": name, "topic": "three-node regression"})
    room_id = group.get("room_id") or ""
    expect(bool(room_id), f"groups.create {name} did not return room_id")
    return room_id


def invite_and_join(owner: Node, invitee: Node, room_id: str) -> None:
    p2p(owner, "command", "groups.invite", {"room_id": room_id, "user_id": invitee.mxid, "display_name": invitee.name})
    wait_until(
        f"{invitee.label} did not receive group invite",
        lambda: find_by(list((p2p(invitee, "command", "sync.bootstrap").get("pending", {}).get("group_invites") or [])), id=room_id),
    )
    joined = p2p(
        invitee,
        "command",
        "groups.join",
        {"room_id": room_id, "server_names": [owner.server_name], "display_name": invitee.name, "avatar_url": invitee.avatar},
    )
    expect(joined.get("status") == "ok", f"{invitee.label} groups.join did not return ok")
    wait_until(
        f"{invitee.label} groups.list missing joined group",
        lambda: find_by(list((p2p(invitee, "command", "groups.list").get("groups") or [])), room_id=room_id),
    )


def assert_members(owner: Node, room_id: str, expected: list[Node]) -> None:
    def ready():
        members = list((p2p(owner, "command", "groups.members", {"room_id": room_id}).get("members") or []))
        for node in expected:
            match = find_by(members, user_id=node.mxid, membership="join")
            if not match or match.get("display_name") != node.name:
                return None
        return members

    members = wait_until("group members did not project joined display names", ready)
    owner_names = [m.get("display_name") for m in members if m.get("display_name") == "owner"]
    expect(not owner_names, "group members collapsed to fallback display name 'owner'")


def assert_message_projection(nodes: list[Node], room_id: str, text: str) -> None:
    for node in nodes:
        conv = wait_until(
            f"{node.label} conversation preview did not receive group message",
            lambda n=node: conversation_for(n, room_id) if (conversation_for(n, room_id) or {}).get("last_message") == text else None,
        )
        assert_caps(conv, send=True, send_media=True, call=True, label=f"{node.label} group")


def assert_history(node: Node, room_id: str, text: str) -> None:
    def has_message():
        for event in matrix_messages(node, room_id):
            if (event.get("content") or {}).get("body") == text:
                return True
        return False

    wait_until(f"{node.label} Matrix /messages did not return historical group message", has_message)


def assert_mcp_group_tools(nodes: list[Node], room_id: str, suffix: int) -> None:
    for sender in nodes:
        search = mcp(sender, "query", "mcp.rooms.search", {"type": "group", "limit": 100})
        rooms = list(search.get("rooms") or [])
        expect(
            any(room.get("room_id") == room_id and room.get("type") == "group" for room in rooms),
            f"{sender.label} mcp.rooms.search did not return shared group",
        )

        text = f"mcp agent group message {sender.label} {suffix}"
        sent = mcp(sender, "command", "mcp.messages.send", {"room_id": room_id, "msg": text})
        expect(sent.get("ok") is True and sent.get("event_id"), f"{sender.label} mcp.messages.send failed")
        from_ts = max(0, int(sent.get("ts") or 0) - 60_000)

        for viewer in nodes:
            wait_until(
                f"{viewer.label} mcp.messages.list did not see {sender.label} message",
                lambda v=viewer, t=text, f=from_ts: any(
                    message.get("msg") == t
                    for message in list(
                        mcp(v, "query", "mcp.messages.list", {"room_id": room_id, "from_ts": f, "limit": 100}).get("messages")
                        or []
                    )
                ),
            )


def assert_mcp_channel_tools(nodes: list[Node], suffix: int) -> None:
    for owner in nodes:
        channel = p2p(
            owner,
            "command",
            "channels.create",
            {
                "name": f"{owner.label} MCP Channel {suffix}",
                "visibility": "public",
                "join_policy": "open",
                "channel_type": "post",
                "comments_enabled": True,
            },
        )
        channel_id = channel.get("channel_id") or ""
        room_id = channel.get("room_id") or ""
        expect(bool(channel_id and room_id), f"{owner.label} channels.create for MCP did not return channel identifiers")

        post_body = f"mcp post {owner.label} {suffix}"
        post = p2p(
            owner,
            "command",
            "channels.posts.create",
            {"channel_id": channel_id, "room_id": room_id, "body": post_body, "message_type": "text"},
        )
        post_id = post.get("post_id") or ""
        expect(bool(post_id), f"{owner.label} channels.posts.create for MCP did not return post_id")

        posts = mcp(owner, "query", "mcp.channel_posts.list", {"room_id": room_id, "limit": 20})
        expect(
            any(item.get("post_id") == post_id and item.get("msg") == post_body for item in list(posts.get("posts") or [])),
            f"{owner.label} mcp.channel_posts.list did not return created post",
        )

        comment_body = f"mcp comment {owner.label} {suffix}"
        created = mcp(owner, "command", "mcp.channel_comments.create", {"post_id": post_id, "msg": comment_body})
        comment_id = created.get("comment_id") or ""
        expect(created.get("ok") is True and comment_id, f"{owner.label} mcp.channel_comments.create failed")

        comments = mcp(owner, "query", "mcp.channel_comments.list", {"post_id": post_id, "limit": 20})
        expect(
            any(item.get("comment_id") == comment_id and item.get("msg") == comment_body for item in list(comments.get("comments") or [])),
            f"{owner.label} mcp.channel_comments.list did not return created comment",
        )


def main() -> int:
    suffix = int(time.time() * 1000)
    nodes = [
        Node("A", "http://127.0.0.1:18008", "direxio-p2p-dual-dendrite-a-1", f"{PUBLIC_HOST}:18448", "", ""),
        Node("B", "http://127.0.0.1:28008", "direxio-p2p-dual-dendrite-b-1", f"{PUBLIC_HOST}:28448", "", ""),
        Node("C", "http://127.0.0.1:38008", "direxio-p2p-dual-dendrite-c-1", f"{PUBLIC_HOST}:38448", "", ""),
    ]
    for node in nodes:
        node.mxid = f"@owner:{node.server_name}"
        bootstrap = read_bootstrap(node.container)
        node.password = bootstrap.get("password") or ""
        node.agent_token = bootstrap.get("agent_token") or ""
        expect(bool(node.password), f"{node.label} bootstrap did not include password")
        expect(bool(node.agent_token), f"{node.label} bootstrap did not include agent_token")
        health = request_json("GET", f"{node.base}/_p2p/health")
        expect(health.get("status") == "ok", f"{node.label} health failed")
        login(node)
        update_profile(node, suffix)
        print(f"PASS {node.label} health/login/profile")

    for node in nodes:
        login(node)

    a, b, c = nodes

    direct_room = ensure_direct(a, b)
    verify_delete_readd(b, a, direct_room)
    status, _ = p2p_status(a, "command", "contacts.request", {"mxid": a.mxid, "display_name": a.name})
    expect(status == 400, "contacts.request allowed adding self")
    print("PASS direct accepted capabilities, delete/re-add old room, self-add guard")

    message_group = create_group(b, f"BCA Three Message {suffix}")
    invite_and_join(b, c, message_group)
    invite_and_join(b, a, message_group)
    assert_members(b, message_group, [a, b, c])
    group_text = f"three-node group message {suffix}"
    event_id = matrix_send_text(b, message_group, group_text)
    expect(bool(event_id), "Matrix group send did not return event_id")
    assert_message_projection([a, b, c], message_group, group_text)
    print("PASS group invite C-first/A-second, member nicknames, group preview capabilities")

    assert_mcp_group_tools([a, b, c], message_group, suffix)
    print("PASS MCP Agent token search/send/list across A/B/C shared group")

    a_first_group = create_group(b, f"BAC Three Order {suffix}")
    invite_and_join(b, a, a_first_group)
    invite_and_join(b, c, a_first_group)
    assert_members(b, a_first_group, [a, b, c])
    a_first_conv = wait_until("A conversations missing A-first group", lambda: conversation_for(a, a_first_group))
    c_second_conv = wait_until("C conversations missing C-second group", lambda: conversation_for(c, a_first_group))
    assert_caps(a_first_conv, send=True, send_media=True, call=True, label="A first-order group")
    assert_caps(c_second_conv, send=True, send_media=True, call=True, label="C second-order group")
    print("PASS group invite A-first/C-second, member nicknames, group capabilities")

    empty_group = create_group(b, f"BC Three Empty {suffix}")
    invite_and_join(b, c, empty_group)
    empty_conv = wait_until("C conversations missing joined empty group", lambda: conversation_for(c, empty_group))
    expect(not empty_conv.get("last_message"), "empty group unexpectedly has last_message")
    assert_caps(empty_conv, send=True, send_media=True, call=True, label="C empty group")
    c_group_rooms = {conv.get("matrix_room_id") for conv in conversations(c) if conv.get("kind") == "group"}
    expect({message_group, empty_group}.issubset(c_group_rooms), "C conversations did not retain multiple joined groups")
    print("PASS empty group and multi-group conversation list")

    channel = p2p(
        b,
        "command",
        "channels.create",
        {
            "name": f"B Three Channel {suffix}",
            "visibility": "public",
            "join_policy": "open",
            "channel_type": "post",
            "comments_enabled": True,
        },
    )
    channel_room = channel.get("room_id") or ""
    channel_conv = wait_until("B conversations missing channel", lambda: conversation_for(b, channel_room))
    assert_caps(
        channel_conv,
        send=True,
        send_media=True,
        call=False,
        label="B channel",
        extra={
            "post_create": True,
            "comment_create": True,
            "reaction_toggle": True,
            "comments_enabled": True,
        },
    )
    print("PASS channel capabilities call=false post/comment/reaction=true")

    assert_mcp_channel_tools([a, b, c], suffix)
    print("PASS MCP Agent token channel post/comment tools on A/B/C")

    run(["docker", "restart", c.container])
    wait_until("C did not become healthy after restart", lambda: request_json("GET", f"{c.base}/_p2p/health").get("status") == "ok")
    login(c)
    wait_until("C conversations missing message group after restart", lambda: conversation_for(c, message_group))
    wait_until("C conversations missing empty group after restart", lambda: conversation_for(c, empty_group))
    assert_history(c, message_group, group_text)
    print("PASS C restart persistence and Matrix history load")

    print("THREE_NODE_REGRESSION_PASS")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"THREE_NODE_REGRESSION_FAIL: {exc}", file=sys.stderr)
        raise
