#!/usr/bin/env python3
import argparse
import json
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable


class ApiError(Exception):
    def __init__(self, status: int, body: Any, message: str):
        super().__init__(message)
        self.status = status
        self.body = body


@dataclass
class Metric:
    name: str
    samples_ms: list[float] = field(default_factory=list)

    def add(self, elapsed_ms: float) -> None:
        self.samples_ms.append(elapsed_ms)

    def summary(self) -> dict[str, Any]:
        samples = sorted(self.samples_ms)
        if not samples:
            return {"count": 0}
        return {
            "count": len(samples),
            "min_ms": round(samples[0], 2),
            "p50_ms": round(percentile(samples, 50), 2),
            "p95_ms": round(percentile(samples, 95), 2),
            "max_ms": round(samples[-1], 2),
            "avg_ms": round(statistics.fmean(samples), 2),
        }


def percentile(samples: list[float], pct: int) -> float:
    if len(samples) == 1:
        return samples[0]
    rank = (len(samples) - 1) * (pct / 100)
    low = int(rank)
    high = min(low + 1, len(samples) - 1)
    weight = rank - low
    return samples[low] * (1 - weight) + samples[high] * weight


def request_json(method: str, url: str, body: Any = None, token: str = "", timeout: float = 20) -> Any:
    data = None if body is None else json.dumps(body).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as res:
            raw = res.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            parsed = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            parsed = raw
        raise ApiError(exc.code, parsed, f"{method} {url} failed with {exc.code}") from exc


def p2p(base_url: str, kind: str, action: str, params: dict[str, Any], token: str) -> Any:
    return request_json(
        "POST",
        f"{base_url.rstrip('/')}/_p2p/{kind}",
        {"action": action, "params": params},
        token,
    )


def matrix_get(base_url: str, path: str, token: str) -> Any:
    return request_json("GET", f"{base_url.rstrip('/')}{path}", token=token)


def measure(metrics: dict[str, Metric], name: str, fn: Callable[[], Any]) -> Any:
    started = time.perf_counter()
    try:
        return fn()
    finally:
        elapsed_ms = (time.perf_counter() - started) * 1000
        metrics.setdefault(name, Metric(name)).add(elapsed_ms)


def login(base_url: str, password: str) -> str:
    res = p2p(base_url, "query", "portal.auth", {"password": password}, "")
    token = str(res.get("access_token") or "")
    if not token:
        raise RuntimeError("portal.auth did not return access_token")
    return token


def run(args: argparse.Namespace) -> dict[str, Any]:
    token = args.access_token or ""
    if not token:
        if not args.password:
            raise RuntimeError("pass --access-token or --password")
        token = login(args.base_url, args.password)

    metrics: dict[str, Metric] = {}
    prefix = args.prefix or f"capacity_{int(time.time())}"
    created_groups: list[str] = []
    created_channels: list[str] = []

    for index in range(args.groups):
        group = measure(
            metrics,
            "groups.create",
            lambda index=index: p2p(
                args.base_url,
                "command",
                "groups.create",
                {"name": f"{prefix} group {index:04d}"},
                token,
            ),
        )
        room_id = str(group.get("room_id") or "")
        if room_id:
            created_groups.append(room_id)

    for index in range(args.channels):
        channel = measure(
            metrics,
            "channels.create",
            lambda index=index: p2p(
                args.base_url,
                "command",
                "channels.create",
                {
                    "channel_id": f"{prefix}_channel_{index:04d}",
                    "name": f"{prefix} channel {index:04d}",
                    "visibility": "public" if index % 2 == 0 else "private",
                    "join_policy": "open",
                    "channel_type": "post" if args.posts_per_channel > 0 else "chat",
                },
                token,
            ),
        )
        channel_id = str(channel.get("channel_id") or "")
        if channel_id:
            created_channels.append(channel_id)

    for channel_id in created_channels:
        for index in range(args.posts_per_channel):
            measure(
                metrics,
                "channels.posts.create",
                lambda channel_id=channel_id, index=index: p2p(
                    args.base_url,
                    "command",
                    "channels.posts.create",
                    {"channel_id": channel_id, "body": f"{prefix} post {index:04d}"},
                    token,
                ),
            )

    bootstrap = measure(metrics, "sync.bootstrap", lambda: p2p(args.base_url, "query", "sync.bootstrap", {}, token))
    groups_list = measure(metrics, "groups.list", lambda: p2p(args.base_url, "query", "groups.list", {}, token))
    channels_list = measure(metrics, "channels.list", lambda: p2p(args.base_url, "query", "channels.list", {}, token))
    public_search = measure(
        metrics,
        "channels.public.search",
        lambda: p2p(args.base_url, "query", "channels.public.search", {"q": prefix, "limit": min(args.channels, 100)}, token),
    )

    sync_result: dict[str, Any] | None = None
    if args.matrix_sync:
        sync_path = "/_matrix/client/v3/sync?timeout=0&filter=" + urllib.parse.quote(
            json.dumps({"room": {"timeline": {"limit": args.sync_timeline_limit}, "state": {"lazy_load_members": True}}}),
            safe="",
        )
        sync_result = measure(metrics, "matrix.sync", lambda: matrix_get(args.base_url, sync_path, token))

    return {
        "base_url": args.base_url,
        "prefix": prefix,
        "created": {
            "groups": len(created_groups),
            "channels": len(created_channels),
            "posts": len(created_channels) * args.posts_per_channel,
        },
        "response_sizes": {
            "bootstrap_groups": len(bootstrap.get("groups") or []),
            "bootstrap_channels": len(bootstrap.get("channels") or []),
            "groups_list": len(groups_list.get("groups") or []),
            "channels_list": len(channels_list.get("channels") or []),
            "public_search": len(public_search.get("channels") or public_search.get("results") or []),
            "matrix_sync_rooms": len(((sync_result or {}).get("rooms") or {}).get("join") or {}),
        },
        "metrics": {name: metric.summary() for name, metric in sorted(metrics.items())},
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a Dirextalk P2P capacity smoke against one node.")
    parser.add_argument("--base-url", required=True, help="Node base URL, for example http://localhost:8008")
    parser.add_argument("--access-token", default="", help="Portal owner access token")
    parser.add_argument("--password", default="", help="Portal owner password; used only when access token is omitted")
    parser.add_argument("--prefix", default="", help="Stable name prefix. Defaults to capacity_<unix_time>.")
    parser.add_argument("--groups", type=int, default=100)
    parser.add_argument("--channels", type=int, default=100)
    parser.add_argument("--posts-per-channel", type=int, default=0)
    parser.add_argument("--matrix-sync", action="store_true", help="Also measure one Matrix /sync request.")
    parser.add_argument("--sync-timeline-limit", type=int, default=10)
    return parser.parse_args()


def main() -> int:
    try:
        result = run(parse_args())
    except Exception as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
