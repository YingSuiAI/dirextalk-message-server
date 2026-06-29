#!/usr/bin/env python3
"""Run read-only PostgreSQL EXPLAIN plans for sync/history hot queries."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass


@dataclass(frozen=True)
class Target:
    room_id: str
    max_stream_id: int
    max_topological_position: int


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def connection_args(database_url: str | None) -> list[str]:
    if database_url:
        return [database_url]
    args: list[str] = []
    mapping = {
        "POSTGRES_HOST": "-h",
        "POSTGRES_PORT": "-p",
        "POSTGRES_USER": "-U",
        "POSTGRES_DB": "-d",
    }
    for env_name, flag in mapping.items():
        value = os.environ.get(env_name)
        if value:
            args.extend([flag, value])
    return args


def run_psql(database_url: str | None, sql: str) -> str:
    psql = shutil.which("psql")
    if not psql:
        raise SystemExit("psql is required. Install PostgreSQL client tools or add psql to PATH.")
    env = os.environ.copy()
    if "POSTGRES_PASSWORD" in env and "PGPASSWORD" not in env:
        env["PGPASSWORD"] = env["POSTGRES_PASSWORD"]
    cmd = [psql, *connection_args(database_url), "-X", "-v", "ON_ERROR_STOP=1", "-At", "-c", sql]
    result = subprocess.run(cmd, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        sys.stderr.write(result.stderr)
        raise SystemExit(result.returncode)
    return result.stdout.strip()


def discover_target(database_url: str | None, room_id: str | None) -> Target:
    if not room_id:
        room_id = run_psql(
            database_url,
            """
            SELECT room_id
            FROM syncapi_output_room_events
            GROUP BY room_id
            ORDER BY COUNT(*) DESC
            LIMIT 1
            """,
        )
    if not room_id:
        raise SystemExit("No syncapi_output_room_events rows found. Pass --room-id or run against a populated node.")

    max_stream = run_psql(
        database_url,
        f"SELECT COALESCE(MAX(id), 0) FROM syncapi_output_room_events WHERE room_id = {sql_literal(room_id)}",
    )
    max_topology = run_psql(
        database_url,
        "SELECT COALESCE(MAX(topological_position), 0) "
        "FROM syncapi_output_room_events_topology "
        f"WHERE room_id = {sql_literal(room_id)}",
    )
    return Target(room_id=room_id, max_stream_id=int(max_stream or "0"), max_topological_position=int(max_topology or "0"))


def explain(database_url: str | None, name: str, sql: str) -> None:
    print(f"\n-- {name}")
    print(run_psql(database_url, "EXPLAIN (ANALYZE, BUFFERS, VERBOSE) " + sql))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"), help="PostgreSQL connection URL")
    parser.add_argument("--room-id", help="Room to measure. Defaults to the room with the most sync events.")
    parser.add_argument("--limit", type=int, default=100, help="Pagination limit used in measured queries")
    args = parser.parse_args()

    target = discover_target(args.database_url, args.room_id)
    room = sql_literal(target.room_id)
    lower_stream = max(target.max_stream_id - 10000, 0)
    lower_topology = max(target.max_topological_position - 10000, 0)
    limit = max(args.limit, 1)

    print(f"-- room_id={target.room_id}")
    print(f"-- max_stream_id={target.max_stream_id}")
    print(f"-- max_topological_position={target.max_topological_position}")

    explain(
        args.database_url,
        "recent events for one sync room",
        f"""
        SELECT event_id, id, headered_event_json, session_id, exclude_from_sync, transaction_id, history_visibility
        FROM syncapi_output_room_events
        WHERE room_id = {room}
          AND exclude_from_sync = FALSE
          AND id > {lower_stream}
          AND id <= {target.max_stream_id}
        ORDER BY id DESC
        LIMIT {limit}
        """,
    )
    explain(
        args.database_url,
        "history context before event",
        f"""
        SELECT headered_event_json, history_visibility
        FROM syncapi_output_room_events
        WHERE room_id = {room}
          AND id < {target.max_stream_id}
        ORDER BY id DESC
        LIMIT {limit}
        """,
    )
    explain(
        args.database_url,
        "history context after event",
        f"""
        SELECT id, headered_event_json, history_visibility
        FROM syncapi_output_room_events
        WHERE room_id = {room}
          AND id > {lower_stream}
        ORDER BY id ASC
        LIMIT {limit}
        """,
    )
    explain(
        args.database_url,
        "topology back pagination",
        f"""
        SELECT event_id, topological_position, stream_position
        FROM syncapi_output_room_events_topology
        WHERE room_id = {room}
          AND (
            (topological_position > {lower_topology} AND topological_position < {target.max_topological_position})
            OR (topological_position = {target.max_topological_position} AND stream_position <= {target.max_stream_id})
          )
        ORDER BY topological_position DESC, stream_position DESC
        LIMIT {limit}
        """,
    )
    explain(
        args.database_url,
        "stream to topology descending",
        f"""
        SELECT topological_position
        FROM syncapi_output_room_events_topology
        WHERE room_id = {room}
          AND stream_position <= {target.max_stream_id}
        ORDER BY topological_position DESC
        LIMIT 1
        """,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
