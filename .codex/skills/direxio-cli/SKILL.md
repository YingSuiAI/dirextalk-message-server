---
name: direxio-cli
description: Use Direxio CLI to call an existing Direxio P2P/Matrix service through DIREXIO_DOMAIN and DIREXIO_AGENT_TOKEN. Use when a user wants to inspect portal status, list contacts/channels/groups, search or fetch public channels, send or read Matrix messages, run Matrix sync/listen workflows, or call supported P2P body actions through the raw action fallback.
---

# Direxio CLI

Use this skill to operate an already-running Direxio service through the first-party CLI.

Do not use this skill to start Docker stacks, deploy servers, run repository regression tests, or debug backend infrastructure. For those tasks, use the repository workflow or a deployment-specific skill instead.

## Setup

Before running commands, ensure both environment variables are set:

```bash
export DIREXIO_DOMAIN="https://example.com"
export DIREXIO_AGENT_TOKEN="<agent token>"
```

Use `DIREXIO_DOMAIN` as the service origin only. Do not include `/_p2p` or `/_matrix`.

The examples below use `direxio` as the command name. If the binary is installed as `direxio-cli`, substitute that executable name.

Start with:

```bash
direxio auth status
direxio p2p apis
```

Do not ask the user for Matrix access tokens or portal passwords for normal workflows. Matrix commands create the needed Matrix session internally through the Agent API and must not print Matrix access tokens.

## Use Domain Commands First

Prefer first-class commands when they exist:

```bash
direxio contacts list
direxio channels list
direxio channels public-search
direxio groups list
direxio matrix session init
direxio matrix messages send --room-id "!room:example.com" --text "hello"
direxio matrix messages list --room-id "!room:example.com" --limit 50
direxio matrix sync --timeout 30s
direxio matrix listen
```

Output should remain machine-readable. Use default pretty JSON for user inspection, and use raw/compact output only when scripting or when the user asks for it.

## Use Raw P2P Fallback

Use `direxio p2p action <action> --params '<json>'` only when no first-class command exists.

Read `references/p2p-actions.md` when you need the available action names, route hints, or action areas.

```bash
direxio p2p action profile.get --params '{}'
direxio p2p action groups.create --params '{"name":"Team"}'
direxio p2p action channels.public.get --params '{"room_id":"!room:owner.example.com","remote_node_base_url":"https://owner.example.com/_p2p"}'
```

Most read/query actions are safe to call directly. Ask for explicit user confirmation before delete, dissolve, remove, mute, unmute, redaction, recall, join approval/rejection, password rotation, permission changes, or other high-risk mutation actions.

## Troubleshooting

- If authentication fails, verify `DIREXIO_DOMAIN` has the correct scheme and host and that `DIREXIO_AGENT_TOKEN` is current.
- If a Matrix command fails, run `direxio matrix session init` once, then retry the Matrix operation.
- If a domain command is missing, inspect `direxio help`, then use the raw P2P fallback with an action from `references/p2p-actions.md`.
