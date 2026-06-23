# Direxio CLI Recipe for Codex

Use `direxio-cli` when the user asks to inspect or operate a Direxio P2P node.

Required environment:

```bash
export DIREXIO_DOMAIN="https://example.com"
export DIREXIO_AGENT_TOKEN="<agent token>"
```

Start by checking:

```bash
direxio auth status
direxio p2p apis
```

Common workflows:

```bash
direxio contacts list
direxio channels list
direxio groups list
direxio matrix session init
direxio matrix messages send --room-id "!room:example.com" --text "hello"
direxio matrix messages list --room-id "!room:example.com" --limit 50
direxio matrix sync --timeout-ms 30000
```

Use `direxio p2p action <action> --params '{}'` only when no domain command exists. Ask the user before delete, dissolve, remove, mute, redaction, approval, or other high-risk mutation actions.
