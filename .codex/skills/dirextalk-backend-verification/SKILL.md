---
name: dirextalk-backend-verification
description: Use when selecting focused Dirextalk Message Server verification after Go, route, contract, docs, Postman, Docker, storage, Matrix state, setup, project-local skill, or script changes.
---

# Dirextalk Backend Verification

## Baseline

Run commands from the repository root in the current shell.

- Any change: `git diff --check`.
- Go files: `gofmt -w <files>` and `gopls check <files>` when installed.
- JSON: `python -m json.tool <file> > $null` on PowerShell or `python3 -m json.tool <file> >/dev/null` on Bash.
- Project skills: `python "$env:USERPROFILE\.codex\skills\.system\skill-creator\scripts\quick_validate.py" <skill-dir>` on PowerShell.
- Dependency, build-tag, or security-scan changes: `govulncheck ./...`; production entry point checks can additionally run `govulncheck ./cmd/dirextalk-message-server`.

## Pick By Surface

- Product actions, MCP transport, projection, product policy, or product storage: `go test ./p2p ./internal/dirextalkmcp ./internal/productpolicy -count=1`.
- Route auth, HTTP helpers, setup, monolith wiring, or config: `go test ./internal/httputil ./setup -count=1`.
- Startup, build tags, command wiring, or broad package contracts: `go build ./cmd/dirextalk-message-server`.
- Inherited Dendrite demo/upgrade tools stay out of default `./...` unless explicitly requested: Pinecone/Yggdrasil demos require `-tags=dendrite_p2p_demo`; upgrade tests require `-tags=dendrite_upgrade_tests`.
- Storage migrations or SQL helpers: owning package storage tests plus `go test ./internal/sqlutil -count=1` when helper behavior changed.
- Postman collection: validate `docs/postman/dirextalk-message-server.postman_collection.json`.
- Docker compose: `docker compose -f docker-compose.p2p.yml config` or `docker compose -f docker-compose.p2p-dual.yml config`.
- Docs/skills-only changes: validate changed skills and run `git diff --check`.

## Real Runtime Checks

Run the three-node regression for changed remote lookup, federation, public join, profile/member propagation, message/redaction projection, or restart behavior across nodes.

For UI-facing backend work, coordinate with Flutter Browser or multi-account smoke before claiming completion.

Report commands run and checks skipped with exact reasons.
