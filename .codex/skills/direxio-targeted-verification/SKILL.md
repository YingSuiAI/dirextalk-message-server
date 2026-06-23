---
name: direxio-targeted-verification
description: Choose focused verification for Direxio Message Server repository changes. Use after modifying Go code, routes, contracts, docs, Postman JSON, Docker/compose files, scripts, CLI commands, storage migrations, event/state flows, setup/config, or project-local skills.
---

# Direxio Targeted Verification

Use this skill to validate enough for the touched surface without defaulting to an expensive full suite.

## Always Consider

- Run `gofmt -w` on touched Go files. Use `goimports` only if already installed.
- If `gopls` is installed and Go files changed, run `gopls check <touched-go-files>` as a quick semantic signal.
- Run `git diff --check`.
- Validate changed JSON with `python3 -m json.tool <file> >/dev/null`.
- Validate changed skills with:

```bash
python3 /mnt/c/Users/84960/.codex/skills/.system/skill-creator/scripts/quick_validate.py <skill-dir>
```

## Pick Checks by Surface

- Product actions, transport, projection, product policy, or product storage: `go test ./p2p ./internal/productpolicy -count=1`.
- Route auth, HTTP helpers, setup, monolith wiring, or config: `go test ./internal/httputil ./setup -count=1`.
- Client API behavior: focused `go test ./clientapi/... -run <TestName> -count=1`, then broaden only when shared routing/auth behavior changed.
- Roomserver, sync, user, federation, media, relay, or appservice behavior: test the owning package and direct consumer package touched by the impact map.
- CLI or agent client changes: `go test ./cmd/direxio-cli ./internal/agentclient -count=1`.
- Startup, build tags, command wiring, or broad package contracts: `go build ./cmd/direxio-message-server`.
- Storage migrations or SQL helpers: owning package storage tests plus `go test ./internal/sqlutil -count=1` when helper behavior changed.
- Postman collection: `python3 -m json.tool docs/postman/direxio-message-server.postman_collection.json >/dev/null`.
- Docker compose: `docker compose -f docker-compose.p2p.yml config` or `docker compose -f docker-compose.p2p-dual.yml config`.
- Multi-node remote lookup, federation, public join, profile/member propagation, message/redaction projection, or restart behavior across nodes:

```bash
export P2P_DUAL_PUBLIC_HOST="${P2P_DUAL_PUBLIC_HOST:-host.docker.internal}"
docker compose -f docker-compose.p2p-dual.yml up -d --force-recreate dendrite-a dendrite-b dendrite-c
python3 scripts/p2p-three-node-regression.py
```

## Lint

Run `golangci-lint run` only when installed and the change is broad enough to justify it. If unavailable, say so and rely on formatting, `gopls check` when available, targeted tests, build, JSON validation, compose validation, and `git diff --check`.

## Reporting

In the final response, list commands run and results. Also list important checks not run and why, especially Docker, multi-node regression, full `go test ./...`, and lint.
