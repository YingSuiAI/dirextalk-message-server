---
name: direxio-targeted-verification
description: Choose focused Direxio Message Server verification after modifying Go code, routes, contracts, docs, Postman JSON, Docker/compose files, scripts, storage migrations, event/state flows, setup/config, or project-local skills.
---

# Direxio Targeted Verification

Use this skill to select project-specific checks for the touched surface without defaulting to an expensive full suite.

Run commands from the repository root in the shell that matches the current environment. Use PowerShell syntax on Windows and Bash syntax on Linux, macOS, or WSL.

## Baseline Checks

- Go files changed: `gofmt -w <touched go files>` and, if installed, `gopls check <touched-go-files>`.
- Any change: `git diff --check`.
- Changed JSON: `python -m json.tool <file> > $null` on PowerShell, or `python3 -m json.tool <file> >/dev/null` on Bash.
- Changed project skills:
  - PowerShell: `python "$env:USERPROFILE\.codex\skills\.system\skill-creator\scripts\quick_validate.py" <skill-dir>`
  - Bash: `python3 "$HOME/.codex/skills/.system/skill-creator/scripts/quick_validate.py" <skill-dir>`

## Pick by Surface

- Product actions, transport, projection, product policy, or product storage: `go test ./p2p ./internal/productpolicy -count=1`.
- Route auth, HTTP helpers, setup, monolith wiring, or config: `go test ./internal/httputil ./setup -count=1`.
- Client API behavior: focused `go test ./clientapi/... -run <TestName> -count=1`, then broaden only when shared routing/auth behavior changed.
- Roomserver, sync, user, federation, media, relay, or appservice behavior: test the owning package and direct consumer package from the impact map.
- Startup, build tags, command wiring, or broad package contracts: `go build ./cmd/direxio-message-server`.
- Storage migrations or SQL helpers: owning package storage tests plus `go test ./internal/sqlutil -count=1` when helper behavior changed.
- Postman collection: validate `docs/postman/direxio-message-server.postman_collection.json` with `json.tool`.
- Docker compose: `docker compose -f docker-compose.p2p.yml config` or `docker compose -f docker-compose.p2p-dual.yml config`.
- Docs/skills/Postman-only changes: run skill validation, JSON validation if Postman changed, regression `rg` for removed/current-forbidden text, and `git diff --check`; skip Go tests unless contracts or code behavior changed.

## Multi-Node Regression

Run the three-node regression for changed remote lookup, federation, public join, profile/member propagation, message/redaction projection, or restart behavior across nodes.

PowerShell:

```powershell
$env:P2P_DUAL_PUBLIC_HOST = if ($env:P2P_DUAL_PUBLIC_HOST) { $env:P2P_DUAL_PUBLIC_HOST } else { "host.docker.internal" }
docker compose -f docker-compose.p2p-dual.yml up -d --force-recreate dendrite-a dendrite-b dendrite-c
python scripts/p2p-three-node-regression.py
```

Bash:

```bash
export P2P_DUAL_PUBLIC_HOST="${P2P_DUAL_PUBLIC_HOST:-host.docker.internal}"
docker compose -f docker-compose.p2p-dual.yml up -d --force-recreate dendrite-a dendrite-b dendrite-c
python3 scripts/p2p-three-node-regression.py
```

## Reporting

Report commands run and results. Also report important checks not run and why, especially Docker, multi-node regression, full `go test ./...`, and lint.
