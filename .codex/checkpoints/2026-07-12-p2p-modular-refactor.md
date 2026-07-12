# P2P Modular Refactor Checkpoint

- Status: in_progress
- Updated: 2026-07-12 Asia/Shanghai
- Repository: `C:\Users\84960\Desktop\dirextalk\dirextalk-message-server`
- Branch: `adam/p2p-modular-refactor`
- Base: `main` / `origin/main` at `a9cab7c1dba00caa43e24c3aa4b267b6c9de575d`

## Outcome And Boundaries

- Reorganize `p2p` into cohesive private business modules without changing runtime ProductCore, Matrix, MCP, WS, storage, or JSON behavior.
- Keep all 146 `p2p/serviceapi.ActionSpecs`, public `plugins.*` actions, root `p2p` setup-facing APIs, PostgreSQL schema, and migrations compatible.
- Do not introduce Gin, Echo, or Chi; keep Gorilla at the monolith routing boundary.
- Treat repository-unused compatibility subpackages as removable Go-internal facades after their in-repo callers are migrated.

## Verified Baseline

- The former `codex/graphify-evaluation-20260711` branch, local `main`, and `origin/main` were identical; the merge and push were no-ops.
- `docs/product-action-contract.json` has schema version 1, 146 actions, and SHA-256 `5387558B3378108A6F9594962A8F6DACA61AC1A8DF8561F3F85A5756552F51B9`.
- Relevant contract sources: `docs/agent-mcp-current-contract.md`, `docs/current-project-documentation.md`, and `.codex/skills/dirextalk-backend-contract-state-storage/SKILL.md`.

## Ordered Work

1. Remove confirmed dead private code and consolidate redundant contract tests without behavior changes.
2. Add indexed action metadata lookup and exact handler coverage.
3. Introduce a tested memory Store implementation and remove split Store/map execution paths.
4. Extract cohesive modules under `p2p/internal` in dependency order.
5. Consolidate HTTP/WS/MCP adapters and confirmed duplicate implementations.
6. Run contract, PostgreSQL/restart, projection/multi-node, lint, build, and independent standards/spec review gates.

## Current Next Action

- Finish and independently audit the Action foundation and MemoryStore implementation, then wire the legacy no-database constructors through MemoryStore without changing their observable semantics.

## Completed Verification

- Mechanical cleanup removed confirmed unused private wrappers and redundant removed-action tests; Store/record contract tests and shared test support were consolidated without changing runtime contracts.
- `p2p/serviceapi.ActionSpecFor` now uses a duplicate-checked read-only index; generated action contract output remains byte-identical.
- Passed: `go test ./p2p/... -count=1` (root package 123.195s), `go build ./cmd/dirextalk-message-server`, focused/race serviceapi tests, focused record tests, `gopls check`, incremental unused/ineffassign/staticcheck/dupl/gocyclo lint, and `git diff --check`.
- Independent standards/spec review reported no actionable findings; its only residual risk was covered by the main task's complete P2P test result above.
- Published `ae9a868` (`refactor: clean p2p migration scaffolding`), `c957194` (`refactor: unify native agent config records`), and `af88654` (`refactor: share native model HTTP transport`) to draft PR #9.
- Native Agent skill/MCP config-record consolidation passed package and race tests while preserving exact errors and response shapes.
- Native Agent OpenAI-compatible/Anthropic direct HTTP and SSE consolidation passed focused provider behavior tests, full package race tests, unused/ineffassign/staticcheck/dupl/gocyclo lint, and `git diff --check`.
- Storage parity audit found pre-existing observable differences between the legacy in-memory maps and PostgreSQL (ordering, filtering, case handling, duplicate writes). The current structural slice will preserve the no-database behavior; cross-store tests cover only shared invariants, and parity changes remain a separate product decision.
