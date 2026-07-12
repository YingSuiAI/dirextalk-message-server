# P2P Modular Refactor Checkpoint

- Status: in_progress
- Updated: 2026-07-12 Asia/Shanghai
- Repository: `C:\Users\84960\Desktop\dirextalk\dirextalk-message-server`
- Branch: `adam/p2p-modular-refactor`
- Base: `main` / `origin/main` at `a9cab7c1dba00caa43e24c3aa4b267b6c9de575d`
- Current published HEAD: `c1089cc2ab141d2d47546f25acd8c1fee636f23b`

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

- Commit and push the verified `conversation` module extraction, then migrate owner-local favorites/follows into `p2p/internal/social` before handling the higher-risk calls lifecycle separately.

## Completed Verification

- Mechanical cleanup removed confirmed unused private wrappers and redundant removed-action tests; Store/record contract tests and shared test support were consolidated without changing runtime contracts.
- `p2p/serviceapi.ActionSpecFor` now uses a duplicate-checked read-only index; generated action contract output remains byte-identical.
- Passed: `go test ./p2p/... -count=1` (root package 123.195s), `go build ./cmd/dirextalk-message-server`, focused/race serviceapi tests, focused record tests, `gopls check`, incremental unused/ineffassign/staticcheck/dupl/gocyclo lint, and `git diff --check`.
- Independent standards/spec review reported no actionable findings; its only residual risk was covered by the main task's complete P2P test result above.
- Published `ae9a868` (`refactor: clean p2p migration scaffolding`), `c957194` (`refactor: unify native agent config records`), and `af88654` (`refactor: share native model HTTP transport`) to draft PR #9.
- Native Agent skill/MCP config-record consolidation passed package and race tests while preserving exact errors and response shapes.
- Native Agent OpenAI-compatible/Anthropic direct HTTP and SSE consolidation passed focused provider behavior tests, full package race tests, unused/ineffassign/staticcheck/dupl/gocyclo lint, and `git diff --check`.
- Storage parity audit found pre-existing observable differences between the legacy in-memory maps and PostgreSQL (ordering, filtering, case handling, duplicate writes). The current structural slice will preserve the no-database behavior; cross-store tests cover only shared invariants, and parity changes remain a separate product decision.
- Published `c86f83b` (internal Action foundation), `3c94861` and `a402cd7` (shared in-memory and PostgreSQL channel keyset pagination), and `4ae4e41` (transport tests organized by business domain).
- Published `eac1993` with a thread-safe MemoryStore implementing all 71 root Store methods. It remains unavailable to production startup and is not yet wired into the no-database constructors.
- MemoryStore passed repeated package tests, race, gopls, unused/ineffassign/staticcheck/dupl/gocyclo lint, and an independent semantic audit. The audit's dynamic-value aliasing and raw-member compatibility findings were fixed before commit with typed-container/struct deep-copy tests.
- Constructor wiring characterization now locks canceled-context legacy writes and retention-pruned dedupe-key reuse. Additional required wiring gates are initial PortalState seeding, `store_mode=memory`, event Store-only dedupe/prune, and volatile account reset behavior.
- Published `c36daf4`, `b20ca91`, and `b323b24` to organize business-state tests, move pure DatabaseStore coverage into `p2p/storage`, and add atomic volatile account-state reset semantics.
- Published `c1089cc`: every legacy no-database Service now uses an independent MemoryStore, seeds normalized PortalState, preserves `store_mode=memory`, uses Store-only event sequencing/dedupe/prune, and removes direct test writes to legacy event/channel-content maps.
- Account deprovision now drains guarded ProductCore, MCP, Native Agent config, and projector work before PostgreSQL reset; terminal requests are rejected and queued roomserver projections are acknowledged without repopulating Store state. Controlled red/green race regressions cover the former map-reset-Store-write interleaving and Native Agent config writeback.
- Verified current wiring: `go test ./p2p/... -count=1` (latest root 100.071s), `go test -race ./p2p -count=1` (104.237s), focused account/MCP/Native Agent race tests, `go vet ./p2p/...`, touched-file `gopls check`, unused/ineffassign/staticcheck and incremental dupl/gocyclo lint, production build, Action contract artifact test, and `git diff --check`. Final independent review reported no blocker.
- The uncommitted conversation slice moves Store CRUD, `conversations.list/get`, hydration, capabilities, and operation construction into two cohesive `p2p/internal/conversation` production files (153 and 208 lines). The root adapter is 183 lines, the root map and 41-line type fragment are gone, and shared member-role/visibility plus Action error helpers now live in their owning foundations.
- Pure PostgreSQL conversation tests moved to `p2p/storage`; duplicate root hydration/capability tests were removed, reducing the remaining cross-domain operation integration file from 879 to 500 lines. Latest final-tree `go test ./p2p/... -count=1` passed (root 95.175s, storage 45.508s), along with module and focused root race tests, focused MCP/projector/operation tests, storage conversation tests, gopls/vet, unused/ineffassign/staticcheck and incremental dupl/gocyclo lint, production build, Action contract verification, `git diff --check`, and an independent engineering/contract review with no P0-P2 findings.

## Related Finding Outside This Structural Slice

- Production account deprovision truncates all PostgreSQL tables and delays process shutdown by two seconds. Non-P2P Matrix consumers are outside the Service operation barrier and may have a pre-existing post-truncate replay window; audit this as a separate account-deprovision hardening change rather than changing Matrix shutdown behavior inside the P2P structure refactor.
