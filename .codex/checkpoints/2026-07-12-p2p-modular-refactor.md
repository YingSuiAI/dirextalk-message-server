# P2P Modular Refactor Checkpoint

- Status: in_progress
- Updated: 2026-07-12 Asia/Shanghai
- Repository: `C:\Users\84960\Desktop\dirextalk\dirextalk-message-server`
- Branch: `adam/p2p-modular-refactor`
- Base: `main` / `origin/main` at `a9cab7c1dba00caa43e24c3aa4b267b6c9de575d`
- Current published HEAD: `a96db443ffdffa29acab6bcb2a7f1f87ee53868a`

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

- Commit and push the verified calls lifecycle module, then migrate contacts/blocks while keeping shared room/member lifecycle separate.

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
- Published `eac5102`: conversation Store CRUD, `conversations.list/get`, hydration, capabilities, and operation construction now live in two cohesive `p2p/internal/conversation` production files; the root adapter is 183 lines, the root map is gone, and pure PostgreSQL conversation tests moved to `p2p/storage`.
- The verified social slice moves all seven owner-local favorites/follows actions into `p2p/internal/social`, removes both root shadow maps, routes MCP favorite summaries through the module, replaces the source-scanning MCP test with a behavior test, and keeps calls separate in `service_calls.go`. A controlled red/green regression fixed restart/clock-rollback favorite ID collisions before PostgreSQL upsert could overwrite an existing row.
- Latest social gates passed: `go test ./p2p/... -count=1` (root 94.896s, storage 45.330s), module and focused root race tests, related `internal/productpolicy`/`internal/httputil`/`setup` tests, gopls/vet, unused/ineffassign/staticcheck and module dupl/gocyclo lint, production build, byte-identical Action contract generation, `git diff --check`, and independent engineering/contract review with no remaining P0-P2 findings.
- Published `a96db44`: all seven favorites/follows actions now live in `p2p/internal/social`; root shadow maps and Store-error fallback paths are gone, and the restart/clock-rollback ID collision regression is covered.
- The verified calls slice moves all six call actions and lifecycle/time parsing into two cohesive `p2p/internal/calls` production files, removes the 219-line root implementation and calls shadow map, and centralizes terminal-state normalization in `internal/dirextalkdomain` for the module and MemoryStore. A mutation lock now serializes single-process read/transition/upsert/publish so a concurrent connected event cannot reopen a terminal call; deterministic regression and race tests cover the former lost-update window.
- Latest calls gates passed: module repeated/race tests, focused root call/account-delete/registry/WS tests and race, domain/MemoryStore tests, `go test ./p2p/... -count=1`, gopls/vet, unused/ineffassign/staticcheck and module dupl/gocyclo lint, production build, byte-identical Action contract generation, `git diff --check`, and an independent engineering/contract review with no P0-P2 findings.

## Related Finding Outside This Structural Slice

- Production account deprovision truncates all PostgreSQL tables and delays process shutdown by two seconds. Non-P2P Matrix consumers are outside the Service operation barrier and may have a pre-existing post-truncate replay window; audit this as a separate account-deprovision hardening change rather than changing Matrix shutdown behavior inside the P2P structure refactor.
- Call persistence and `call.changed` event insertion are not one transaction. If event insertion fails after a terminal call write, retry may return the terminal row without restoring the missing event; address with a durable outbox/transactional boundary in a separate behavior change.
- PostgreSQL active-call filtering matches only exact lowercase terminal states, while MemoryStore normalizes whitespace and case. Current writers emit lowercase states, but cross-store normalization and empty-list ordering/JSON parity remain separate compatibility decisions.
