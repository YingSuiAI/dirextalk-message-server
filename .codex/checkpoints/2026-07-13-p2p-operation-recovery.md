# P2P Operation Recovery Checkpoint

- Status: complete
- Updated: 2026-07-14 Asia/Shanghai
- Repository: `C:\Users\84960\Desktop\dirextalk\dirextalk-message-server`
- Delivery branch: `main`
- Recovery branch: `adam/p2p-operation-recovery`
- Released commit/tag: `30afd1f47cee880825d288232046769e0f1699f9` / `v1.0.3`
- HEAD before this uncommitted slice: `b8c5547`
- Base refactor branch: `adam/p2p-modular-refactor`
- Paused WS-refactor stash: `stash@{0}` / `1cc53d87589caf2a4f03f152f2ca6234ceedf408`; do not apply or drop during recovery/release work.
- Client owner: Codex task `019f5a87-921b-7a20-9b77-1c49cc360a51`, branch `codex/fix-join-approval-reconciliation`; this server task must not edit Flutter.

## Implemented Contract

- Optional owner-action `operation_id`, durable PostgreSQL/MemoryStore recovery records, cached success, restart replay, structured HTTP/WS recovery errors, and server-owned settlement context.
- Matrix membership `join` is final; group/contact/channel retries verify Matrix rather than trusting ProductCore state or an `already joined` string.
- Direct-room CreateRoom uses a hashed internal idempotency key and stable Dendrite room ID so an operation-state write failure/restart cannot duplicate the room.
- Contacts accept: accepted or joining + `matrix_join_unconfirmed`; no 200 join_failed. Reject returns current accepted/joining/rejected state. Old room + peer returns the authoritative replacement room.
- Channel decision/callback replays accept terminal and in-flight states, never kick on ordinary retry, retain joining on ambiguous ACK/network loss, preserve stable remote 4xx, and repair projection after a committed Matrix join.
- Same-process group/channel decisions use a bounded target workflow lock; public cross-node callbacks are deliberately excluded to avoid requester-owner-requester lock cycles.
- Stable codes and field levels are recorded in `docs/api-interface-change-record.md` and were sent to the client task.

## Critical Regression Coverage

- Group replay, lost `/send_join` response, canceled request settlement, conversation partial write/restart.
- Contact lost Join response, replacement old-room replay, Contact/conversation partial save, operation commit write failure, request cancellation, Matrix/projector repair, and stale reject room.
- Channel Invite timeout, callback ACK loss with restart, no kick/duplicate join, stable terminal 410, Matrix join plus projection failure/restart, terminal/in-flight replay, and concurrent approve/reject serialization.
- Dendrite idempotent CreateRoom room reuse.
- Channel join-request projection generation fencing: delayed/markerless prior
  state events cannot overwrite an active request (including `invite`), and a
  projection miss cannot race a newly persisted generation into an overwrite.
- Contact Matrix membership probes and JoinRoom dispatch now classify both
  wrapped and plain-text `deadline exceeded` as recoverable
  `joining`/`matrix_join_unconfirmed`; non-ambiguous policy failures retain
  their original error status.

## Verified Checks

- `go test ./p2p ./internal/productpolicy -count=1` — PASS, 78.933s / 0.038s after the final P1 recovery fixes.
- `go test ./internal/httputil ./setup -count=1` — PASS; `go test -tags=dendrite_upgrade_tests ./cmd/dendrite-upgrade-tests -count=1` — PASS (no test files under the existing tag boundary).
- `go test ./p2p/serviceapi ./internal/dirextalktransport/... -count=1` — PASS; the generated action contract artifact matches `ActionSpecs`.
- `bash scripts/release/contract_test.sh` — PASS (its printed negative-gate failures are intentional fixtures).
- `go build -o $TEMP/dirextalk-message-server-v1.0.3.exe ./cmd/dirextalk-message-server` — PASS.
- Changed Go files: `gofmt`, `gopls check`, `golangci-lint --new-from-rev=HEAD` (`unused`, `ineffassign`, `staticcheck`), `git diff --check`, and `docker compose -f docker-compose.p2p-dual.yml config -q` — PASS.
- A final P0/P1-only independent review found no P0/P1 after the generation fence and missing-first-member recovery fixes.
- Direct failure injection: stale generation A cannot overwrite initial generation B or emit Matrix Join/Invite; initial local group/channel joins establish only durable `joining`, and an unconfirmed Matrix join remains `joining` for reconciliation — PASS.
- `go test ./p2p/internal/action ./p2p/internal/contacts ./p2p/internal/members ./p2p/internal/projector ./p2p/internal/httpapi ./p2p/storage -count=1` — all passed after correcting the intentional additive Contact View contract; storage 42.906s.
- `go test ./federationapi -run '^TestFederationAPIJoinRetriesAfterLostSendJoinResponse$' -count=1` — PASS, 19.767s.
- `go test ./internal/dirextalktransport/... ./internal/productpolicy -count=1` — PASS.
- All focused fault-injection/concurrency/HTTP/WS tests listed in the task output passed.
- After the first real three-node run exposed a channel `approved` state being
  projected back to `invite`, the focused projection checks passed:
  `go test ./internal/dirextalkprojection -run '^(TestJoinRequestMemberApprovedDoesNotDowngradeActiveWorkflow|TestJoinRequestMemberFencesDelayedJoinRequestGeneration)$' -count=1`
  and `go test ./p2p -run '^TestProjectJoinRequestState(FencesDelayedGeneration|InsertDoesNotOverwriteConcurrentGeneration)$' -count=1`.
  The fix preserves active approval/join workflows, carries optional
  `request_id` in join-request Matrix state, fences every mismatched existing
  generation, and uses insert-if-absent/CAS rather than a read-then-upsert.
- The final clean-state Docker A/B/C run passed on 2026-07-14:
  `python scripts/p2p-three-node-regression.py` printed
  `THREE_NODE_REGRESSION_PASS`, including contact delete/re-add and mutual
  delete replacement-room acceptance, empty-state recovery, group/channel
  membership, channel backfill, MCP, restart persistence, and account deletion.
- The final independent P0/P1 review found no P0/P1 after the plain-text
  federation deadline fix. Its six focused contact accept regressions passed,
  covering probe and JoinRoom deadlines, lost responses, a normal join,
  federated retry, and non-ambiguous failure handling.
- CI run `29271110904` passed for `30afd1f`; the repository release contract,
  formal verification, and the exact-digest retained-data upgrade all passed.
- v1.0.3 was published with fixed and `latest` digest
  `sha256:c7d92c066c75d72e17313f9b46dc86536397a60ae785f0a3412331857cd79722`.
  The annotated tag resolves to `30afd1f`; the formal GitHub Release assets
  and all checksum files were independently verified, and the pulled image
  reports `v1.0.3`.

## Completion

- The server result and immutable release coordinates were sent to client task
  `019f5a87-921b-7a20-9b77-1c49cc360a51`; no Flutter files were modified here.
- Temporary Docker proxy, credential configuration, and release runtime helper
  files were removed after publication. The paused WS-refactor stash remains
  untouched.
