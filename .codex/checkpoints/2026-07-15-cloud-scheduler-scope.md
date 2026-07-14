# Cloud Scheduler Scope Checkpoint

- Status: active
- Updated: 2026-07-15 Asia/Shanghai
- Repository: `C:\Users\84960\Desktop\dirextalk\dirextalk-message-server`
- Branch: `adam/0714`
- HEAD before the active uncommitted slice: `cf22ebe`
- Task tracker: [../../docs/cloud-scheduler-delivery-tracker.md](../../docs/cloud-scheduler-delivery-tracker.md)

## Scope corrected by the owner

- The Cloud Deployment Planner is a server-side Eino Native Agent capability.
  Its source-controlled built-in asset must live under
  `p2p/nativeagent/skills/cloud_deployment_planner/`, not in a Codex
  workspace skill and not in `dirextalk-deployer`.
- Do not edit, reuse, test, or commit deployer/release/updater scripts for this
  Message Server slice. No such script has been changed in the active diff.
- Maintain the linked task tracker; check items only after verification and
  commit.
- Historical provenance was checked after this scope decision: the current
  `dirextalk-deployer` worktree is clean. Its earlier Cloud Stack/Worker
  commits are `d110b0f…9067057` and the Message Server image additions
  are `02887e4`/`6675df5`/`d911624`, all recorded with Git
  author `adam`. Classify/migrate or safely revert them in their owning
  repository; do not rewrite shared history without a selected range.

## Active uncommitted slice

- Adds private trusted Recipe-execution manifest registration and owner-only
  prepare/approve actions in the Message Server.
- Approval creates only a queued `install` Job/Step plus a private
  execution-ID outbox record. There is no runner, Worker task issuance, root
  execution, AWS mutation, or service readiness path.
- The persistence boundary now rechecks a live active Worker lease before
  preparation and approval, so a stale registration cannot authorize a later
  install intent.

## Verified so far

- Focused storage confirmation test — PASS after adding stale Worker lease
  rejection.
- `go test ./p2p/serviceapi -count=1` — PASS after regenerating
  `docs/product-action-contract.json` and correcting an already-stale
  action-count assertion.
- `go test ./p2p ./p2p/internal/cloud ./p2p/internal/cloudorchestrator
  ./p2p/internal/cloudworker/... ./p2p/storage ./p2p/serviceapi -count=1`
  — PASS.
- `go build ./cmd/dirextalk-message-server` and
  `go build ./p2p/cmd/cloud-orchestrator` — PASS.
- `git diff --check` — PASS.

## Next action

Commit only the verified Message Server Recipe-confirmation files, then begin
the separate Eino-skill directory packaging stage. Preserve the unrelated
untracked `.run/Cloud Worker Tests.run.xml` unless its owner explicitly
asks to include it.
