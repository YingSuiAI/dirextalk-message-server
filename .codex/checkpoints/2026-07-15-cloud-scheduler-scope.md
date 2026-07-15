# Cloud Scheduler Scope Checkpoint

- Status: `in_progress`
- Updated: 2026-07-15 Asia/Shanghai
- Repository: `C:\Users\84960\Desktop\dirextalk\dirextalk-message-server`
- Branch: `adam/0714`
- Task tracker: [../../docs/cloud-scheduler-delivery-tracker.md](../../docs/cloud-scheduler-delivery-tracker.md)

## Fixed boundaries

- The Eino Cloud Deployment Planner remains a Message Server native Agent
  Skill at `p2p/nativeagent/skills/cloud_deployment_planner/`.
- AWS mutations remain behind the standalone typed Go Connection Stack. The
  Message Server does not import an AWS SDK, run AWS CLI, or require Node/npm.
- Agent and public MCP surfaces cannot approve spending, upload secrets, open
  ingress, restore, destroy, or invoke arbitrary AWS APIs.
- Do not change normal deployer, updater, or release scripts for this feature.
- Preserve the unrelated untracked `.run/Cloud Worker Tests.run.xml` file.

## Last completed stage

Stage M (original-instance retained-volume restore) is complete in the current
worktree. It adds the device-bound plan/approval contract, durable Orchestrator
command ledger, default-off typed Connection Stack mutation with compensation,
semantic readiness verification, retained-volume visibility, lifecycle
exclusion, and the Flutter approval/progress flow. Focused server, Stack and
Flutter tests, server builds, Flutter analyze, and the Web release build pass.

Flutter device-signed deployment intent is complete:

- Flutter `92b5698` renders QuoteV1, verifies the fixed first-deployment scope,
  matches the Go deterministic-CBOR golden and resumes the exact signed
  approval/idempotency keys after response loss.
- Server docs `23ad570` record that the current provision consumer is disabled;
  the UI therefore says approval queues an intent and does not create or bill.
- Focused Flutter tests, analyze and Web release build passed. The Web build
  found and fixed a dart2js unsafe-int boundary by failing closed above
  Number.MAX_SAFE_INTEGER.

Stage L (retained service backup) is complete in commit `3c37f30`:

- Device-approved `cloud.services.operation.plan/approve` supports `backup`.
- Approval binds exact Service/Deployment revisions, connection, recipe,
  instance, complete EBS volume set, quote scope, and manual retention.
- ProductCore persists Approval, backup ledger, Job/Step, and private outbox
  atomically; the Orchestrator persists the signed `service.backup` command
  before I/O and safely replays it after response loss.
- The Connection Stack defaults backup off, consumes approvals once, creates
  a deterministic retained AMI with encrypted snapshots, and verifies the
  AWS terminal state by read-back.
- Backup success or failure does not change Service/Resource lifecycle axes;
  retained backups are listed through Service list/get and are not deleted by
  service destruction.

Verified at stage close:

- Standalone Stack: `go test ./... -count=1`, `go vet ./...`, and Linux amd64
  Broker build passed.
- Root affected packages: focused `go test`, `go vet`, and Linux amd64
  Cloud Orchestrator build passed.
- Post-review ServiceBackup storage, runtime, and cloud boundary tests passed.
- `git diff --check`, dependency-boundary checks, and added-diff secret scan
  passed. No real AWS request or cloud spend occurred.

The owner visibility closeout is also complete:

- Message Server commit `5e86dd3` admits the existing `backup` Job projection,
  advances the enclosing Service revision at a backup terminal state, and
  publishes a strictly validated full Service summary containing only manual
  retained AMI/snapshot evidence. Service status and resource axes remain
  unchanged.
- Flutter commit `b06a35e` decodes and displays retained backups, manual
  retention, AMI and encrypted snapshot count without adding restore/delete
  controls.
- Server Cloud projection tests, focused ServiceBackup PostgreSQL tests, Go
  vet, Flutter Cloud workload tests, Flutter analyze and both repositories'
  `git diff --check` passed. The intentionally broad `storepg` package was not
  used as the stage signal because its roughly thirty per-test database
  migrations exceed the outer 120-second command timeout; focused affected
  tests passed in about twelve seconds.

## Current decision boundary

The next tracker stage is **experimental-to-managed acceptance**. It must not
change maturity until the locked artifact, restart recovery, probes,
volume/secret slots, upgrade/rollback, backup/restore, and destroy contracts
have all been verified and accepted with a new device-bound approval.

## Continuation

- Keep the overall Cloud Scheduler goal active after committing Stage M.
- Implement experimental-to-managed acceptance as one independent stage.
- Keep both restore mutation gates disabled and do not run real AWS tests until
  their separately authorized provider/deployment stage.
- Do not use `rootkey.csv`, the supplied model token, or real AWS until the
  chosen mutation contract and its safety gates pass the fake/provider stage.
