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

The next tracker item is a separately quoted and device-approved restore or
rollback operation. A naive "launch a recovered clone from the AMI" is unsafe:
the image contains the old Worker/service identity, checkpoints, and possibly
integration credentials, so the clone may create a double-active identity
before bootstrap rotation.

Choose one Stage M contract before implementation:

1. **In-place retained-volume rollback (recommended):** create encrypted EBS
   volumes from the approved snapshots, stop the current instance, replace the
   attached volumes on their exact device mappings, restart and verify, while
   retaining the previous volumes for a separately approved rollback/destroy.
   This has explicit downtime and EBS charges but avoids duplicate identity.
2. **Isolated recovered clone:** launch a second instance from the retained AMI
   while retaining the original. This first requires pre-network boot fencing
   plus Worker/service identity and secret rotation before any service starts.
3. **Materialize only:** create encrypted restored EBS volumes and leave them
   unattached. This avoids downtime and double-active identity, but a separate
   approved cutover is required before it becomes a service restore.

## Continuation

- Keep the goal active; this is a product decision pause, not a technical
  blocker or completed goal.
- After the user selects 1, 2, or 3, freeze only that external contract and
  implement its quote, approval, durable command, AWS read-back, recovery,
  focused tests, one stage-end review, tracker update, and commit.
- Do not use `rootkey.csv`, the supplied model token, or real AWS until the
  chosen mutation contract and its safety gates pass the fake/provider stage.
