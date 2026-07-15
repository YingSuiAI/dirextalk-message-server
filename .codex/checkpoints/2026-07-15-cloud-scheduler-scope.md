# Cloud Scheduler Scope Checkpoint

- Status: `in_progress`
- Updated: 2026-07-15 Asia/Shanghai
- Repository: `C:\Users\84960\Desktop\dirextalk\dirextalk-message-server`
- Branch: `adam/0714`
- Task tracker: [../../docs/cloud-scheduler-delivery-tracker.md](../../docs/cloud-scheduler-delivery-tracker.md)

## Outcome and fixed boundaries

- The Eino Cloud Deployment Planner is a Message Server native Agent Skill at
  `p2p/nativeagent/skills/cloud_deployment_planner/`. It is not a Codex Skill,
  a public MCP mutation tool, or a deployer asset.
- The user AWS Connection Stack is a separate control-plane product boundary.
  Its old Node/SAM bundle has been removed from `dirextalk-deployer`; it must
  be rebuilt as an independent nested Go module at
  `cloud-orchestrator/connection-stack-v2/`.
- The Message Server root module must not import an AWS SDK, execute AWS CLI,
  or gain an npm/Node runtime dependency. The nested Go module may have its
  own Go Lambda dependencies and is never imported by the root server binary.
- The current port is fail closed: no credential bootstrap, EC2 creation,
  Worker root execution, ingress, lifecycle mutation, or real AWS test is
  enabled until the independently reviewed broker parity stage exists.
- Do not touch normal deployer, updater, or release scripts for the Go port.
  Preserve the unrelated Message Server `.run/Cloud Worker Tests.run.xml` and
  Flutter `pubspec.lock` worktree changes.

## Verified work

- `e4a8a6a feat(cloud): persist recipe execution confirmations` and
  `c5c61cc feat(nativeagent): package cloud planner skill` are committed in
  Message Server. Focused native-Agent tests and the Message Server build
  passed for the latter.
- `016c62b chore(deployer): remove connection stack bundle` is committed in
  `dirextalk-deployer`. It removes the historical Node/SAM Connection Stack,
  its 24 focused tests, and the sole test-suite registration while preserving
  normal deployer lifecycle behavior. Its focused distribution test and
  explicit Git-Bash `npm test` passed.
- Live worktree facts at resume: Message Server has only the active tracker
  edit plus the unrelated untracked run configuration; deployer is clean.
- No AWS credential, model token, or real cloud account was read, printed,
  persisted, or used in this cleanup/port stage.

## Current stage

Build the standalone Go Connection Stack foundation as one coherent boundary:

1. Port the closed signed-command and approval validation contract into the
   nested Go module, with durable contract tests independent of Node.
2. Add the Go Lambda Broker entry point and CloudFormation asset without
   importing it into the Message Server root module.
3. Keep unported resource-mutating operations explicitly rejected rather than
   silently claiming old Node feature parity.

## Verification and continuation

- Before the current stage, inspect the existing Cloud Orchestrator HTTPS
  client and the deleted Stack's historical protocol; no live AWS invocation
  is allowed.
- At stage close run the nested module's Go tests/build, the affected Message
  Server Cloud Orchestrator tests/build, `git diff --check`, and one
  accumulated contract review. Then update the task tracker and commit only
  current-task changes.
- Next concrete action: establish the Go module's protocol compatibility
  surface from the existing Go Broker client and historical Stack contract,
  then implement the Go-only Lambda boundary.
