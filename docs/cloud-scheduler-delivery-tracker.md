# Cloud Scheduler Delivery Tracker

- Status: active
- Updated: 2026-07-15 Asia/Shanghai
- Owning repository: `dirextalk-message-server`
- Delivery branch: `adam/0714`
- Source contract: [Cloud Orchestrator MVP Contract](cloud-orchestrator-mvp-contract.md)

This is the authoritative checklist for the server-side Cloud Scheduler work.
Mark an item complete only after its focused verification has passed and its
task commit exists. A checked source-contract item does not mean that AWS
resources have been created or that a workload is production-ready.

## Scope lock

- [x] The Cloud Deployment Planner belongs to the server-side Eino Native
  Agent, not to a Codex workspace skill or a client-side skill.
- [x] The Message Server never imports the AWS SDK, runs AWS CLI, or holds
  long-lived AWS credentials.
- [x] Current delivery work must not edit, reuse, or commit
  `dirextalk-deployer`, release, updater, or deployment scripts.
- [x] A worker/root executor remains an isolated future execution boundary;
  it is not the existing deployer and is not the current `cloud-worker`
  `execution_probe` process.
- [ ] Move the built-in Cloud planner from its current inline prompt into the
  source-controlled Eino asset directory
  `p2p/nativeagent/skills/cloud_deployment_planner/SKILL.md`, loaded by
  the Native Agent runtime. This must remain distinct from user-managed
  `native_agent_skills_*` configuration and from all deployer scripts.

## Workboard

### 0. Historical deployer-artifact cleanup

- [x] Verified that the active Message Server diff has no deploy/release/updater
  script change and that `dirextalk-deployer` is currently clean.
- [x] Recorded the historical Cloud script provenance: current branch commits
  `d110b0f…9067057` in `dirextalk-deployer` and
  `02887e4`/`6675df5`/`d911624` in the Message Server were
  authored as `adam`. They predate this uncommitted slice.
- [ ] Classify each historical artifact before changing it: Eino prompt/content
  moves to `p2p/nativeagent/skills/`; a typed Connection Stack or Worker
  runtime, if retained, moves out of `dirextalk-deployer` into its own
  Cloud Orchestrator asset boundary; unused artifacts are removed in their
  owning repository.
- [ ] Clean historical commits only with a selected commit range and a safe
  method (normally a revert after migration). Do not rewrite shared history
  implicitly.

### 1. Eino Native Agent Cloud skill

- [x] Restricted `cloud_deployment_planner` is exposed only by the
  server-side Eino Native Agent and can create a credential-free research goal
  and read de-secretsed status.
- [x] Cloud dialogue mode excludes shell, runtime CLI, external MCP, managed
  skills, AWS credentials, approvals, lifecycle actions, and destruction.
- [ ] Package the built-in prompt and its capability contract in the dedicated
  Eino `skills/` source directory described above.
- [ ] Add the next typed planning/status capabilities only through narrow
  Native Agent ports; never grant the model direct AWS, root, secret,
  approval, ingress, start, stop, or destroy authority.

### 2. Durable Cloud control plane

- [x] Connection bootstrap, research, quote, Plan confirmation, and
  device-signed Plan approval are durable PostgreSQL control-plane contracts.
- [x] Sealed `RecipeExecutionManifestV1` and a separate
  `RecipeExecutionApprovalV1` contract exist; they do not execute a
  process or call AWS.
- [x] Finished the Recipe execution confirmation slice:
  private trusted-manifest registration, owner HTTP-only prepare/approve,
  install-intent persistence, migration, tests, focused build, review, and a
  single commit. It must still not issue a Worker task, run root commands, or
  mutate AWS.
- [ ] Add a real independently deployed Cloud Orchestrator consumer only after
  its artifact-delivery and executor contracts are reviewed.

### 3. Worker and single-VM executor

- [x] Existing `execution_probe` is recognized as a transport/lease proof,
  not service readiness or Recipe execution.
- [ ] Define a fixed Worker AMI, signed artifact delivery, root executor
  protocol, checkpoints, restart recovery, log/event redaction, and external
  health evidence.
- [ ] Add the typed `install` intent consumer only after the preceding
  executor boundary is complete and tested. It must validate live Worker
  evidence before acting.
- [ ] Validate one disposable-account, single-VM test service before OpenClaw,
  Hermes, knowledge-base, website, local-model, or training scenarios.

### 4. AWS connection and lifecycle

- [x] The intended AWS mutation boundary is a user-owned Connection Stack with
  a closed Broker command set; no current Message Server path mutates AWS.
- [ ] Finish/review Connection Stack artifact and credential bootstrap in its
  own owning repository and release process; do not couple it to this Eino
  skill or Message Server task.
- [ ] Implement typed create/observe/start/stop/destroy and read-back
  verification, including retained-tracked resources and idempotent recovery.
- [ ] Add secrets bootstrap, ingress-plan approval, cost estimate/alerts, and
  destruction approvals without leaking secret material into Agent, events, or
  MCP.

### 5. Product surfaces and acceptance

- [x] Public MCP remains Cloud read-only; Agent/MCP cannot approve spending,
  upload secrets, open ingress, run root execution, or destroy resources.
- [ ] Add Flutter connection/plan/workload/service views only after the
  server-side public contracts for each view are stable.
- [ ] Run disposable-account integration tests only after the Worker executor
  and Connection Stack are ready. No production AWS credential or server is a
  test fixture.
- [ ] Validate natural-language planning for OpenClaw, Hermes, a knowledge
  node, static site, local-model inference, and single-machine training as
  acceptance scenarios rather than hard-coded templates.

## Current next action

Package the Eino Cloud planner in its dedicated
`p2p/nativeagent/skills/` directory before expanding any cloud execution
capability. Do not modify deployment scripts for that action.
