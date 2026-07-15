# Cloud Cleanup, Agent + Client Delivery Tracker

- Status: scope frozen, active
- Scope frozen: 2026-07-15 Asia/Shanghai
- Owning repositories: `dirextalk-message-server`, `dirextalk-flutter`
- Delivery branch: `adam/0714`
- Server contract: [Cloud Orchestrator MVP Contract](cloud-orchestrator-mvp-contract.md)

This is the sole task tracker for the current Cloud Scheduler delivery slice.
An item is checked only after its focused verification and task commit exist.
Existing committed work is recorded as a baseline, not as permission to expand
the current scope.

## Audit conclusion

The previous tracker was not correct for the owner's current instruction: it
mixed historical Connection Stack/Worker artifacts into a deployer package and
then treated their cleanup as optional future work. They are an active
prerequisite: no new Agent or Flutter feature work starts until those artifacts
are either migrated out of the deployer or safely removed there.

The audited baseline shows that both required product halves already exist in
source:

- The Message Server has a source-controlled Eino Cloud Deployment Planner at
  `p2p/nativeagent/skills/cloud_deployment_planner/SKILL.md`, with only the
  credential-free `native_agent_cloud_deployment_plan` and read-only
  `native_agent_cloud_status` tools (`c5c61cc`).
- Flutter already has owner-scoped `cloud.*` adapters, realtime projection
  reduction, `/agent/workloads` task/service pages, plan/service details, and
  a client-selected active-Connection entry into the restricted Agent chat
  (`d8e859e`, `22fc0bd`, `1386a71`).
- The current Agent response is still ordinary model text plus generic tool
  trace. It does not yet provide a deterministic, client-safe workload card or
  plan deep link derived from the typed planning result. That is the first
  product gap after cleanup is complete.

## Delivery order

1. **Historical Cloud artifact cleanup (complete).** The audit found the
   Connection Stack has an independent Cloud Orchestrator consumer but does not
   belong in `dirextalk-deployer`. Commit `016c62b` removes its Node/SAM bundle,
   directly coupled tests, and package/test wiring without changing normal
   deployer lifecycle behavior.
2. **Standalone Go control-plane port (now).** Rebuild the retained closed
   Connection Stack contract as a nested, independent Go module. It must not
   add Node/npm to any Message Server or deployer runtime and must not be
   imported by the Message Server root module.
3. **Agent-to-client workload milestone (only after step 2).** Implement the
   scoped Eino Agent and Flutter contract below.

## Cleanup decision (audited 2026-07-15)

The Connection Stack V2 has an independent consumer: the separately supervised
`p2p/cmd/cloud-orchestrator` process and its closed HTTPS Broker client. It is
therefore retained as a product capability, but its existing Node/SAM bundle is
not migrated. The old implementation is removed from `dirextalk-deployer` and
is rebuilt as an independent Go module at:

```text
dirextalk-message-server/cloud-orchestrator/connection-stack-v2/
```

The new module owns a Go Lambda Broker, Go contract tests, its CloudFormation
template, and the Go custom-runtime `bootstrap` binary. It has its own `go.mod`
so the Message Server process neither imports the AWS SDK nor gains an AWS
runtime dependency. No JavaScript source, npm lockfile, Node runtime, or shell
deployment entrypoint is retained in the Message Server.

The Go rebuild starts with the previously documented signed outer-command
protocol and a syntactic `approval_proof` boundary plus a fail-closed Lambda.
It does **not** yet claim deterministic-CBOR `ApprovalV1` verification or an
approval consumption store. Until that complete parity slice is explicitly
implemented and reviewed, it must reject EC2 creation, Recipe execution,
lifecycle mutation, credential bootstrap, real AWS testing, and new IAM
capabilities. The old deployer bundle and its 24 tests are removed first,
together with only the Stack test registration in `tests/npm_test_suite.sh`.
The deployer's generic Windows Git-Bash test runner, package metadata, normal
lifecycle scripts, and unrelated release fixes stay in place. This is
implemented as new focused commits, never a broad history rewrite or a range
revert.

## Current Go port contract

The current implementation boundary is exactly
`cloud-orchestrator/connection-stack-v2/`:

- It is a nested Go module with its own `go.mod`, Go Lambda entry point,
  CloudFormation template, and Go tests. It is not imported by the root
  Message Server module and does not add an AWS SDK, Node, npm, AWS CLI, or
  shell deployment runtime to the server process.
- `POST /v2/commands` strictly parses the V2 outer command, canonical base64
  encoding, payload SHA-256, duplicate-free UTF-8 JSON, exact millisecond
  lifetime, exact registered
  Connection/PKIX-SPKI Ed25519 node key, and the existing non-deployment
  signature base. It returns only de-secretsed `{"error":{"code":"..."}}`
  responses with `Cache-Control: no-store`.
- Every action returns `operation_not_enabled`. In particular,
  `deployment.create` is rejected before any signature/proof/provider work:
  there is no ApprovalV1 verifier, receipt/counter store, EC2 API call,
  Worker endpoint, root execution, service readiness claim, or cloud resource
  side effect in this port stage.
- The CloudFormation execution role grants CloudWatch Logs writes only. It has
  no EC2, EBS, DynamoDB, IAM PassRole, Secrets Manager, S3, or network
  mutation permission. The Go artifact is supplied through a versioned S3
  artifact parameter by an approved external pipeline or the AWS console; no
  deploy helper is shipped here.

## Current delivery objective

After the Go control-plane boundary is verified, make the existing restricted
Cloud dialogue and Cloud Workloads UI operate as one coherent owner workflow:

1. The client selects an already active Cloud Connection before opening the
   restricted Cloud Agent conversation.
2. The Eino Agent may create a research-only Goal/Plan or read de-secretsed
   status through its existing narrow ports.
3. When a typed planning result creates or reuses a Plan, the server returns a
   deterministic, de-secretsed workload navigation summary; Flutter renders a
   milestone card that opens that Plan in `/agent/workloads/plans/:planId`.
4. The Workloads pages remain the source for plan, job, deployment, and service
   projections and use the existing revision-aware realtime reducer.

This is a planning and visibility slice. It must not imply that a VM was
created, billing started, a Recipe executed, or a service became ready.

## Fixed external contract for this slice

Both `agent.chat` and the terminal `agent.chat.stream` `done` event may carry
an optional field only when the restricted Cloud planning tool succeeds:

```text
cloud_workload = {
  schema: "dirextalk.cloud-agent-workload/v1",
  plan_id: string,
  goal_id: string,
  status: string,
  revision: integer
}
```

The server derives this object from the typed Cloud planner result, never by
parsing model prose. It contains no prompt, Cloud Connection id, provider
account/region data, quote, credential, secret reference, worker receipt, log,
endpoint, or command. The field is optional so existing non-Cloud Agent clients
remain compatible. Flutter treats an unknown or invalid object as absent and
uses `plan_id` only to open the existing Plan detail route.

## In scope

- The completed removal of the historical
  `dirextalk-deployer/scripts/connection-stack-v2/**` and its directly coupled
  tests/package wiring, recorded by commit `016c62b`.
- The destination standalone Go Connection Stack module at
  `cloud-orchestrator/connection-stack-v2/`; it is outside deployer/release/
  updater scripts, is not an Eino Skill, and is not a Message Server process
  dependency.
- `dirextalk-message-server/p2p/nativeagent/**` and the smallest adjacent
  Agent stream/response adapter needed for `cloud_workload`.
- `dirextalk-flutter/lib/presentation/agent/**`, existing Cloud Workloads
  provider/page/route code, and their data adapter only where required to
  consume and render the contract above.
- Focused server and Flutter tests plus this contract/tracker documentation.

## Explicitly out of scope

- `dirextalk-updater/**`, Docker publishing, normal Message Server deployment
  scripts, actual release execution, and unrelated historical Git cleanup.
- New Connection Stack/CloudFormation/Broker/Worker/AMI capabilities beyond
  the closed Go contract and fail-closed Lambda boundary; EC2 execution,
  real-account tests, image pushes, and credential-file access. The isolated
  Go Lambda module may use the AWS SDK when a later parity operation needs it,
  but the Message Server process must not.
- AWS key upload, secret bootstrap, purchase/approval, ingress, root command
  execution, stop/restart/destroy, cost enforcement, and service pairing.
- New Cloud lifecycle UI controls without a completed independent server-side
  control-plane contract.

The last two categories remain future work and are deliberately not represented
as new implementation tasks in this delivery slice.

## Workboard

### 0. Historical Connection Stack and Worker cleanup — Go port blocking

- [x] Produce a file/commit-level inventory of the Connection Stack V2,
  Worker-related code, tests, package exports, and deployer entrypoints.
- [x] Record that the Stack has an independent Cloud Orchestrator consumer and
  must move to a standalone Go boundary rather than stay in deployer.
- [x] Remove the Node/SAM bundle, its 24 directly coupled tests, and its suite
  registration from `dirextalk-deployer` (`016c62b`); the focused distribution
  test and explicit Git-Bash `npm test` passed.
- [x] Rebuild the retained signed outer Broker protocol as isolated Go code under
  `cloud-orchestrator/connection-stack-v2/`, with a separate `go.mod`, Go
  contract tests, and no dependency from the Message Server process. The
  initial port deliberately leaves action-specific payload/approval parity for
  a later capability stage.
- [x] Add the Go Lambda entry point and CloudFormation asset without a
  JavaScript, npm, or shell deployment runtime.
- [x] Verify the root Message Server module does not acquire AWS/Node
  dependencies and the Go port fails closed for unported mutations. Nested Go
  tests, `go vet`, Linux Lambda build, CloudFormation YAML parse, root Broker
  tests/build, and root module exclusion all pass.
- [x] Commit the Go port in the Message Server repository. Only then begin
  section B.

### A. Baseline retained for the later Agent/client stage

- [x] Package the server-side Eino Cloud planner as a dedicated native Skill;
  it remains distinct from user-managed skills and all deployment scripts
  (`c5c61cc`).
- [x] Retain the existing credential-free Cloud planning/status ports and
  their restricted Cloud dialogue mode; do not grant the Agent a cloud control
  capability.
- [x] Retain the existing Flutter Cloud Workloads routes, owner-only HTTP
  adapters, connection selection, and revision-aware realtime projection
  reducer as the client integration baseline.

### B. Deferred until cleanup: Agent-to-client workload milestone

- [ ] Add the optional `dirextalk.cloud-agent-workload/v1` summary to direct
  and streaming Agent results, derived only from a successful typed Cloud
  planning tool result.
- [ ] Prove that no secret-like input/output, model prose, malformed tool
  result, or non-Cloud tool call can create the summary.
- [ ] Render a Cloud workload milestone card in Flutter Agent chat, with a
  safe Plan-detail deep link and an honest non-ready status label.
- [ ] Keep unknown/absent summary behavior backward compatible and cover it in
  Flutter reducer/widget tests.

### C. Stage close

- [ ] Run the affected Message Server Native Agent tests and focused Flutter
  tests/analyzer; review the combined contract diff once.
- [ ] Commit only the current-task changes in each owning repository. Preserve
  unrelated work, including the Message Server's untracked Cloud Worker run
  configuration and Flutter's unrelated `pubspec.lock` change.

## Acceptance checks

- A restricted Cloud chat can create/reuse exactly one research-only Plan and
  returns a typed workload summary with that Plan id.
- A normal Agent chat, failed planning attempt, status-only response, or model
  text containing a forged id cannot create a workload card.
- Flutter opens the existing Plan page from the card; a missing or invalid
  summary leaves ordinary chat rendering unchanged.
- The UI and Agent never show or persist credentials, secrets, AWS account
  details, raw goals, Worker data, quotes, or lifecycle controls in this flow.
- The deployer contains no Connection Stack/Worker bundle or Cloud-specific npm
  runtime; the standalone Connection Stack contains Go code only and is absent
  from the root Message Server module dependency graph.
- A valid non-deployment signed command reaches only the Go fail-closed gate;
  malformed, expired, future-dated, oversized, duplicate-key, wrong-key, and
  query-bearing requests cannot reach any provider operation. A deployment
  command never reaches signature/proof/provider execution in this stage.

## Next action

Implement the deferred Agent-to-client workload milestone only after this Go
port commit is present; do not expand the Stack into a provider mutation path
without a separate approval/store/provider parity stage.
