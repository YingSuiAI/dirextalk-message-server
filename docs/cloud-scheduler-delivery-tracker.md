# Cloud Cleanup, Agent + Client Delivery Tracker

- Status: Worker bootstrap/observation parity complete; execution task channel next
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
- `connection.registration.verify` and `quote.request` are enabled after node
  authentication and the generation fence. They use an atomic DynamoDB
  receipt/counter/issued-quote transaction; quote reads are limited to EC2
  instance metadata/offerings and AWS Price List. All stored results are
  strict, de-secretsed contract objects and are revalidated before replay.
- `deployment.create` and the Worker claim route are complete but disabled by default. Their explicit gate
  verifies the registered device signature and persisted QuoteV1 digest,
  atomically consumes approval/challenge into a deployment reservation, uses
  one deterministic ClientToken for a fixed isolated EC2 create, and commits
  only read-back EC2/EBS/ENI evidence. The fixed claim route verifies AWS IID
  signatures and independent EC2 state before rotating a short lease; the
  signed `deployment.observe` read returns only de-secreted active evidence.
  The same gate admits only the fixed digest-bound `execution_probe` task and
  its de-secreted heartbeat/checkpoint events. Recipe/root/readiness/lifecycle
  actions remain `operation_not_enabled`.
- The CloudFormation execution role always grants its own log/receipt writes
  and the bounded quote read APIs. RunInstances/create-time tagging/read-back
  statements and exact Worker session/task-table access exist only behind the
  same explicit gate. It has no IAM PassRole, Secrets Manager, S3 write,
  ingress, root-execution, or lifecycle permission. The Go
  artifact is supplied through a versioned S3
  artifact parameter by an approved external pipeline or the AWS console; no
  deploy helper is shipped here.

## Completed Agent/client delivery objective

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

## Completed delivery objective — Go read-only Broker parity

Make the independently deployed Go Connection Stack compatible with the
already committed Orchestrator registration and quote clients without enabling
any billable or mutating AWS operation:

1. Align CloudFormation parameter names, the explicit `prod` stage, Stack
   runtime identity, and Broker URL with the existing Role Plan and
   registration endpoint contract.
2. Persist exact command receipts and the last accepted per-Connection node
   counter atomically. An exact replay returns the original result as
   `idempotent`; the same command id with a different signed identity and a
   non-increasing new counter fail closed.
3. Enable only `connection.registration.verify` and `quote.request`. The former
   attests immutable Stack/Worker configuration; the latter may call only EC2
   instance metadata/offerings and AWS Price List read APIs.
4. Keep `deployment.create`, Worker routes, secret bootstrap, approval
   consumption, ingress and every provider mutation at `operation_not_enabled`.

This stage uses fake AWS/provider tests and DynamoDB request-contract tests. It
does not access `rootkey.csv`, deploy the Stack, call a real AWS account, create
an EC2 instance, or start billing.

## In scope

- The completed removal of the historical
  `dirextalk-deployer/scripts/connection-stack-v2/**` and its directly coupled
  tests/package wiring, recorded by commit `016c62b`.
- The destination standalone Go Connection Stack module at
  `cloud-orchestrator/connection-stack-v2/`; it is outside deployer/release/
  updater scripts, is not an Eino Skill, and is not a Message Server process
  dependency.
- The standalone module's DynamoDB receipt/counter/issued-quote store,
  registration attestation, read-only EC2/Pricing quote provider,
  CloudFormation resources/IAM, and cross-module contract fixtures.
- `dirextalk-message-server/p2p/nativeagent/**` and the smallest adjacent
  Agent stream/response adapter needed for `cloud_workload`.
- `dirextalk-flutter/lib/presentation/agent/**`, existing Cloud Workloads
  provider/page/route code, and their data adapter only where required to
  consume and render the contract above.
- Focused server and Flutter tests plus this contract/tracker documentation.

## Explicitly out of scope

- `dirextalk-updater/**`, Docker publishing, normal Message Server deployment
  scripts, actual release execution, and unrelated historical Git cleanup.
- Real-account tests, Stack deployment, Worker image pushes, and
  credential-file access. Typed EC2 creation remains disabled by default and
  AWS SDK dependencies remain confined to the standalone Go Lambda module;
  the Message Server process must not acquire them.
- AWS key upload, secret bootstrap, purchase/approval, ingress, root command
  execution, stop/restart/destroy, cost enforcement, and service pairing.
- New Cloud lifecycle UI controls without a completed independent server-side
  control-plane contract.

Those mutation and lifecycle categories remain future work and are deliberately
not represented as implementation tasks in this read-only parity stage.

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

- [x] Add the optional `dirextalk.cloud-agent-workload/v1` summary to direct
  and streaming Agent results, derived only from a successful typed Cloud
  planning tool result.
- [x] Prove that no secret-like input/output, model prose, malformed tool
  result, or non-Cloud tool call can create the summary.
- [x] Render a Cloud workload milestone card in Flutter Agent chat, with a
  safe Plan-detail deep link and an honest non-ready status label.
- [x] Keep unknown/absent summary behavior backward compatible and cover it in
  Flutter reducer/widget tests.

### C. Stage close

- [x] Run the affected Message Server Native Agent tests and focused Flutter
  tests/analyzer; review the combined contract diff once.
- [x] Commit only the current-task changes in each owning repository. Preserve
  unrelated work, including the Message Server's untracked Cloud Worker run
  configuration and Flutter's unrelated `pubspec.lock` change.

### D. Go Connection Stack read-only registration/quote parity

- [x] Align the Go template and Lambda runtime with the existing Role Plan,
  explicit `/prod/v2/commands` endpoint, Stack identity and fixed Worker
  attestation parameters.
- [x] Add strict typed registration/quote payloads and responses compatible
  with the existing Orchestrator validators and golden vectors.
- [x] Add an atomic durable receipt/counter/issued-quote store with exact
  replay, command-id conflict, stale-counter and indeterminate-commit recovery.
- [x] Enable only registration verification and On-Demand quote reads through
  bounded provider interfaces; keep all mutation actions fail-closed.
- [x] Cover authentication-before-provider, replay-after-expiry, concurrent
  idempotency, provider failure, response de-secreting and IAM negative rules.
- [x] Run the nested Go tests/vet/Linux build, affected root Broker contract
  tests and template checks; perform one accumulated review and commit only
  current-task files.

### E. Typed deployment mutation boundary

- [x] Port the exact `deployment.create` payload and node-signature binding to
  the standalone Go Stack without enabling its HTTP action.
- [x] Port deterministic-CBOR ApprovalV1 payload generation, strict nested
  shape validation and device-signature verification.
- [x] Add a cross-module golden command produced by the Orchestrator and
  verified by the Stack, including proof drift and expanded-scope rejection.
- [x] Add the registered device-key resolver and one-time approval/challenge
  consumption to the same atomic deployment reservation transaction.
- [x] Bind the request to the persisted issued quote and deterministic-CBOR
  quote digest, fixed Worker
  AMI/network/manifest and an exact deterministic EC2 ClientToken.
- [x] Implement the typed create provider behind a disabled-by-default runtime
  gate, with fake provider fault injection and AWS read-back evidence.
- [x] Commit the receipt, approval consumption, deployment reservation and
  discovered EC2/EBS/ENI identities atomically before returning success.
- [x] Enable `deployment.create` only after replay, response-loss, concurrent
  approval consumption, stale generation/counter and read-back tests pass.

### F. Worker bootstrap and signed observation boundary

- [x] Reserve a one-deployment Worker session atomically with the approved
  deployment and bind it to the independently read-back EC2 instance during
  finalization.
- [x] Embed only a fixed, non-secret bootstrap manifest in EC2 UserData; the
  manifest contains no AWS credential, service secret, bearer, command, Recipe
  or public-ingress instruction.
- [x] Add the exact Worker claim route and Cloud Worker golden vector. Verify
  the AWS IID RSA signature plus account/Region/AMI/type/architecture/AZ and
  independent EC2/VPC/subnet/SG/tag/IMDS/no-public-IP/no-IAM read-back before
  issuing a five-minute lease.
- [x] Persist only the access-token SHA-256 digest, fence lease rotation by the
  prior epoch and immutable deployment bindings, and support bounded reconnect
  without accepting an expired first claim.
- [x] Add the signed `deployment.observe` golden contract, durable command
  receipt/counter, fresh idempotent observations and strict exclusion of
  session IDs, bearers/hashes, endpoint, IID, raw events and logs.
- [x] Keep a non-active or expired Worker observation retryable in the existing
  Orchestrator runner; only active, fresh evidence advances the durable
  provision state to `worker_bootstrap_verified`/`verifying`.
- [x] Add the retained/PITR/SSE/TTL Worker session table, exact claim route and
  IAM behind the disabled-by-default deployment gate; require an explicitly
  pinned IID RSA public key before that gate can be enabled.
- [x] Pass standalone Go tests/vet/Linux Lambda build, affected Orchestrator,
  Cloud Worker, store and command tests, one accumulated security/spec review,
  and commit only current-stage files.

### G. Fixed Worker execution-probe channel

- [x] Add exact signed `worker.task.issue` and `worker.task.observe` contracts
  compatible with the existing Orchestrator golden envelopes; admit only the
  digest-bound `execution_probe` task kind.
- [x] Add active-bearer heartbeat, task claim and task event routes with exact
  session/deployment/task path binding, lease epoch fencing and expiry checks.
- [x] Persist one retained/PITR/SSE `WorkerTasksTable` keyed by deployment/task;
  keep only immutable digests and the latest de-secreted sequence/checkpoint/
  error summary, never raw event JSON, logs, commands, URLs or secret values.
- [x] Atomically fence the signed node counter, command receipt and conditional
  task reservation so neither a conflict nor a lost response can leave a
  committed success receipt without its exact task.
- [x] Make claim and event processing response-loss safe with strongly
  consistent read-back, deterministic task ordering, attempt fencing and exact
  event-hash replay; keep heartbeat evidence in the Worker session record.
- [x] Wire the standalone Go Lambda and default-off CloudFormation routes/IAM
  to the existing non-root `cloud-worker`, without adding Recipe, root, shell,
  ingress, secret or general AWS capabilities.
- [x] Pass standalone Go tests/vet/Linux Lambda build and affected
  Orchestrator/Worker/store tests, perform one accumulated security/spec review,
  and commit only current-stage files.

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
- A valid signed registration/quote/observe/fixed-probe command reaches only
  its bounded Go provider or AWS-owned state store; every other command reaches
  only the fail-closed gate.
  Malformed, expired, future-dated, oversized, duplicate-key, wrong-key, and
  query-bearing requests cannot reach any provider operation. With the default
  gate a deployment command is rejected before proof/provider execution; when
  explicitly enabled, only the fully bound one-time transaction reaches the
  typed provider and exact retries reuse its deterministic ClientToken.

## Next action

Implement the first sealed private-Recipe installation slice without widening
the Stack into an arbitrary command runner. Reuse the existing
`RecipeExecutionManifestV1`, owner approval and coordinator contracts to add a
separate typed install task, compiled artifact delivery, durable checkpoint
resume and honest experimental-service verification. Root execution must stay
inside the exclusive VM and may receive only approved opaque secret/data/volume
slots; public ingress, arbitrary AWS APIs, destroy, local AWS credentials,
Stack deployment and real-account tests remain outside that slice until their
own typed boundaries exist.
