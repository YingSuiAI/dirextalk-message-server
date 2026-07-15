# Cloud Cleanup, Agent + Client Delivery Tracker

- Status: Flutter device-signed deployment intent is complete; in-place retained-volume rollback is the frozen next mutation
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
  `p2p/nativeagent/skills/cloud_deployment_planner/SKILL.md`, with a fixed
  allowlist containing the credential-free research tool plus read-only status
  and private-Recipe recommendation tools.
- Flutter already has owner-scoped `cloud.*` adapters, realtime projection
  reduction, `/agent/workloads` task/service pages, plan/service details, and
  a client-selected active-Connection entry into the restricted Agent chat
  (`d8e859e`, `22fc0bd`, `1386a71`).
- Successful restricted planning now adds a deterministic client-safe workload
  card, while status reads add de-secreted deep links and next-step guidance.
  Neither surface is lifecycle evidence or a mutation capability.

## Delivery order

1. **Historical Cloud artifact cleanup (complete).** The audit found the
   Connection Stack has an independent Cloud Orchestrator consumer but does not
   belong in `dirextalk-deployer`. Commit `016c62b` removes its Node/SAM bundle,
   directly coupled tests, and package/test wiring without changing normal
   deployer lifecycle behavior.
2. **Standalone Go control-plane port (complete).** The retained closed
   Connection Stack contract is a nested independent Go module; it adds no
   Node/npm runtime and is not imported by the Message Server root module.
3. **Agent-to-client workload milestone (complete).** The scoped Eino Agent and
   Flutter workload card, projection and reconnect contract are implemented.
4. **First-validation artifact/lifecycle closure (source complete).** Dynamic
   artifact transfer, Go AMI assembly, persistent monitoring, signed pairing
   resume and the fake lifecycle are implemented; the local prerelease artifact
   is built, while its S3 registration and dynamic AMI remain the next action.

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
  The same gate admits only fixed digest-bound Worker tasks, the sealed Recipe
  install/readiness flow and compiled start/stop/restart actions; it never
  accepts arbitrary commands, paths or AWS APIs. Verified destruction and
  retained encrypted backup use their own independent default-off gates.
- The CloudFormation execution role always grants its own log/receipt writes
  and the bounded quote read APIs. RunInstances/Worker-task permissions,
  exact-resource destruction and tagged CreateImage/read-back permissions
  exist only behind their respective explicit gates. It has no IAM PassRole,
  Secrets Manager, S3 write, ingress or arbitrary AWS permission. The Go
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

## Original parity-stage exclusions (historical)

This list defined the initial read-only parity slice. Workboard E through J
subsequently implemented the typed create, Worker, Recipe/readiness and verified
destroy boundaries; the remaining exclusions still apply unless a later
workboard section explicitly checks them off.

- `dirextalk-updater/**`, Docker publishing, normal Message Server deployment
  scripts, actual release execution, and unrelated historical Git cleanup.
- Real-account tests, Stack deployment, Worker image pushes, and
  credential-file access. Typed EC2 creation remains disabled by default and
  AWS SDK dependencies remain confined to the standalone Go Lambda module;
  the Message Server process must not acquire them.
- AWS key upload, secret bootstrap, ingress, arbitrary root commands,
  stop/restart, cost enforcement, and service pairing.
- New Cloud lifecycle UI controls without a completed independent server-side
  control-plane contract.

Those categories were deliberately absent from the original read-only parity
stage; later checked workboard sections are the authoritative delivery record.

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

### H. Sealed private-Recipe task transport

- [x] Claim approved `cloud.recipe_execution.install.requested` outbox intents
  in the independent Orchestrator and revalidate the active Connection,
  Deployment, EC2 resource, Worker lease, approved manifest and install Job
  before every external attempt.
- [x] Add exact signed `worker.recipe_task.issue` and
  `worker.recipe_task.observe` commands carrying only the canonical sealed
  `RecipeExecutionManifestV1`, immutable digests, opaque slots and declared
  checkpoints; exclude commands, URLs, paths, artifact bodies and secret
  values.
- [x] Persist issue/observe commands, leases and Recipe task projections in
  PostgreSQL with response-loss replay, stale-lease fencing and monotonic
  Job/Step completion; cover issue, defer/reclaim, observe and success with a
  real PostgreSQL integration test.
- [x] Extend the standalone Go Connection Stack with atomic counter/receipt/
  task reservation, strict manifest digest verification, active-bearer claim
  and lease/attempt/sequence/checkpoint-fenced event routes on the existing
  retained Worker task table.
- [x] Add a separate Cloud Worker Recipe client and executor injection
  boundary. A Worker can claim only when a fixed digest/action catalog, CAS
  checkpoint store and typed action driver are all explicitly supplied; the
  production command supplies none and therefore remains fail-closed.
- [x] Pass standalone Stack tests/vet/Linux build and affected Orchestrator,
  PostgreSQL, Worker and command tests, perform one accumulated
  security/spec review, and commit only current-stage files.

### I. Fixed production Recipe and experimental Service readiness

- [x] Add one immutable compiled non-business Recipe bundle whose descriptor,
  action ID, Worker image/resource manifest and sealed execution manifest are
  digest-bound; keep all task-selected commands, paths, ports and URLs absent.
- [x] Add a restart-persistent private Worker checkpoint store with atomic
  replace, compare-and-swap, binding-derived filenames and strict state
  decoding; prove checkpoint recovery without claiming cross-process fencing.
- [x] Add one audited root ActionDriver that installs only the fixed hardened
  systemd probe service through absolute typed operations, plus a loopback-only
  probe binary; keep the production gate disabled by default and fail closed
  unless the fixed binaries, catalog, checkpoint store and transports all load.
- [x] Add exact signed readiness issue/observe commands and active-bearer
  claim/event routes. Persist only challenge digest/expiry and de-secreted
  evidence, rotate a lost pre-event challenge safely, and fence every event by
  Worker lease epoch, attempt and sequence.
- [x] Require the fixed semantic body digest plus a distinct Stack observation
  digest before creating a Service. Treat this only as Stack-witnessed
  freshness suitable for `experimental`, not hostile-root-proof monitoring or
  `managed` acceptance.
- [x] Atomically create the Recipe-bound experimental Service and canonical
  Service/Deployment/Job projections on success. On failure or interruption,
  create no Service and retain the still-billable resource as
  `retained_tracked` without stopping or destroying it.
- [x] Pass standalone Stack tests/vet/Linux build, affected Orchestrator,
  PostgreSQL, Worker, probe-service and command tests, Linux builds, secret and
  dependency checks, one accumulated security/spec review, and commit only
  current-stage files.

### J. Device-approved verified Service destruction

- [x] Add owner HTTP-only prepare/approve actions whose Ed25519 proof binds the
  exact Service/Deployment revisions, Connection, Recipe digest and tracked
  EC2/EBS/ENI identifiers; keep Agent and MCP unable to sign or approve it.
- [x] Atomically move the Service and public/private resource axes to
  `destroying`, create a destroy Job/Step and enqueue only a private typed
  Orchestrator intent after signature and revision verification.
- [x] Persist one lease-fenced `deployment.destroy` command and node counter in
  PostgreSQL before network I/O, and replay the exact envelope after timeout,
  response loss or an AWS transition still in progress.
- [x] Add the standalone Stack's one-use approval/challenge reservation,
  original create-receipt resource binding, typed EC2 terminate/delete provider
  and retained DynamoDB receipt state behind independent default-off gates.
- [x] Delete in instance, ENI and EBS dependency order and require individual
  AWS read-back absence for every approved identifier before returning a
  committed `verified_destroyed` receipt.
- [x] Commit `destroyed`/`verified_destroyed` only after the persisted receipt
  is independently revalidated in the Orchestrator transaction; otherwise
  retain tracked resources as `blocked`, the Service as `degraded`, and the Job
  checkpoint as `destroy_blocked`.
- [x] Cover approval tampering/replay, exact command recovery, provider
  transition retries, lost responses, stale claims, private-resource leakage
  and verified/blocked terminal states; pass the affected root and standalone
  Go checks and one accumulated security/spec review.

### K. Device-approved managed Service lifecycle operations

- [x] Add owner HTTP-only `cloud.services.operation.plan/approve`; accept only
  `service_id`, expected revision and `start|stop|restart`, while deriving the
  artifact, opaque action, root scope, timeout and checkpoints from the exact
  installed managed Recipe. Keep Agent and MCP read-only.
- [x] Bind deterministic-CBOR Ed25519 approval to the expected Service status,
  Service/Deployment revisions, Connection, Recipe and installed/compiled
  artifact digests plus the fixed lifecycle capability.
- [x] Persist approval, sealed operation task, Job/Step and private outbox
  atomically. Reject concurrent lifecycle or destroy work for the same Service
  and fence execution against the exact signed Service revision and status.
- [x] Reuse the signed `worker.recipe_task.issue/observe` channel without adding
  a command, path, URL, slot or AWS capability. Persist the exact signed
  envelope before I/O and replay it after disconnect or response loss.
- [x] Extend the fixed audited Worker bundle with typed systemd
  start/stop/restart actions and restart-persistent checkpoints; retain the old
  install-only artifact digest for already approved executions.
- [x] Project queued/running/terminal Job revisions; on success preserve
  `experimental` until management acceptance (or publish managed `active`) and
  publish `stopped` for stop, on failure publish `degraded`, and in every
  outcome leave the EC2 resource active, tracked and billable.
- [x] Cover approval tampering/idempotency, exact command recovery, stale lease
  and Service revision fencing, managed action execution, active-operation
  destroy exclusion and stopped terminal state; pass the affected Go checks,
  Linux builds and one accumulated security/spec review.

### L. Device-approved retained encrypted Service backup

- [x] Extend the existing owner HTTP-only Service operation prepare/approve
  actions with `operation=backup`; bind one Ed25519 approval to the exact
  Service/Deployment revisions, Connection, Recipe digest, EC2 instance,
  complete tracked EBS volume set and manual-retention policy. Agent, MCP and
  Worker receive no approval or AWS control capability.
- [x] Atomically persist the private approval, retained-backup ledger,
  backup Job/Step and outbox intent without changing the Service status or
  Deployment resource axis. Exclude concurrent lifecycle, backup and destroy
  work for the same Service.
- [x] Persist the exact signed `service.backup` command and node counter before
  network I/O; replay the identical envelope after timeout, response loss or
  an incomplete AWS transition.
- [x] Add the standalone Stack's one-use approval/challenge reservation and
  retained/PITR/SSE backup table behind an independent default-off gate. The
  typed provider uses a deterministic unique AMI name as the mutation fence,
  because EC2 `CreateImage` has no ClientToken, and creates no independent AWS
  control path.
- [x] Return success only after AWS read-back verifies the retained AMI and one
  completed encrypted snapshot for every approved volume. The backup is
  crash-consistent (`NoReboot=true`), manually retained and remains visible
  through `cloud.services.list/get`; success, failure and Service destruction
  do not implicitly delete it.
- [x] Cover deterministic-CBOR parity, approval/resource drift, durable command
  ordering, Stack reservation/replay, unique-name recovery, encrypted
  read-back and PostgreSQL terminal axes; pass the affected Go checks, Linux
  builds and one accumulated security/spec review.
- [x] Close the owner visibility path: accept `backup` Job projections, advance
  the Service revision without changing its status/resource axes when a backup
  reaches a terminal state, publish the strict full Service summary with only
  retained AMI/snapshot identifiers, and render status, manual retention and
  encrypted snapshot count in Flutter; restore/delete controls remained gated
  at that stage
  (`dirextalk-flutter` `b06a35e`).

### M. Flutter AWS Connection Stack onboarding

- [x] Add a persistent Ed25519 device approval identity backed only by platform
  secure storage. Export only the stable key id and RFC 8410 SPKI public key;
  corrupted storage fails closed instead of silently rotating an identity
  already bound to a Cloud Connection.
- [x] Add typed HTTP-only Flutter adapters for
  `cloud.connections.role_plan` and
  `cloud.connections.registration.complete`; exclude reserved command paths
  from collection-id inference and redact candidate Broker URL/Stack ARN from
  client diagnostics.
- [x] Add a resumable AWS Connection Sheet that persists only non-secret Role
  Plan metadata and request fingerprints, opens the immutable CloudFormation
  Quick Create handoff, and submits Stack outputs for independent verification.
  It contains no AK/SK upload and explicitly states that this stage neither
  creates EC2 nor starts billing.
- [x] Cover RFC 8410 identity/signature golden behavior, secure persistence and
  corruption, onboarding resume, China-partition URL encoding, typed request
  routing/log redaction, and the complete widget handoff flow
  (`dirextalk-flutter` `462c326`).

### N. Flutter device-signed deployment intent

- [x] Decode the immutable QuoteV1 candidate set and render Region, validity,
  capacity, hourly estimate, 30-day estimate and exclusions without implicitly
  purchasing a resource.
- [x] Add HTTP-only adapters for `cloud.plans.confirmation.prepare` and
  `cloud.plans.approve`, redact the complete signed approval from diagnostics,
  and reject all public-ingress, secret, integration, Spot, device-key and
  Plan/quote drift before the device can sign.
- [x] Produce byte-identical deterministic-CBOR ApprovalV1 signatures on
  Flutter native and web, persist only the unsigned short-lived challenge and
  UUID idempotency keys, and replay the exact signature after an ambiguous
  response or application restart.
- [x] Keep the current UI honest while the independent provision executor is
  disabled: approval records a queued deployment intent but explicitly says
  that it does not create AWS resources or begin billing. The future
  “确认创建并开始计费” label remains gated on an enabled typed executor.
- [x] Cover the Go/Dart golden vector, unsafe-scope rejection, HTTP-only
  routing/log redaction, quote review, ambiguous retry, static analysis and a
  Flutter Web release build (`dirextalk-flutter` `92b5698`).

### O. Original-instance retained-volume rollback

- [x] Freeze `ServiceRestoreApprovalV1` and the typed `service.restore`
  command/result contracts around one retained backup, the original instance,
  exact encrypted replacement-volume mappings, manual original-volume
  retention and `reattach_original` failure policy. Keep both Stack and
  Orchestrator mutation gates default-off.
- [x] Add the standalone Go Connection Stack provider and durable DynamoDB
  reservation/receipt path. Use deterministic CreateVolume ClientTokens,
  enforce stop/detach/attach/start convergence, persist fallback phase outside
  the VM, and return only AWS-read-back `aws_restore_applied`,
  `aws_original_restored` or `restore_blocked` evidence.
- [x] Add PostgreSQL v57 signed-command journaling and single-counter replay,
  the independent `ServiceRestoreRunner`, terminal non-success projection and
  critical alerts. An applied AWS mapping enters `verifying`, never success.
- [x] Reuse the typed Worker/Stack semantic-readiness challenge with a
  restore-purpose task. Only AWS mapping evidence plus semantic readiness can
  finish the restore as succeeded; fallback and blocked results remain failed
  or blocked with resources tracked.
- [x] Include durable restore summaries in `cloud.services.list/get` and strict
  `cloud.service.changed` projections. Flutter now persists the short-lived
  plan/confirmation idempotency flow, verifies the shared Go/Dart
  deterministic-CBOR golden, signs on the device, shows Region/cost/downtime
  and retained-volume warnings, and renders restore progress/history.
- [x] Cover exact signed-envelope reuse, current-volume/revision binding,
  applied-to-verifying behavior, semantic verification, verified original
  fallback, blocked non-success, strict projection and Flutter contract/UI
  checks. No real AWS mutation is enabled by this stage close.

### P. Experimental-to-managed Service acceptance (complete)

- [x] Freeze `ServiceManagementAcceptanceApprovalV1` around the exact current
  Service/Deployment/Recipe revisions, installed and source artifact digests,
  health/lifecycle contracts, exact semantic-readiness and Stack-observation
  digests, volume/data/secret slots, post-restore restart,
  retained backup/restore evidence and the complete tracked destroy set.
- [x] Add owner HTTP-only `cloud.services.management.plan/approve`. Planning
  may move only a matching `experimental` Service and, when needed, its
  `experimental` Recipe to `awaiting_management_acceptance`; an already
  `managed` Recipe is bound without changing revision. Approval requires a five-minute
  device signature over the server-derived confirmation and atomically publish
  `active` Service plus `managed` Recipe revisions.
- [x] Persist prepare/approve idempotency, immutable confirmation evidence and
  terminal acceptance results. Reject stale revisions, expired challenges,
  changed evidence, signature/key mismatches and concurrent lifecycle,
  backup, restore or destroy work without changing maturity.
- [x] Add the Flutter device-signed management-acceptance flow and display the
  full evidence checklist before the user accepts managed operation. Keep
  Agent chat and public MCP read-only and unable to sign or promote maturity.
- [x] Pass focused Go/Dart contract, persistence, projection and UI checks,
  regenerate the public action artifact, perform one accumulated stage review,
  and commit the two owning repositories independently.

### Q. Selectable private Recipe and scoped service-secret lifecycle

- [x] Let only the owner bind one current private Recipe id/revision when a
  research Goal is created. Revalidate the immutable Recipe at claim and
  commit, allow the Eino Cloud Skill to recommend only de-secreted Recipe
  summaries, and keep Agent/MCP unable to select, approve or mutate resources.
- [x] Register one verified `CompiledRecipeArtifactV1` internally and require
  exact Plan/Recipe/artifact/manifest agreement for volume, data and secret
  slots. Preserve legacy no-slot hashes and expose no artifact registrar to
  ProductCore, Agent or public MCP.
- [x] Add owner HTTP-only `cloud.secrets.bootstrap.plan`, deterministic
  Ed25519/CBOR and X25519/HKDF/AES-GCM Go/Dart vectors, and a cancellable
  Flutter upload flow whose plaintext, token, keys, ciphertext and Stack URL
  never enter logs, persistence, Matrix, Agent prompts or ProductCore events.
- [x] Add the default-off Go Connection Stack service-secret session,
  KMS/Secrets Manager provider and completed-only Dynamo binding. Keep the
  ten-minute bootstrap authorization separate from deployment-secret
  retention and resolve the dynamic provider version only inside the Stack.
- [x] Materialize only for an active Worker lease and exact current task,
  deployment, Recipe, artifact, slot and `secret_ref`. The trusted Worker
  catalog may choose only a fixed file/environment destination; an upload race
  must wait with a bounded retry, survive restart, and never run the driver
  without the value.
- [x] Persist the node-signed `service.secret.observe` command before I/O with
  a fenced PostgreSQL lease and exact replay. Reconcile observe-before-create
  and completed-at-expiry races without storing provider versions, endpoints,
  tokens, ciphertext or secret values in the Message Server database.
- [x] Extend device-approved Service destruction and Flutter confirmation to
  bind the exact Recipe-derived `secret_ref` set. Delete deterministic Secrets
  Manager resources only after EC2/ENI/EBS dependency cleanup, require
  `NotFound` read-back, purge Stack bindings, and leave AccessDenied as
  `destroy_blocked` rather than `verified_destroyed`.
- [x] Run the affected root/nested Go tests and vet, real PostgreSQL recovery
  tests, Linux Orchestrator/Worker/Broker builds, focused Flutter tests,
  analysis, Web release build, secret-canary and diff checks; perform one
  accumulated review, update contracts once, and commit both repositories.

### R. Production-shaped generic OCI Recipe execution

- [x] Add a strict, digest-pinned `OCIServiceBundleV1` and a trusted two-phase
  compiler. Compile the executable bundle before the Worker catalog exists,
  then finalize the public artifact only after the catalog-derived Worker
  resource manifest digest is known; reject every bundle drift on finalize.
- [x] Add an immutable OCI catalog and Worker resource manifest that bind the
  measured Worker binary, fixed Podman runtime, bundle digest, action IDs and
  file-secret destinations. Reject tag, catalog, binary, runtime and approved
  manifest drift before the Worker can claim a Recipe task.
- [x] Add an install-only typed root Driver with no shell or generic argv. Use
  deterministic container identity, fixed checkpoint recovery, loopback-only
  ports, read-only container mode, dropped capabilities and fixed Podman
  operations; retry liveness, readiness and semantic probes until timeout.
- [x] Carry scoped service values only through verified tmpfs files mounted
  read-only beneath `/run/secrets`. The OCI runtime rejects environment-secret
  targets because Podman would persist expanded values in container metadata.
- [x] Add a separate default-off OCI Worker gate. Validate strict root-owned
  catalog/resource files, the running binary and both bootstrap digests before
  constructing the Recipe loop. Keep the first-validation external readiness
  contract fixed until a separately versioned catalog-backed probe is added.
- [x] Prove compiler-to-catalog-to-Executor-to-Driver execution, restart
  recovery, drift rejection, zero-claim fail-closed behavior and Linux amd64
  Worker construction; run one accumulated security/spec review and resolve
  its secret-persistence, checkpoint and startup-probe findings.

### S. First-validation artifact and recoverable lifecycle closure

- [x] Add the signed dynamic Recipe-artifact prepare/complete channel. Bind the
  upload to deployment/task/execution/Recipe/artifact/manifest digests, verify
  the exact versioned S3 `versionId`, checksum, size, media type and KMS
  encryption, and persist Stack plus PostgreSQL receipts before a Worker may
  consume the artifact.
- [x] Add Go-only `cloud-worker-artifact` and `worker-image-builder` commands.
  The first produces the measured Worker/catalog archive; the second accepts
  only a unique prerelease version, builds a private encrypted IMDSv2 AMI,
  verifies image/snapshot read-back, and supports the default-off dynamic
  Recipe-artifact mode. No Node/npm or release/deployer script is involved.
- [x] Add the compiler-owned `OCIServiceRuntimeProfileV1` and carry its exact
  entrypoint/argv, run-as, bounded tmpfs, storage and secret-environment
  bindings through compiler, artifact, Worker catalog and typed Podman host.
  Digest-pinned OpenClaw and Hermes fixtures prove their required non-shell
  profiles; these fixtures are not a claim of live AWS deployment.
- [x] Add a persistent PostgreSQL Service monitor with lease/generation
  fencing, restart recovery, periodic Stack-witnessed semantic checks, alerts
  and recovery that cannot clear an unrelated degradation. This remains
  control-plane-witnessed monitoring, not hostile-root-proof external health.
- [x] Replace the placeholder pairing resume with an owner HTTP-only,
  device-signed, idempotent transition for the exact
  `waiting_user_pairing` Deployment and existing install Job. Flutter signs the
  challenge and applies only higher revisions; no pairing code enters
  ProductCore, events or Agent context.
- [x] Preserve newer Cloud revisions across Flutter bootstrap/reconnect and
  recover cursor resets or revision gaps without regressing in-flight state.
  The Eino status projection now returns de-secreted client deep links and
  deterministic next-step guidance, including `destroy_blocked`/continued
  billing warnings, while Agent and MCP retain no lifecycle mutation tool.
- [x] Exercise the durable approval, provision, install, readiness and verified
  destruction runners as one fake-provider lifecycle, including retained and
  blocked resource semantics without a real AWS mutation.
- [x] Build the local Linux/amd64 validation set as the unique prerelease
  `v1.1.0-cloud-mvp.20260715.1`. The digest-pinned OpenClaw image starts with
  gateway authentication enabled and returns the stable `/health` evidence
  digest `sha256:6191c1f860b8a0225c697e46ebce756193dfb18c189218cfe742037501da05eb`;
  the measured Worker archive is 11,034,112 bytes with archive digest
  `sha256:cbf8dcc549f13354daa2bfbe91760fd0767b47023a076eee474815cfcca005ca`
  and catalog digest
  `sha256:a420cb82e325ec2e3fd47365b1407140c9f592ef61d1b0836d88302be1a3ab22`.
  This local evidence is not an S3 registration, AMI, or AWS deployment.
- [x] Add owner HTTP-only, device-signed `cloud.jobs.cancel.plan/approve` for
  cancellable provision/install/verify Jobs. Cancellation atomically fences
  late Worker/provider results, finishes execution with outcome `canceled`,
  retains every discovered resource as tracked and billable, and never stops
  or destroys infrastructure.
- [x] Add owner HTTP-only, device-signed
  `cloud.deployments.destroy.plan/approve` for a failed, interrupted or
  canceled Deployment that has tracked resources but no Service. The approval
  binds the exact EC2/EBS/ENI/secret-ref ledger, preserves the original
  execution/outcome axes, uses the typed Connection Stack destroy/read-back
  provider, and reports success only as `verified_destroyed`. Fatal or
  forbidden cleanup remains `blocked`; no synthetic Service is created.

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

Upload the already built uniquely versioned **prerelease** Worker artifact and
register its exact dynamic-artifact S3 `versionId`, then use the Go builder to
assemble one matching dynamic-artifact AMI. Do not use `latest`, the formal
`v1.0.3` tag, or any release/deployer script.

Real AWS validation is currently blocked until a non-root, least-privilege AWS
credential is available and SSH access to `a8.dirextalk.ai` is working. Even
after those prerequisites are fixed, immediately before any billable create
the owner must confirm the latest Region, instance/disk specification and live
quote. Until then keep every real mutation gate off and use the completed fake
lifecycle only.
