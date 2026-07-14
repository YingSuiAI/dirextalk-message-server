# Cloud Orchestrator MVP Contract

This document defines the current-stack ProductCore boundary for the generic
cloud scheduling MVP. It is intentionally a control-plane contract, not an
AWS SDK integration in the message-server process.

## Boundary

- The Message Server is an owner-authenticated façade and durable projection.
  It never receives AWS long-lived credentials, never imports an AWS SDK, and
  never invokes an AWS CLI command.
- The server-side Eino Native Agent owns a built-in Cloud Deployment Planner
  skill and its `native_agent_cloud_deployment_plan` tool. It can only submit
  a validated, connection-bound research Goal through a narrow port; it has no pricing,
  approval, AWS, ingress, lifecycle, or credential API. This is not a Codex
  workspace Skill and it is not exposed through external MCP.
- A request-scoped `cloud_dialogue_mode=true` is a strict capability reduction
  for Cloud planning acceptance tests and future Cloud dialogue UI. It exposes
  only `native_agent_cloud_deployment_plan` and the read-only
  `native_agent_cloud_status`, forces no-memory operation, and excludes runtime
  shell/CLI tools, external MCP, dynamic Skill/MCP management, ordinary
  Dirextalk tools, installed Skill prompts, and request/config prompt injection.
  It never grants mutation, approval, secret, or AWS access.
- The separately deployed Cloud Orchestrator binary now lives at
  `p2p/cmd/cloud-orchestrator`. It consumes `p2p_cloud_outbox` with a dedicated
  database role, uses a private mTLS research endpoint, and makes the fixed
  HTTPS Connection Stack V2 research, quote, and registration calls. It has no
  Matrix config, model key, AWS SDK, Docker socket, or migration capability.
  Its mounted Ed25519 node key signs exact durable envelopes but is never
  persisted or sent to the Message Server. The typed `deployment.create`
  runner is present only as a tested control-plane component: the production
  main process deliberately leaves provision outbox entries unclaimed. It does
  run a separate signed, read-only `deployment.observe` loop for a deployment
  that already has a durable create receipt. That loop cannot create, stop,
  start, expose, or destroy a Worker; it advances the private Job checkpoint
  only after the Connection Stack returns current, IID-verified active-lease
  evidence for the receipt-bound instance. The Connection Stack source now has
  a closed Worker bootstrap claim/events API,
  IMDSv2 identity verification, and EC2 read-back, but the production
  Orchestrator remains gated until a reviewed fixed Worker AMI, recipe
  executor, service-health evidence, and operational Stack configuration are
  deployed together. The current source provides a bounded Worker
  reauthentication protocol: a first claim remains limited to the at-most-ten
  minute bootstrap window, while an already active, independently IID-verified
  Worker may rotate its bearer and epoch during a 24-hour recovery-retention
  fence. It is intentionally not part of the default compose stack until its
  private researcher, node-key mount, Connection Stack registration, Worker
  identity certificate, and least-privilege database role are deployed. The
  Message Server neither hosts that Worker session broker nor enables an AWS
  mutation executor.
- The user-owned AWS Connection Stack is the AWS mutation boundary. Its Broker
  Lambda accepts a closed command set only. A Worker has root only inside its
  own exclusive VM and receives no EC2/IAM/EBS control credentials.
- The public `/mcp` endpoint remains read-only with respect to Cloud. No
  `cloud.*` ProductCore action is callable by `agent_token`, and no Cloud
  mutation is exposed as an MCP tool in this stage. Its only Cloud tools are
  `dirextalk_cloud_workloads_list`, `dirextalk_cloud_workloads_get`, and
  `dirextalk_cloud_status`; they return whitelisted Plan/Deployment/Service
  projections, de-secretsed Job aggregate counts, and alert metadata, never a Goal prompt, Plan narrative, outbox
  record, connection account data, secret reference, pairing data, or service
  secret.

## Current implemented control-plane slices

`cloud.connections.role_plan` creates a private, short-lived
`awaiting_stack` bootstrap and returns an owner-only CloudFormation handoff.
The handoff contains the immutable template and complete source-tree digests,
template URL, deterministic stack name, requested Region, and the
Orchestrator/device **public** signing
identities. It neither accepts AWS credentials nor creates a public Connection
record. The Flutter approval private key must remain in system secure storage.
The Message Server enables this action only when its public Stack identity is
complete: `P2P_CLOUD_CONNECTION_STACK_TEMPLATE_URL`,
`P2P_CLOUD_CONNECTION_STACK_TEMPLATE_DIGEST`,
`P2P_CLOUD_CONNECTION_STACK_SOURCE_TREE_DIGEST`,
`P2P_CLOUD_CONNECTION_NODE_KEY_ID`, and
`P2P_CLOUD_CONNECTION_NODE_PUBLIC_KEY_SPKI_BASE64`. The source-tree digest
binds the local SAM source used by the non-root deploy helper to the reviewed
release rather than trusting a template file alone.

After the owner deploys that stack in their AWS account,
`cloud.connections.registration.complete` accepts only the exact Broker command
URL and Stack ARN plus a UUID idempotency key and expected revision. The server
persists these private candidate facts, creates a standalone
`connection_registration` Job, and queues signed verification. The completion
response, websocket events, MCP, list/get projections, and audit events omit
the endpoint and Stack ARN; the submitted stack is not visible as an active
Connection yet.

The registration runner persists one exact
`connection.registration.verify` envelope before its first Broker request and
replays it after indeterminate failures. Only the Broker's exact
`expired_command` result allocates a new counter. The Broker must attest the
same bootstrap, Connection ID, account, Region, Stack ARN, node key/generation,
command ID, request digest, and exact API Gateway command URL. A fenced
transaction then writes the private Broker metadata and one public active
Connection, emits a de-secretsed `cloud.connection.changed` projection, and
finishes the registration Job. Any mismatch fails closed and leaves no active
Connection.

`cloud.goals.create` durably creates a private Goal, a Plan in `researching`,
and a separate `research_queued` Job/Step (`queued` / `pending`). It creates
three de-secretsed Cloud audit events, three projection-outbox entries, and one
`cloud.goal.research.requested` outbox entry in one PostgreSQL transaction. Its
PostgreSQL conflict handling also replays concurrent submissions of the same
idempotency key to the one winning Goal/Plan. It performs no cloud mutation.

The Eino tool derives a UUID idempotency key scoped to one Agent chat
invocation, so a model tool retry for the same intent returns the existing
research Goal/Plan without collapsing a later, separately requested identical
workload. Before either entrypoint writes anything, credential-shaped content (AWS keys,
private keys, GitHub/model tokens, or secret assignments) is rejected. A
planning goal may name only a `secret_ref` placeholder; actual secret material
belongs to the later client-encrypted bootstrap path.

The Native Agent runtime also runs with an isolated runtime home rather than
the message-server host home and rejects direct or common wrapped AWS CLI
invocation. This is defense in depth, not a substitute for the typed
Connection Stack boundary or a sandbox for arbitrary runtime commands.

The researcher can return only an experimental `RecipeV1`, a non-price
`ResearchDraftV1` (region plus one to three On-Demand candidate requests), and
display title/summary. It cannot return a `PlanV1`, `QuoteV1`, price, quote
identifier, approval, plan hash, or digest. The fenced Store derives the
immutable `QuoteRequestV1`, transitions the Plan to `quoting`, finishes the
research Job as `research_ready`, and atomically queues a separate quote Job
and `cloud.plan.quote.requested` outbox item.

The quote runner allocates a monotonically increasing per-connection node
counter, persists the exact signed V2 envelope before HTTP, and replays that
same envelope after any indeterminate failure. Only the Broker's exact
`expired_command` response retires it and permits a new counter. The Broker
can only return the read-only AWS price/instance-offering quote; on strict
receipt validation the Store writes the immutable Quote, keeps the Plan in
`quoting`, records its `quote_id`, and emits safe Plan/Job projections. Each
candidate also carries verified architecture, vCPU, memory, GPU count, and
total GPU-memory facts from the Broker's read-only instance-type lookup; a
later confirmation never guesses capacity from an instance-type string.

`cloud.plans.confirmation.prepare` is the owner-only transition from a quoted
Plan to one immutable `ready_for_confirmation` `PlanV1`. It binds exactly one
quoted tier, recipe digest, capacity scope, no-public-ingress network scope,
and empty secret/integration scopes. The Store verifies the active Connection's
registered Flutter Ed25519 public key, persists the canonical Plan version and
an unsigned short-lived `ApprovalV1` challenge, and returns that reviewable
challenge. It does not create an AWS resource or hand a secret to a Worker.

`cloud.plans.approve` consumes the exact signed, unexpired persisted challenge.
In one PostgreSQL transaction it verifies the device signature and revision,
marks the Plan `approved`, creates a queued/pending Deployment and provision
Job/Step, and writes a private `cloud.deployment.provision.requested` outbox
row. It emits only de-secretsed Plan/Deployment/Job summaries. The provision
outbox is intentionally unclaimed by the production Orchestrator until the
reviewed Worker AMI/executor and the deployed Connection Stack identity
configuration establish the complete execution boundary. The typed Broker
`deployment.create` runner must not become active behind an operator switch
merely because the bootstrap claim endpoint exists. This action has not yet
created EC2, EBS, an ingress rule, or a billable resource.

After a later typed creator has recorded a private Worker receipt, the
Orchestrator may issue a durable, signed `deployment.observe` read. The Stack
re-reads its receipt-bound Worker session and returns only
`dirextalk.aws.deployment-observation/v1`: the deployment and instance binding,
the fixed `provisioning` receipt status, active lease epoch/expiry, sequence,
and observed time. It never returns a session ID, bearer/hash, endpoint, IID,
raw event, or log. A non-active or stale observation is deferred; only current
active evidence atomically advances the provision Job from
`worker_bootstrap_pending` to `worker_bootstrap_verified` and moves execution
to `verifying`. A read retry reuses its exact persisted envelope; only the
Stack's explicit `expired_command` result permits a new node counter.

If the challenge or its bound Quote expires before approval, the same
transaction instead marks the approval and Plan `expired`, emits a safe Plan
event, records the failed approval idempotency outcome for replay, and creates
no Deployment or provision outbox. A `ready_for_confirmation` Plan can never
be left permanently unapprovable after its challenge expires.

The private research and quote outboxes contain the natural-language goal or
pre-price request only. ProductCore websocket events and `cloud.events.list`
carry summaries only; they never copy the goal prompt, AWS credentials,
session tokens, pairing URLs, QR payloads, service secrets, Broker endpoint,
signed envelope, receipt, or node-key identity.

The implementation persists recipes, verified quotes, quote command receipts,
private connection bootstraps and registration command receipts, jobs and job
steps, plus goals, plans, canonical Plan versions, one-time approval challenges,
connections, deployments, services, alerts, Cloud audit events, private
research/quote/registration/provision outbox records, Worker-bootstrap
observation leases and exact signed observation-command receipts, and projection
outbox records. The de-secreted private observation evidence is kept separate
from public projections. The consumed approval signature stays in the private
approval table; it is not part of any ProductCore response, event, MCP result,
or Agent input.
Deployment creation is limited to the approved durable intent above; Service
writers and all actual cloud mutations remain outside the Message Server.

## ProductCore actions

All actions require the owner access token. Read actions may use owner HTTP or
ready realtime `client.request`; every create, approval, pairing, service
operation, and destruction action is HTTP-only. `agent_token`, old owner
tokens on `/mcp`, and websocket `client.request` cannot invoke an HTTP-only
Cloud mutation.

| Action family | Current behavior | Transport |
| --- | --- | --- |
| `cloud.bootstrap` | returns owner projections (`goals`, `plans`, `jobs`, `connections`, `deployments`, `services`, `recipes`, `alerts`) | HTTP + WS request |
| `cloud.{connections,plans,deployments,services,recipes}.list/get` | typed owner-only projection reads; only `cloud.plans.get` may attach a de-secretsed quote detail | HTTP + WS request |
| `cloud.events.list` | de-secretsed durable Cloud audit events; `limit` is 1–200 | HTTP + WS request |
| `cloud.goals.create` | creates a `researching` Goal/Plan and a planner outbox request | HTTP-only |
| `cloud.connections.role_plan` | creates/replays a short-lived private Stack bootstrap and returns a safe CloudFormation Role Plan | HTTP-only |
| `cloud.connections.registration.complete` | records Stack outputs as a private pending verification and returns its safe Job binding; it cannot activate a Connection directly | HTTP-only |
| `cloud.plans.confirmation.prepare` | binds a quoted capacity tier into an immutable no-ingress/no-secret/no-integration PlanV1 and returns a short-lived device challenge | HTTP-only |
| `cloud.plans.approve` | verifies that exact device signature, then atomically queues the private provision intent; it does not create an AWS resource itself | HTTP-only |
| `cloud.deployments.pairing.resume`, `cloud.services.*.plan/approve` | declared high-risk contracts; return `503 cloud_orchestrator_unavailable` until their independent control-plane transitions are installed | HTTP-only |

`cloud.connections.role_plan` accepts exactly:

```json
{
  "provider": "aws",
  "region": "ap-northeast-1",
  "device_approval_key_id": "device-key-id",
  "device_approval_public_key_spki_base64": "Ed25519-SPKI-base64",
  "idempotency_key": "UUID"
}
```

It returns a `role_plan` with `bootstrap_id`, `cloud_connection_id`, expiration,
template URL/digest, complete source-tree digest, stack name, and public
CloudFormation parameters. The server rejects an unavailable/invalid stack
identity, non-AWS provider,
invalid Region or Ed25519 SPKI, non-UUID idempotency key, and a conflicting
idempotency replay. It never returns a node private key, AWS credential,
Broker endpoint, Stack ARN, or service secret.

`cloud.connections.registration.complete` accepts exactly:

```json
{
  "bootstrap_id": "cloud_bootstrap_…",
  "expected_revision": 1,
  "idempotency_key": "UUID",
  "broker_command_url": "https://abcdefghij.execute-api.ap-northeast-1.amazonaws.com/prod/v2/commands",
  "stack_arn": "arn:aws:cloudformation:ap-northeast-1:123456789012:stack/dirextalk-example/…"
}
```

The server validates the regional API Gateway URL and same-Region
CloudFormation ARN before durable storage, then returns only
`bootstrap_id`, `cloud_connection_id`, status/revision, and `job_id`. It uses
the expected revision and completion idempotency digest to reject stale or
conflicting completion attempts. A role-plan expiry is terminal for that
bootstrap; the owner must request a new Role Plan.

`cloud.goals.create` accepts exactly:

```json
{
  "goal": "Deploy a private knowledge service with a reviewable recipe.",
  "cloud_connection_id": "existing-connection-id",
  "idempotency_key": "UUID"
}
```

`goal` is 1–12,000 Unicode characters. `cloud_connection_id` is required and
must already exist; otherwise the server returns
`400 cloud_connection_required` before it creates an outbox record. This avoids
an unbound research request that cannot produce a valid connection-bound quote.
The raw idempotency UUID is never stored; the durable row
contains a SHA-256 digest and a second request digest. Replaying an identical
request returns the original Goal/Plan; reusing the key with different intent
returns `409 cloud_idempotency_conflict`.

`400 cloud_goal_secret_not_allowed` means the goal contained credential-shaped
material. Clients must remove it and submit a `secret_ref` placeholder instead;
the server does not redact and persist a partially accepted goal.

The response is:

```json
{
  "goal": {
    "goal_id": "cloud_goal_…",
    "plan_id": "cloud_plan_…",
    "status": "researching",
    "revision": 1,
    "created_at": 0,
    "updated_at": 0
  },
  "plan": {
    "plan_id": "cloud_plan_…",
    "goal_id": "cloud_goal_…",
    "status": "researching",
    "revision": 1,
    "created_at": 0,
    "updated_at": 0
  }
}
```

Clients must not put AWS keys, GitHub credentials, model tokens, or pairing
codes in `goal`. The later secure bootstrap channel uploads client-encrypted
material directly to the AWS Connection Stack KMS/Secrets Manager path and
returns only `secret_ref` values to ProductCore.

`cloud.plans.confirmation.prepare` accepts exactly:

```json
{
  "plan_id": "cloud_plan_…",
  "expected_revision": 3,
  "quote_id": "quote_…",
  "candidate_tier": "recommended",
  "idempotency_key": "UUID"
}
```

It accepts only `economy`, `recommended`, or `performance`; the tier's
architecture/CPU/memory/GPU/disk capacity must satisfy the persisted Recipe.
This first confirmation transition accepts only On-Demand candidates; Spot is
rejected until a separate Recipe checkpoint/resume/interruption contract is
implemented and tested.
The returned `confirmation` contains the immutable Plan and unsigned
`ApprovalV1`; its expiry is at most five minutes and never later than the
quote's `valid_until`.

`cloud.plans.approve` accepts exactly:

```json
{
  "plan_id": "cloud_plan_…",
  "expected_revision": 4,
  "approval": { "schema_version": 1, "approval_id": "…", "signature": "…" },
  "idempotency_key": "UUID"
}
```

The complete `approval` object must be the previously returned challenge with
only its device signature added. Reusing an idempotency key for a different
prepare or approval request returns `409 cloud_idempotency_conflict`; stale
revision, expired quote/challenge, and invalid signature fail closed. The
approval response omits the challenge and signature and reports only safe
Plan, Deployment, and Job summaries.

## State and event rules

Plan states are fixed as:

`researching → quoting → ready_for_confirmation → approved | expired | superseded`

Execution, outcome, resource, service, and integration remain separate axes;
the initial schemas intentionally do not collapse them into a single status.
Each Cloud entity owns a positive monotonic `revision`. ProductCore event
types are `cloud.goal.changed`, `cloud.plan.changed`, `cloud.job.changed`,
`cloud.deployment.changed`, `cloud.service.changed`,
`cloud.integration.changed`, `cloud.connection.changed`, and
`cloud.alert.raised`. Clients ignore duplicate/older revisions and refresh
only the affected entity after a revision gap or cursor reset.

`p2p_events` remains only the websocket projection. It is not the Cloud
ordering authority; `p2p_cloud_events` records aggregate revision facts and
is available to the control plane after restarts.

The independent Orchestrator writes only `p2p_cloud_events` and
`p2p_cloud_projection_outbox` in its fenced transaction. The Message Server
owns the relay to `p2p_events`: it claims one projection with a lease, decodes
only fixed `cloud.goal.changed`, `cloud.plan.changed`, `cloud.job.changed`, and
`cloud.deployment.changed` schemas, and calls its local events module with
`dedupe_key=cloud-event:<cloud_event_id>`. It acknowledges only after that
append. A crash between append and acknowledgement is therefore safe to replay
without duplicating an owner event. Unknown types, extra fields, malformed
JSON, credential-shaped text, raw Worker logs, Goal prompts, and secret
material are terminally rejected from the websocket projection.

Research and quote progress are Job axes, not guesses derived from Plan status.
The initial research Job is `research_queued`; a fenced claim records
`research_leased`, a classified retry records `research_retry_scheduled`, and
a successful draft records `research_ready`. Its successor quote Job moves
through `quote_queued`, `quote_leased`, `quote_retry_scheduled`,
`quote_command_expired`, `quote_ready`, or `quote_failed`. Only these
de-secretsed checkpoint and error-code values enter Job events/status; raw
provider replies, prompts, Broker errors, and logs stay private. A failed
research Job may leave its Plan at `researching`; a failed quote Job leaves the
Plan at `quoting` without an approval surface.

`cloud-orchestrator` uses a bounded attempt timeout shorter than each lease;
it defers a timed-out research or quote attempt rather than accepting a late
result. It reads its PostgreSQL URL only from the regular file named by
`CLOUD_ORCHESTRATOR_DATABASE_URL_FILE`, never a CLI flag or log line. Its
research endpoint is `CLOUD_ORCHESTRATOR_RESEARCHER_URL` and must be exact
HTTPS `/v2/cloud-research` with no user info, query, fragment, or redirects.
It also reads exactly one PKCS#8 Ed25519 node signing key from the regular
mounted file named by `CLOUD_ORCHESTRATOR_NODE_SIGNING_KEY_FILE`. The key is
used only to sign the fixed Connection Stack V2 `quote.request` and
`connection.registration.verify` envelopes; there is no generic AWS request
path.

It also requires a dedicated mounted mTLS CA, client certificate, client key,
and expected server name (`CLOUD_ORCHESTRATOR_RESEARCHER_CA_FILE`,
`CLOUD_ORCHESTRATOR_RESEARCHER_CERT_FILE`,
`CLOUD_ORCHESTRATOR_RESEARCHER_KEY_FILE`, and
`CLOUD_ORCHESTRATOR_RESEARCHER_SERVER_NAME`). Its transport disables proxy
use, requires TLS 1.3, and rejects a researcher certificate that does not
match the configured name.

The exact V2 Broker client independently rejects proxies, redirects, non-HTTPS
or non-`/v2/commands` endpoints, TLS below 1.2, unexpected JSON, oversized
responses, response/receipt mismatches, and any returned quote/registration
attestation that is not bound to the persisted signature-base `request_sha256`.
It accepts no action other than `quote.request` and
`connection.registration.verify`.

`p2p/cmd/cloud-researcher` is the corresponding independently deployable,
non-root private model boundary. It listens only with TLS 1.3 mutual
authentication and requires a mounted server certificate/key, trusted client
CA, exact OpenAI-compatible endpoint/model identifier, and a regular mounted
model-key file. The model key is read only by this process; it is not accepted
as a command argument, sent to the Orchestrator, stored in PostgreSQL, or
included in ProductCore/Matrix events, logs, errors, or recipes. Its default
model HTTP transport disables environment proxies and redirects. The current
model-assisted proposal remains `experimental`: typed validation checks the
contract shape and secret guardrails, but does not independently verify an
official source, signed artifact, AWS availability, or account-specific price.
Those checks are prerequisites for any later approval or typed provider
mutation.

The repository includes `Dockerfile.cloud-orchestrator`, a distinct non-root
image that contains only this binary and CA certificates; it must be given a
read-only root filesystem, its DSN and node-signing-key secret files, and no
message-server volumes, Docker socket, AWS credentials, Matrix configuration,
or Agent data.
`Dockerfile.cloud-researcher` is likewise a non-root image and must receive
only its read-only mTLS/model-key mounts, not the Orchestrator DSN, Message
Server data, AWS credentials, Docker socket, or Worker material.

## Approval and lifecycle gate

The domain contract package now defines deterministic-CBOR `plan_hash` and a
signed challenge that bind all of:

`plan_hash + revision + quote_id + cloud_connection_id + recipe_digest + resource/network/secret/integration scope + expiry`.

The ProductCore prepare/approve actions now use the active Connection's
device-key registry and a persisted one-time challenge. They bind the exact
canonical signing payload before the provision intent becomes visible. Dart
golden-vector verification and the later typed Worker/Broker create command
remain release gates for any actual provider mutation.

When the typed Worker creation executor is enabled, the UI label is
**“确认创建并开始计费”**. Before that executor exists, the current confirmation
surface must say that it records an approved deployment intent and creates no
resource or billing. Price and budget fields remain estimates/alerts: they do
not promise an AWS billing hard stop. Failure, cancellation, successful
installation, and `waiting_user_pairing` retain resources until the owner
explicitly plans and approves a verified destroy. Public ingress remains a
separate plan and confirmation.

## Explicitly not enabled yet

The current slice does not upload credentials, deploy a Connection Stack on the
owner's behalf, create an EC2 instance, install a service, expose a network
endpoint, or destroy a resource. It can issue a reviewed CloudFormation handoff,
verify a user-deployed Stack, obtain a read-only quote, and persist an approved
provision intent; it does not execute AWS mutations. It ships independently
buildable research/quote/registration processes and their private event relay,
but does not yet deploy a researcher endpoint, Worker AMI, Broker executor, or
AWS integration test. Those transitions must be implemented through the typed
Connection Stack/Broker path; neither the Eino Agent tool, external MCP, nor
the message-server gains arbitrary AWS access.
