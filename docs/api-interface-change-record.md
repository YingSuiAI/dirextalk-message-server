# API Interface Change Record

## 2026-07-19 New-Device Unread Recovery Read Markers

`sync.bootstrap` now includes additive metadata-only `read_markers`. The field is always an array, including when empty, and is ordered by `room_id` for a stable snapshot:

```json
{
  "read_markers": [
    {
      "room_id": "!room:example.com",
      "event_id": "$event",
      "origin_server_ts": 1784426400000
    }
  ]
}
```

These records are recovery boundaries only. They do not contain message content, media metadata, senders, or timeline events. Clients continue to obtain unread counts, receipts, messages, media, and paginated history from Matrix Client-Server APIs.

`sync.read_marker` and `channels.read_marker` now resolve `event_id` to the
server-owned Matrix timeline position for the supplied `room_id` and advance
the durable marker only when that topological position is newer. The optional
request `origin_server_ts` is non-authoritative; the bootstrap record uses the
resolved event timestamp. Equal, missing, invalid, or skewed timestamps
therefore cannot pin or regress the boundary, while delayed and repeated
requests remain successful no-ops. Resolution is bound to the authenticated
owner MXID and uses the existing Matrix history-visibility and local-hidden
event access checks. An event that is absent, belongs to another room, or is
not visible to that owner is rejected with the same non-leaking validation
error. The same monotonic tuple-CAS rule applies to the in-memory store and
PostgreSQL, and durable markers remain available through `sync.bootstrap`
after a server restart.
## 2026-07-19: signed private Worker-control endpoint scope

The existing Agent Cloud v2 plan/quote/approval service-operation scope now
accepts the additive `worker_control` private Interface endpoint without a
schema-version change. Its exact operation key is
`worker-worker-control-interface`; it is paired with the ordered
`worker-s3-gateway` Gateway and `worker-secretsmanager-interface` Interface
operations when `private_connectivity=no_nat_endpoints_v1`.

The Worker-control operation is fixed to `ap-northeast-3`, private DNS,
`endpoint_dedicated_from_worker`, the canonical
`grpcs://worker-control.y1.dirextalk.ai:443` target, and the
operator-frozen `com.amazonaws.vpce.ap-northeast-3.vpce-svc-<17 lowercase hex>`
service identity. Message Server forwards and revalidates endpoint type,
service identity, route/control-plane scope, and the exact two-Interface
usage total (currently 1460 endpoint-hours and 2 MiB/month). Unknown,
duplicate, omitted, or price/identity-drifted entries fail closed. Historical
v1 and pre-private-connectivity v2 plans retain their original decoding and
validation behavior.

No new public ProductCore, MCP, authorization, Worker credential, or prompt
surface is introduced.

## 2026-07-18: Agent-owned immutable runtime model profile

Added `agent.runtime.profile.get` and `agent.runtime.profile.update` as
owner-authenticated, HTTP-only ProductCore actions. Every response path is
non-cacheable with `Cache-Control: no-store` and `Pragma: no-cache`; the
actions are unavailable through realtime WebSocket and to `agent_token`.

Get accepts no parameters. Update accepts exactly `idempotency_key` as a
canonical UUID, `profile_id`, and nonnegative `expected_revision`, with only
optional `temperature`, `top_p`, and `max_output_tokens`. Provider, model,
base URL, owner identity, credential/secret fields or references, nested model
profiles, and unknown fields are not representable as mutation intent.

Both actions return the exact de-secreted
`{available, configured, revision, available_profile_ids, profile}` shape.
`profile` is either `null` or exactly
`{profile_id, provider, model, base_url, temperature, top_p,
max_output_tokens, context_window, reasoning_effort}`. The response contains
no Agent owner, service-key status, secret reference, credential-presence bit,
or API key. The immutable profile catalog is owned by the independent Agent
deployment; clients select a public profile ID rather than constructing
provider credentials.

Remote `agent.chat` rejects the legacy request-scoped `model_profile_id` and
`model_profile` fields. The Agent service key must authorize `runtime.read`,
`runtime.write`, and `runtime.chat`, while the model credential reaches Agent
only through an operator-provisioned read-only mounted secret file. On the
first selection, Message Server uses the explicit first-validation runtime
baseline of 48 context messages, 12 memory messages, and 24 steps; later
profile changes preserve the Agent-owned non-profile configuration.

After an ambiguous update result, clients reconcile through
`agent.runtime.profile.get` and its current revision/state. They must not
rebuild and blindly replay a mutation from a stale revision. This entry
supersedes the earlier current-client request-local model-profile contract for
the independent Agent path; the existing local Runner may retain that behavior
only as compatibility while it remains supported.

## 2026-07-18: fail-closed direct Cloud cutover preflight

Added `cloud.cutover.preflight` as an owner-authenticated, read-only
ProductCore action available through the existing HTTP/query and owner
realtime request paths. It accepts no parameters and returns only the exact
`{ready, blocked, reason, count}` status. It never returns Cloud resource IDs,
credentials, event bodies, logs, or storage/provider error details.

The Message Server checks every legacy local Cloud fact family exposed by the
compatibility store before a future direct cutover: goals, plans, jobs,
connections, deployments, services, recipes, alerts, and bounded legacy-event
presence. It also makes an existence-only read over every private durable
legacy Cloud table, including bootstrap, outbox, command, approval, lifecycle,
and operational facts that may not have a public projection yet. Any
Connection, Deployment, or Service is reported as
`legacy_cloud_active_resources_present`; other retained data is
`legacy_cloud_data_present`. A missing store, incomplete read, schema mismatch,
or unknown read failure instead returns the redacted
`legacy_cloud_read_failed` blocked result, not a 5xx. The event check is
intentionally bounded, so a blocked `count` is a safe observed lower bound
rather than an unbounded history scan; data/active reasons have a positive
count, while a read failure has count zero.

This action performs no migration, deletion, mutation, or cutover. Flutter
uses the normal Message Server ProductCore client only, displays the status as
read-only, and treats an unavailable or malformed response as blocked.

## 2026-07-17: owner-only on-demand encrypted deployment pairing payload

- Added `cloud.deployments.pairing.payload.retrieve` as an owner-authenticated,
  HTTP-only ProductCore action. The request binds one canonical Agent
  deployment to a caller-generated one-time X25519 public key and UUID
  idempotency key. Message Server resolves the current owner-bound pairing ID
  and revision from Agent rather than accepting either from the client.
- The response contains only the Agent's authenticated ciphertext envelope and
  minimal pairing status metadata, including the exact pre-mutation payload
  scope revision needed to validate the authenticated data. `payload_digest`
  is the deterministic envelope-and-AAD binding digest, never a digest of the
  pairing plaintext. Message Server does not persist, publish, enqueue,
  project, or send the envelope over realtime WS/Matrix paths.
- Every response path, including authentication and action errors, uses
  `Cache-Control: no-store` and `Pragma: no-cache`. The existing
  `cloud.deployments.pairing.resume` public request and response shape remains
  unchanged.

Last updated: 2026-07-17

## 2026-07-17 Agent-owned AWS Foundation lifecycle façade

Three additive owner HTTP-only ProductCore actions expose the independent
Agent's device-approved Foundation lifecycle:
`cloud.connections.foundation.confirmation.prepare`,
`cloud.connections.foundation.approve`, and
`cloud.connections.foundation.operations.get`. All three responses are
`no-store`; WebSocket and Agent tokens cannot call them.

Prepare returns the complete Agent-authored signing scope and exact CBOR bytes.
The scope binds owner, Connection and bootstrap revisions, action, observed AWS
identity, Foundation template digest, and the fixed private release environment.
Approve forwards only this bound device approval and reconciles an ambiguous
response through the exact owner-scoped operation ID. Get exposes only the
public operation status/read-back. Message Server neither persists nor
decrypts bootstrap credentials and has no AWS provider capability.

The existing credential and identity actions gain strictly disjoint Foundation
request shapes. Establish derives its target and Region from the existing
owner-scoped Role Plan and accepts `bootstrap_id`, `expected_revision`,
`lifecycle_action=establish`, plus `idempotency_key` for session creation (or
`session_id` and `expected_session_revision` for identity preview). Upgrade,
teardown, and destroy-blocked remediation instead accept the closed
`lifecycle_action`, `cloud_connection_id`, and
`expected_connection_revision`; Message Server reads that exact owner-scoped
Agent Connection before and after the remote call and derives target, account,
and Region from it. Mixing either lifecycle shape with the other shape is
rejected. The original Role Plan request without `lifecycle_action` remains
compatible and continues to create only an `aws_connection` session; it cannot
be replayed as a Foundation lifecycle session.

Connection read-back additionally recognizes `tearing_down` as the pending
state created atomically by teardown approval. It remains visible to clients
but is not treated as an active Connection and cannot start another lifecycle
bootstrap. Operation read-back also recognizes terminal
`failed_terminal`/`fresh_bootstrap_required`: clients must upload fresh admin
credentials and create a new approval rather than retrying the consumed or
expired authorization.

## 2026-07-17 Agent Deployment health summary

The owner-readable `cloud.deployments.list/get` ProductCore projection gains an
additive optional `health` object when an independent Agent health monitor has
a summary. It contains only aggregate status, its independent monotonic
revision, observed/next-due timestamps, per-kind counts, an evidence digest,
and evidence type. It deliberately excludes probe targets, URLs, headers,
request/response content, pairing material, and secret references.

The Deployment's existing revision remains unchanged by a health-only update;
clients merge the nested health revision independently and ignore stale health
summaries. Absent `health` remains compatible with older Agent deployments and
means no independently persisted health summary is available.

## 2026-07-16 Agent-owned manual Deployment destruction façade

For canonical lowercase UUID Deployment IDs, the existing owner HTTP-only
`cloud.deployments.destroy.plan/approve` actions now delegate exclusively to
the independent Agent. Legacy/local Deployment IDs retain their prior store
contract. Agent-mode prepare additionally requires `signer_key_id` and returns
the existing `confirmation` envelope with the current Deployment, an Agent
destroy-approval descriptor, and its exact deterministic-CBOR signing bytes
and digest.

The signed scope binds the Agent instance, owner, Deployment revision, Task,
Plan hash, Connection, and the normalized complete EC2/EBS/ENI/security-group
resource graph, including dependencies, retention, destroy deadline, original
approval, and provider read-back evidence. Message Server receives neither AWS
credentials nor an arbitrary destroy target.

Approve projects the Agent's durable `CloudDestroyOperation` into the existing
`{deployment, job}` response; it does not persist or manufacture a local Cloud
fact. Ambiguous responses are reconciled by operation ID and Deployment read-
back. Only an Agent operation plus Deployment read-back can yield
`verified_destroyed`; `destroy_blocked` remains a failed destroy Job and may
temporarily accompany an older Deployment projection while that projection is
refreshed. Both actions are non-cacheable and remain unavailable to Agent/MCP
tokens and websocket mutation.

The Agent may automatically retry the exact approved resource graph at most
three times with durable exponential backoff. A terminal `destroy_blocked`
operation is never polled again: its read-back sets `requires_new_approval`, so
remediation must create a new current-scope challenge and device signature.

## 2026-07-16 Agent Approval-v1 and AWS Connection establishment

When the independent Agent backend is enabled, root credential onboarding now
uses a canonical lowercase UUID as its cloud connection target. The existing
legacy Connection Stack identifiers and response shapes remain unchanged when
that backend is disabled.

The owner-only HTTP action `cloud.plans.confirmation.prepare` additionally
requires `signer_key_id` in Agent mode. Message Server reads the current Agent
Plan over typed gRPC, binds the configured owner, exact revision, quote,
candidate and every approval scope, and returns the Agent-owned unsigned
Approval-v1 descriptor plus its exact deterministic-CBOR signing bytes. The
client signs those bytes with its Ed25519 approval key. `cloud.plans.approve`
returns only `{plan, submission_status: "waiting_connection"}` after the
Agent's durable approved Plan is read back; it never fabricates a Deployment
or Job when the post-approval launcher is not ready.

In Agent mode, `cloud.connections.registration.complete` accepts exactly
`bootstrap_id`, `expected_revision`, `session_id`,
`expected_session_revision`, `plan_id`, `expected_plan_revision`, `approval`,
and `idempotency_key`. Message Server reloads the owner Role Plan before and
after the call, binds its device key, Connection UUID and Region to the
approved Plan, then calls only the typed Agent establishment RPC. A lost or
ambiguous response is reconciled through Agent `GetCloudConnection`; absent a
verified fact, the response is `pending_reconciliation`, never `active` or
failed. The normal `cloud.connections.get` query also reads that owner-bound
Agent fact so reconnects can recover after a short challenge or Role Plan has
expired. Only an independently read-back `active` status completes onboarding.

Confirmation, approval, and registration responses are non-cacheable. Service
Keys provide transport identity only: approval-device administration, raw
secret retrieval and arbitrary AWS operations remain unavailable through this
façade.

## 2026-07-16 Agent-backed AWS identity preview

The owner-authenticated, HTTP-only
`cloud.connections.identity.preview` action accepts exactly `bootstrap_id`,
the durable Role Plan `expected_revision`, `session_id`, and
`expected_session_revision`. Message Server reloads the owner-scoped Role Plan
before and after the Agent call and derives the AWS Region and cloud connection
target from that record; the client cannot provide or override owner, target,
or Region.

When the remote Agent backend is enabled, the action performs only the Agent's
read-only STS caller-identity inspection for an already uploaded encrypted
bootstrap session. It validates the returned persistent evidence binding for
session, revision, Agent owner, connection target, Region and validity window,
then returns `identity`, `cloud_connection_id`, `bootstrap_session_id`,
`session_revision`, `verification_status=identity_verified`, `observed_at`,
and `expires_at`. It does not create or list an active Cloud Connection and it
does not consume the uploaded secret. Responses are non-cacheable; Agent
tokens and realtime WS requests cannot invoke the action. With the legacy
local backend, the action is present but returns a stable unavailable error and
does not change the existing registration flow.

## 2026-07-16 Immutable Connection Stack template contract

`cloud.connections.role_plan` and the dedicated credential-bootstrap request
now carry the closed `connection_template` union. A normal role plan contains
only a version-pinned S3 binding; a rootkey-enabled role plan contains only the
reviewed publish intent and therefore has no CloudFormation handoff URL. The
legacy `template_url` and `template_digest` fields remain derived display
values only for an S3 binding, and every client/server parser rejects a missing
or mismatched typed reference rather than treating either legacy field as an
execution authority.

Server startup now accepts only
`P2P_CLOUD_CONNECTION_TEMPLATE_JSON`; the prior URL/digest environment
variables fail closed even when present but empty. Flutter persists the same
closed union, clears old cached plans, and never creates an external
CloudFormation URL for a publish intent. This does not add any Agent/MCP
mutation permission or expose AWS credentials.

The independent Go Connection Stack now binds an immutable Broker ZIP object
version and a Foundation-owned private Worker security group. If a CreateStack
response is lost, it accepts the deterministic stack only after exact
CloudFormation parameter, tag, ARN and original-template read-back. This is an
internal controller contract; real AWS deployment remains disabled pending the
separately tracked root-bootstrap resolver and isolated staging validation.

## 2026-07-16 Restricted AWS rootkey bootstrap

`cloud.connections.role_plan` now accepts the optional boolean
`allow_root_credential_bootstrap`, defaulting to `false`. The value is a
non-secret, owner-selected capability of that exact immutable Role Plan and is
included in its request digest/idempotency replay contract; changing it under
the same idempotency key conflicts rather than widening an existing plan.

The existing owner HTTP-only
`cloud.connections.credential_bootstrap.create` action derives a ten-minute,
single-use X25519/AES-GCM upload session from the matching
`awaiting_stack` Role Plan and expected revision. The CSV plaintext goes
directly from the client to the Go Connection Stack controller, which can call
only the fixed `CreateStack` path and clears credential buffers after use.
Session URL/bearer, AWS credentials, caller ARN and receipt internals do not
enter ProductCore persistence, events, logs, Agent, MCP or Worker inputs.

An AWS root access key is accepted only when the exact server-issued Role Plan
has that capability bit. A root upload on a default or stale plan is rejected;
the upload remains one-time and restart/expiry requires a new session. This
supersedes earlier onboarding notes that described the then-current
CloudFormation-only flow as having no credential upload. It does not enable
EC2 deployment, public ingress, arbitrary AWS API calls, or Agent/MCP cloud
mutation.

## 2026-07-15 First-validation Cloud artifact and recovery closure

The internal Connection Stack contract now has a signed `artifact.put`
prepare/complete flow for dynamic Recipe artifacts. Completion binds and
read-backs the exact versioned S3 `versionId`, archive checksum, size, media
type and KMS encryption before the Stack records `verified`; later reads remain
version-pinned. This is an internal Orchestrator/Stack/Worker contract and adds
no ProductCore, Agent or public MCP artifact-write action.

Go-only Worker archive and AMI builders now validate a uniquely versioned
prerelease artifact, measured Worker/catalog digests, a digest-pinned OCI
source, private networking, IMDSv2 and encrypted image/snapshot read-back. The
compiler-owned OCI runtime profile carries typed entrypoint/argv, run-as,
bounded tmpfs, storage and secret-environment bindings; OpenClaw and Hermes are
contract fixtures only. The unique local validation artifact
`v1.1.0-cloud-mvp.20260715.1` has been built and its pinned OpenClaw health
contract exercised; it has not been registered in S3, assembled into a dynamic
AMI or deployed in the real test account. Release/deployer scripts are not part
of this path.

`cloud.deployments.pairing.resume` is now an owner-authenticated HTTP-only
two-phase transition. Without `approval` it prepares a five-minute challenge
for the exact `waiting_user_pairing` Deployment revision; with the registered
device signature it atomically requeues only that Deployment, its existing
install Job and a private resume intent. It accepts no pairing URL, code or
secret. Flutter signs the same deterministic payload, validates returned
revisions and applies the authoritative mutation result before normal realtime
reconciliation.

Cloud projection recovery now preserves newer Flutter state when a reconnect
bootstrap races realtime events and refreshes the Cloud projection on cursor or
entity-revision gaps. The Orchestrator also has a persistent lease/generation-
fenced Service monitor that survives restart and raises or recovers only its
own semantic-health degradation. A stateful fake provider exercises approval,
provision, install, readiness and verified destruction as one durable
lifecycle; this does not enable real AWS mutation.

Restricted Eino Cloud status results add de-secreted `client_deep_link` and
deterministic `next_step` guidance for Plan, Job, Deployment, Service and Alert
state. `destroy_blocked`, `blocked` and `orphaned` guidance says resources may
still incur charges and directs every retry to the owner HTTP/device-signature
flow. The optional private Recipe id/revision remains client-bound outside tool
arguments. Agent and MCP still have no purchase, pairing, lifecycle, secret or
destroy mutation capability.

Two owner-authenticated HTTP-only actions now prepare and consume a device-
signed Job cancellation: `cloud.jobs.cancel.plan` and
`cloud.jobs.cancel.approve`. Only deployment-bound provision/install/verify
Jobs with pending outcomes are eligible. Cancellation fences private tasks,
outbox work and late results, publishes `job_canceled`, and moves any
discovered active resource to `retained_tracked`; it never calls AWS stop or
destroy.

Two further owner HTTP-only actions cover residual infrastructure when no
Service exists: `cloud.deployments.destroy.plan` and
`cloud.deployments.destroy.approve`. The five-minute approval is derived from
the current private ledger and binds Deployment revision, Plan, Connection,
resource status, exact EC2/EBS/ENI identifiers and secret refs. It accepts
`active`, `retained_tracked`, `blocked` and `orphaned`, creates a separate
destroy Job, and uses the typed `deployment.destroy` Connection Stack
provider. The Stack accepts the new `deployment_destroy` proof without a
synthetic `service_id` while preserving the legacy Service-destroy proof. Only
provider `NotFound` read-back yields `verified_destroyed`; errors remain
`blocked`, and the original Deployment execution/outcome is preserved.

Flutter signs both contracts with the system approval key, keeps them HTTP-
only, displays continued-billing/read-back warnings, and exposes residual
Deployment controls at `/agent/workloads/deployments/:id`. Agent and public
MCP remain read-only and cannot call either mutation. Rootkey bootstrap and
verified `a8.dirextalk.ai` SSH are available only through the constrained owner
path above; real AWS validation still needs the prerelease Worker artifact/AMI,
deployed Connection Stack controller and disposable cleanup/read-back path.
Immediately before any billable create, the owner must confirm the latest
Region, instance/disk specification and live quote.

## 2026-07-15 Flutter Device-Signed Deployment Intent

Flutter now consumes the existing owner HTTP-only
`cloud.plans.confirmation.prepare` and `cloud.plans.approve` actions. The Plan
page renders the immutable QuoteV1 candidates and opens a separate review
sheet; no Plan-page tap implicitly approves a purchase.

The client validates the returned Plan/quote/device binding and the complete
On-Demand, no-public-ingress, no-secret and no-integration scope before signing
the deterministic-CBOR ApprovalV1 challenge with its secure-storage Ed25519
identity. Its CBOR output matches the Go golden vector. Only the unsigned
short-lived challenge and UUID idempotency keys are resumable; after a lost
response or restart the exact same signature is replayed, and the signed
approval is redacted from diagnostics.

The current independent Cloud Orchestrator deliberately does not claim the
provision outbox. Therefore this client surface says that it records an
approved queued intent without creating AWS resources or starting billing.
The “确认创建并开始计费” wording remains reserved for a later build in which the
typed provision executor is explicitly enabled.

## 2026-07-15 Flutter AWS Connection Stack Onboarding

Flutter now consumes the existing owner HTTP-only
`cloud.connections.role_plan` and
`cloud.connections.registration.complete` actions through a resumable AWS
Connection Sheet. It creates one persistent Ed25519 approval identity in system
secure storage, exports only its stable id and RFC 8410 SPKI public key, and
fails closed if the stored private seed is corrupted.

The client persists only safe Role Plan/idempotency metadata, opens the exact
CloudFormation Quick Create handoff for the selected AWS partition, and submits
Broker URL plus Stack ARN only for independent verification. Those two Stack
outputs are not cached and are redacted from client diagnostics. This flow has
no AK/SK upload and does not create EC2, activate a Connection, or begin
billing.

## 2026-07-15 Cloud Agent Workload Navigation Summary

Restricted `cloud_dialogue_mode=true` Native Agent planning may now attach an
optional, additive `cloud_workload` object to direct `agent.chat` results and
the final `agent.chat.stream` `done` event. It has the fixed
`dirextalk.cloud-agent-workload/v1` schema and only `plan_id`, `goal_id`, Plan
`status`, and Plan `revision`.

The server derives the object solely from a successful, internally consistent
typed Cloud planner Goal/Plan result. It never parses model prose or traces and
omits the object for a normal Agent chat, status read, failed/malformed result,
unrecognized fields, or a second distinct research goal in one restricted
request. It contains no goal prompt, Connection/account/region, quote, recipe,
secret, receipt, endpoint, log, Worker, billing, readiness, or lifecycle fact.
Clients must treat unknown or invalid objects as absent and use only `plan_id`
to open the existing Plan detail page.

## 2026-07-15 Recipe Execution Confirmation Boundary

Added owner HTTP-only
`cloud.deployments.recipe_execution.confirmation.prepare` and
`cloud.deployments.recipe_execution.approve` actions. They accept only a
deployment ID, expected revision, UUID idempotency key, and, for approval, an
exact signed device challenge; clients cannot provide an execution manifest,
artifact, command, URL, secret, or Worker payload.

A private registrar first verifies and persists a trusted manifest bound to the
current approved Plan, active deployment resource, Broker Worker manifest
digest, and active Worker observation. Approval then creates only a queued
`install` Job/Step and a private outbox record containing the opaque
execution ID. No current consumer can turn that intent into a Worker task,
root execution, AWS mutation, or service readiness.

## 2026-07-15 Sealed Recipe Execution Foundation

Added internal `RecipeExecutionManifestV1` deterministic-CBOR artifacts and a
`cloudworker/recipeexec` coordinator. The manifest binds one execution and
deployment to an immutable Plan revision/hash, Recipe digest, Worker resource
manifest digest, locked compiled-artifact digest, opaque action identifier,
timeout, ordered recovery checkpoints, and opaque volume/data/secret slots.
It rejects command text, URLs, file paths, raw credentials, invalid references,
and secret slots that are outside the reviewed Plan.

The coordinator accepts only a trusted artifact resolver, durable
compare-and-swap checkpoint store, and idempotent action driver. It requires
the returned artifact digest and action declaration to match the manifest,
resumes only from the exact next checkpoint, and does not replay an action
after a terminal checkpoint. It does not run a process, download a bundle,
retrieve a secret, call AWS, alter the existing `execution_probe` task, or
mean service readiness. Its later persistent confirmation consumer remains
unable to execute the coordinator or issue a Worker task.

## 2026-07-14 Device-Signed Cloud Plan Confirmation

`cloud.plans.confirmation.prepare` and `cloud.plans.approve` are now owner
HTTP-only ProductCore actions. They are unavailable to `agent_token`, `/mcp`,
and realtime `client.request`.

Preparation binds a `quoting` Plan's expected revision, immutable verified
quote, selected `economy`/`recommended`/`performance` tier, Recipe resource
requirements, and active Connection device key into canonical `PlanV1`. Quote
candidates now carry architecture, vCPU, memory, GPU count, and total GPU
memory, so confirmation rejects insufficient capacity rather than inferring it
from an instance-type name. The initial Plan scope is fixed to no public
ingress, no secret delivery, and no integration. The response returns only the
reviewable Plan and an unsigned challenge; its expiry is at most five minutes
and never outlives the quote.

Approval requires the exact stored challenge with a valid Flutter Ed25519
signature. A single PostgreSQL transaction marks the Plan `approved`, creates
a queued/pending Deployment and `provision` Job/Step, records a private
`cloud.deployment.provision.requested` outbox row, and emits safe
`cloud.plan.changed`, `cloud.deployment.changed`, and `cloud.job.changed`
projections. Idempotency keys are owner-scoped and reject different request
shapes, including concurrent cross-Plan collisions. Neither the approval
signature nor private outbox data appears in a response, audit event, realtime
projection, Agent input, or MCP result.

This ProductCore action is an approved durable intent, not an AWS mutation: it
creates no EC2/EBS/ENI, ingress rule, or billable resource. The independent Go
Stack now implements the only permitted typed `deployment.create` path, but it
is disabled by default and ProductCore approval does not bypass or enable it.

If the challenge or bound quote expires, approval atomically marks the
challenge and Plan `expired`, emits only a safe Plan projection, and records
the idempotency outcome for retry; it leaves no queued Deployment or provision
outbox behind.

## 2026-07-14 User-Owned Connection Stack Registration

`cloud.connections.role_plan` is now an owner HTTP-only action. It accepts an
AWS Region, a Flutter device approval Ed25519 public key/key ID, and a UUID
idempotency key, then returns a short-lived safe CloudFormation Role Plan. The
plan exposes only its bootstrap/Connection IDs, template URL/digest, complete
source-tree digest, deterministic stack name, requested Region, expiration, and public node/device
key parameters. It neither accepts AWS credentials nor creates an active Cloud
Connection.

After the owner deploys the Stack in their AWS account,
`cloud.connections.registration.complete` is the second owner HTTP-only action.
It accepts the bootstrap ID, expected revision, UUID idempotency key, the exact
regional Broker API Gateway command URL, and the same-Region CloudFormation
Stack ARN. These two output values remain private control-plane material: the
response and all ProductCore/MCP/realtime/audit projections return only a safe
registration status/revision and Job ID. The action creates a standalone
`connection_registration` Job but does not activate a Connection.

The independent `cloud-orchestrator` persists and signs exactly one fixed
`connection.registration.verify` command before its first HTTPS request. It
replays that envelope after ambiguous failure and allocates a new counter only
after an exact Broker `expired_command` result. Broker proof must bind the
bootstrap, Connection ID, account, Region, Stack ARN, API endpoint, node
key/generation, command ID, and request digest. Only a fenced successful proof
atomically creates the public active Connection and safe
`cloud.connection.changed` projection; invalid or mismatched proof fails closed.

This remains an onboarding/attestation boundary only. It does not upload AWS
keys, create EC2/EBS/network resources, open ingress, approve spend, install a
Worker, or expose an AWS mutation action to Agent, MCP, or the Message Server.

## 2026-07-14 Cloud Research Draft And Signed Read-Only Quote

The private researcher result is now deliberately narrower: it returns only an
experimental `RecipeV1`, non-price `ResearchDraftV1`, title, and summary. It
cannot return a final Plan, Quote, price, approval, quote ID, or plan hash. The
fenced Orchestrator Store derives a `QuoteRequestV1`, moves the Plan from
`researching` to `quoting`, and creates a distinct quote Job/outbox record.

`p2p/cmd/cloud-orchestrator` now requires a mounted PKCS#8 Ed25519 node-key
file and runs a second bounded quote loop. The loop persists one exact signed
Connection Stack V2 `quote.request` envelope before the first HTTPS attempt;
ambiguous failures replay the same envelope/counter and only the Broker's
`expired_command` result permits a new one. The client accepts no AWS action
other than quote request, no generic provider endpoint, and no AWS SDK or CLI.

After a strictly bound Broker receipt, the Store persists a verified immutable
quote and keeps the Plan in `quoting` with its `quote_id`; it does not create a
final approval Plan or billable resource. `cloud.plans.get` gains an additive
de-secretsed nested `quote` projection. `cloud.bootstrap` and all list actions
remain summary-only. The projection relay now accepts the new de-secretsed
`quote` Job kind. This stage does not expose a create/approve/destroy action,
accept credentials, or run an AWS mutation.

## 2026-07-14 Private Cloud Researcher mTLS Boundary

The independent private `p2p/cmd/cloud-researcher` runtime and its non-root
`Dockerfile.cloud-researcher` image now implement the internal typed research
endpoint used by `cloud-orchestrator`. This is not a ProductCore action, MCP
tool, Matrix API, or user-facing secret-upload API. The Orchestrator now
requires a mounted mutual-TLS client identity and expected Researcher server
name; the Researcher requires TLS 1.3 with verified client certificates. A
model credential is accepted only from the Researcher's regular mounted file,
never an environment value, command flag, Agent request, event, or response.
The endpoint accepts one strict `ResearchInput` JSON shape and returns a
validated `ResearchOutput` shape at exact `/v2/cloud-research`; malformed,
oversized, secret-bearing, legacy-path, or untyped requests are rejected
without emitting their content.

This completes only the private research transport boundary. It does not
activate `cloud.plans.approve`, create a Connection Stack, provision an EC2
Worker, expose public ingress, use an AWS credential, or create billable
resources.

## 2026-07-14 Cloud Orchestrator Research Runtime And Projection Relay

The repository now builds the standalone `p2p/cmd/cloud-orchestrator` process
and the separate non-root `Dockerfile.cloud-orchestrator` image. The worker is
not part of the default Message Server compose service and has no Matrix
configuration, model/API key, AWS SDK, Docker socket, product migration, or
arbitrary provider API capability. It reads PostgreSQL configuration only from
the regular file named by `CLOUD_ORCHESTRATOR_DATABASE_URL_FILE`, requires an
exact configured HTTPS `CLOUD_ORCHESTRATOR_RESEARCHER_URL` at exactly
`/v2/cloud-research`, rejects redirects, and bounds each private research
attempt below its durable claim lease. This is a research/typed-contract
worker only; it does not create an AWS resource.

Cloud creation now atomically inserts a queued/pending `research_queued` Job
and Step plus owner-safe `p2p_cloud_projection_outbox` records for its initial
Goal, Plan, and Job audit events. A fenced standalone worker records the Job
lease, retry, success, or failure checkpoint in the same transaction as its
outbox transition, then writes Cloud events and projection records. The
Message Server, not the worker, relays those records to
the product event stream using `cloud-event:<cloud_event_id>` as the durable
dedupe key. The relay accepts only strict `cloud.goal.changed`,
`cloud.plan.changed`, and `cloud.job.changed` schemas; unknown/extra fields,
credential-shaped text, private Goal prompts, and raw Worker material never
reach WS or Matrix projections.

`cloud_dialogue_mode=true` now exposes one additional read-only Native Agent
tool, `native_agent_cloud_status`, beside the existing
`native_agent_cloud_deployment_plan`. It returns the same de-secretsed Cloud
snapshot as the owner bootstrap projection and rejects parameters. It does not
add a create, approve, secret, ingress, service-operation, or destroy path.

Research planning now requires an existing `cloud_connection_id`.
`cloud.goals.create` and the Eino tool reject an omitted value with
`400 cloud_connection_required` before durable insertion: current QuoteV1 and
PlanV1 contracts bind one Connection, so accepting an unbound request would
create a Job that no compliant researcher could settle. A future “plan first,
attach later” experience requires an explicit revisioned `waiting_connection`
state and attach action; it is not silently emulated by this contract.

## 2026-07-15 Cloud Dialogue Input And Connection Scope

`cloud_dialogue_mode=true` now rejects credential-shaped material in request
`prompt`, `message`, and every `messages[].content` or `messages[].text`
before it resolves a model profile or contacts a model provider. The rejection
is a fixed safe error and never echoes request content; it also prevents the
Cloud planner from receiving a Goal.

Cloud dialogue planning now receives a client-selected top-level
`cloud_connection_id` only through request context. The restricted
`native_agent_cloud_deployment_plan` schema accepts `goal` alone and rejects a
model-supplied Connection identifier. A status-only dialogue may omit a
selection, but a planning call without one is rejected before durable Goal
creation. Ordinary non-dialogue Native Agent and direct product planning
contracts remain unchanged.

`native_agent_cloud_status` no longer reuses the owner `cloud.bootstrap`
payload. Its model-facing DTO retains Goal/Plan/Job/Deployment/Service/Alert
progress and aggregate Connection state, but omits account IDs, regions,
Connection identifiers, private Goal prompts, artifact digests, Recipe data,
and alert messages. The owner ProductCore bootstrap remains unchanged.

## 2026-07-14 Restricted Cloud Native Agent Dialogue

Owner `agent.chat` and owner realtime Native Agent stream requests may set
`cloud_dialogue_mode=true` to enter a request-scoped capability-reduced Cloud
planning conversation. This is not an approval or mutation flag. The model
receives exactly one tool when the Cloud planner is configured:
`native_agent_cloud_deployment_plan` (and, after the relay update, the
read-only `native_agent_cloud_status`). The server excludes the runtime shell,
runtime CLI tools, external MCP tools, dynamic Skill/MCP management tools,
ordinary Dirextalk tools, installed Skill prompts, request/config system
prompts, and memory. The only resulting write remains the existing validated
connection-bound research Goal/queued Job; billing, ingress, secret delivery, approval, service operation,
and destroy remain outside the conversation and require their typed control
plane contracts.

The default model identifier for the `deepseek` provider is now
`deepseek-v4-pro`, matching the provider's current OpenAI-compatible Chat API.

## 2026-07-14 Cloud Orchestrator Control-Plane Foundation

Added the owner-only `cloud.*` ProductCore namespace. The read projection
actions are `cloud.bootstrap`, `cloud.connections.list/get`,
`cloud.plans.list/get`, `cloud.deployments.list/get`,
`cloud.services.list/get`, `cloud.recipes.list/get`, and
`cloud.events.list`; they allow owner HTTP fallback and ready realtime
`client.request`. `cloud.goals.create`, `cloud.connections.role_plan`,
`cloud.plans.approve`, `cloud.deployments.pairing.resume`,
`cloud.services.operation.plan/approve`, and
`cloud.services.destroy.plan/approve` are owner HTTP-only. ProductCore
`cloud.*` actions are never available to `agent_token`, `POST /mcp`, or
realtime `client.request`. `/mcp` only registers the separately named,
de-secretsed read tools `dirextalk_cloud_workloads_list`,
`dirextalk_cloud_workloads_get`, and `dirextalk_cloud_status`; it never
registers a Cloud mutation or secret tool.

The implemented first transition is `cloud.goals.create`. It accepts a UUID
`idempotency_key`, a 1–12,000-character `goal`, and a required existing
`cloud_connection_id`. It atomically persists a private Goal and a Plan in
`researching`, a queued/pending research Job/Step, three de-secretsed Cloud
audit events, and an outbox request for
the independently deployed Cloud Orchestrator. Repeating the same UUID and
same intent returns the original Goal/Plan; using that UUID for a different
intent returns `409 cloud_idempotency_conflict`. The raw idempotency key and
goal prompt are not in realtime events. The full contract and boundaries are
in `docs/cloud-orchestrator-mvp-contract.md`.

High-risk actions are intentionally declared but currently return
`503 cloud_orchestrator_unavailable`; they do not create AWS resources. The
Message Server does not accept AWS credentials or contain an AWS SDK/CLI path.
Future approval must bind a deterministic-CBOR plan hash, quote, connection,
recipe, resource/network/secret/integration scopes, expected revision, expiry,
and device signature before it can enqueue a typed Broker command.

## 2026-07-15 Device-approved verified Service destruction

`cloud.services.destroy.plan` now prepares an owner/device confirmation bound
to the current Service and Deployment revisions, Cloud Connection, Recipe
digest and the private tracked EC2/EBS/ENI set. `cloud.services.destroy.approve`
is idempotent and verifies that exact Ed25519 payload before atomically moving
the Service and resource axes to `destroying`, creating a destroy Job, and
queuing a private Orchestrator intent. Neither action is available through
Agent, MCP or realtime `client.request`.

The independent Orchestrator persists and exact-replays a node-signed
`deployment.destroy` command. The standalone Connection Stack consumes the
approval/challenge once, binds it to the original deployment receipt, deletes
only those identifiers, and returns `verified_destroyed` only after AWS
read-back proves absence. Product projections move to `destroyed` and
`verified_destroyed` only after the Orchestrator revalidates the persisted
receipt in its lease-fenced transaction. Unverified terminal failures become
`degraded`, `blocked`, and `destroy_blocked`. Both Orchestrator and Stack
executors are disabled by default; no credential upload or generic AWS API was
added.

The server-side Eino Native Agent now owns a private built-in Cloud Deployment
Planner skill and its `native_agent_cloud_deployment_plan` tool. It calls only
the narrow research-goal port and derives an internal UUID scoped to one Agent
chat to replay a model retry without deduplicating a later explicit task; it
is not a Codex workspace Skill, cannot call AWS or approve a plan, and is not
listed by external MCP. Its child runtime uses an isolated home and rejects
direct or common wrapped AWS CLI invocation. Both it and the owner HTTP action reject
credential-shaped goal content before any database/event/outbox write; a goal
may use a `secret_ref` placeholder only.
Last updated: 2026-07-19

## 2026-07-17 Native Agent Current User Context

Native Agent now identifies itself to the model as `Ying`. Every `agent.chat` and realtime Native Agent stream rebuilds a server-provided current Dirextalk user block in the system prompt from the authoritative owner user ID and latest profile nickname. The prompt block uses `user_id` and `nickname` terminology, treats the ID as authoritative and the nickname as untrusted display-only metadata, and is inserted before configured or request-scoped system prompts. Clients cannot submit or override either identity, and no request or response schema changes.

## 2026-07-17 Central Version Direct Upgrade Contract

`release.v2.status` and `release.v2.apply` are owner-token, HTTP-only ProductCore actions. They are not valid realtime `client.request` actions and `agent_token` is rejected. `release.v2.status` accepts no parameters and returns the local running `current_version`, current portal-device `client_version`, `available`, `updater_available`, `updater_ready`, `desired_state`, a token-free optional `active_job`, and sanitized `watchdog` status. It never performs GitHub release discovery, returns a release plan, or exposes an image, digest, command, path, plan token, or job bearer.

`release.v2.apply` accepts exactly `target_version`, lowercase canonical UUID `idempotency_key`, and `confirm="apply_release_change"`. `target_version` is canonical stable `vX.Y.Z`; image, digest, URL, plan token, shell, Compose, service, and all unknown fields are rejected. The message server first requires updater direct-release contract v2 and performs an atomic replay-only lookup for the supplied target/key. A replay miss is the only result that may continue toward new-job creation. Before creating a new job, the server queries the fixed central `appId=1&channelId=server` record, requires HTTP success plus business `code=0`, `appId="1"`, `channelId="server"`, canonical `version`/`preVersion`, an exact target match, and current reported client version at least `preVersion`. It sends the three public fields plus its authoritative current-device `client_version` to the updater's Unix control interface. The updater resolves the fixed `dirextalk/message-server:<target_version>` tag to an immutable registry digest and persists that digest before host mutation; it does not require GitHub Release assets. The middle-platform record is the version and client-compatibility authority. The owner portal device/generation is revalidated and serialized with portal session changes through the complete mutation, so an HTTP request authorized for an old device cannot create a job after a device switch.

Replay-only recovery works for active and terminal persisted jobs. A known key bound to the same target rotates and returns a replacement bearer ticket without consulting the mutable central record or creating a job; an unknown key returns `idempotency_not_found` and a target mismatch is rejected. New-job gates run only after that atomic miss, so an active-to-terminal or rollback transition between status and apply cannot turn recovery into an unchecked create. A different active target, maintenance state, unsupported updater contract, unavailable updater, unverified release edge/digest, schema mismatch, or incompatible client remains fail-closed. Failed central validation for new jobs returns `central_version_invalid`; a temporary central failure returns `central_version_unavailable`; incompatible client and changed-target failures are structured and create no updater job.

V1 release actions remain registered for existing clients. V2 exposes no discovery plan or rollback operation to ProductCore clients, while the updater internally verifies the canonical formal-release assets before accepting a central-selected target.

## 2026-07-17 Group And Channel Member Ordering

When the durable conversation projection contains the room creator's Matrix ID, `groups.members` and `channels.members` now place only that exact full MXID first and return only that member with `role: "owner"`. Other personal-node identities such as `@owner:another.example` remain ordinary members even though they share the `owner` localpart.

Remaining members are ordered by ascending persisted `joined_at`, with missing/zero timestamps last and the full Matrix `user_id` as the deterministic tie-breaker. Legacy or not-yet-hydrated rooms without a projected creator use that join order for the whole list rather than trusting a stored owner role. Clients should preserve the returned order and must not infer the room creator from the `@owner` localpart.

The creator projection is assigned only from the authoritative `m.room.create` sender when that raw sender is already a validated full MXID. Binding an existing `room_id`, the current node identity, later room-profile event senders, and the non-authoritative legacy `content.creator` field do not infer or replace the creator. Member reads lazily resolve current create-event sender IDs through the roomserver, persist the resolved MXID, and clear stale projected creators when current create state cannot identify one; unresolved rooms use the join-order fallback.

Matrix product-write policy uses the same create-sender authority for the privileged owner role. A member-policy `role` value cannot promote a non-creator to owner or demote the confirmed creator; member-policy mute state remains authoritative.

The `dirextalk_room_members_list` MCP tool applies the same rule once, after merging product projections with Matrix room-state members: exact creator first, then ascending `joined_at`, missing timestamps last, and full MXID as the tie-breaker.

## 2026-07-16 Native Agent Room And Post References

Successful non-stream `agent.chat` responses and realtime Native Agent stream `done` payloads may now include additive `references[]`. The server derives these references only from full, successful built-in Dirextalk tool results produced during that run; it does not parse the model's final Markdown or accept third-party MCP/runtime output as a navigation contract.

Contact list/search and room search results produce deduplicated room references. A messages-list result produces one reference for its containing room and does not expose or target a message `event_id`. Channel-post list results produce one post reference per valid post. References preserve tool/result order and use either `{kind:"room",room_id,room_type?,title?,preview?}` or `{kind:"channel_post",room_id,channel_id,post_id,title?,preview?}`. `room_type`, when present, is normalized to `direct`, `group`, or `channel`.

`mcp.channel_posts.list` and the embedded `dirextalk_channel_posts_list` result envelope now include top-level `channel_id` alongside `room_id`, `name`, `posts`, and pagination fields. This is additive and lets clients open an exact channel post without inferring channel identity from content.

## 2026-07-13 Join And Decision Recovery Contract

`groups.join`, `groups.invite.reject`, `contacts.request`, `contacts.requests.accept/reject`, `channels.join`, and `channels.join_request.approve/reject` now accept an optional `operation_id`. Old clients may omit it; the server derives and durably stores a stable ID for the current request/invite generation. Internal cross-node `channels.public.join_request/join_result` carry the same durable generation as optional `request_id`. Successful HTTP results expose additive top-level `operation_id` and `current_room_id`; successful WS results expose them inside `server.response.result`. These fields do not move into or replace the existing ProductCore `operation` object.

HTTP and WS error envelopes preserve the existing `code` field and add the same value as `error_code`, plus optional `operation_id` and `current_room_id`. Stable recovery codes are `request_not_found` (404), `request_expired` (410), `matrix_join_unconfirmed`, `join_result_unconfirmed`, `matrix_join_failed`, `operation_id_invalid` (400), `operation_id_conflict` (409), and `operation_recovery_failed`. The last code means a committed/recoverable operation still needs projection or persistence reconciliation; it is not limited to one database write.

Matrix `m.room.member membership=join` remains the final joined fact. A repeated mutation checks Matrix membership and repairs ProductCore member/contact/conversation/channel projections. It never treats an `already joined` error string alone as success, and ordinary group/channel invite or channel approval retries never kick a joined member merely to rebuild the flow. Direct-room creation uses a hashed internal idempotency key so an operation-state write failure or restart does not create another room.

`groups.invite` and `channels.invite` accept optional `rebuild_generation` only for an explicit retained-room empty-state rebuild. It is 1-128 characters from `[A-Za-z0-9._:-]`, and an explicit rebuild targets exactly one user. Old clients omit it: when Matrix confirms that the target is already joined, the server returns the current joined member and performs no kick, replacement invite, or `rooms.reactivate` callback. With a valid generation, the owner first calls the target node's `rooms.reactivate`; that node checks its own Matrix joined fact before changing ProductCore state and returns the same `rebuild_generation` plus `needs_rebuild=false,status=joined` or `needs_rebuild=true,status=invite`. The owner may kick and re-invite only after an exact generation echo with `needs_rebuild=true`. A missing, malformed, stale, or mismatched confirmation never authorizes the kick. The generation is also the canonical durable operation identity, so response loss, restart, or a different caller-supplied `operation_id` cannot repeat the kick. While that generation is workflow-busy or in flight, replay returns `status=joining,error_code=operation_recovery_failed` with the canonical operation and room IDs; the retained room's old Matrix join cannot be misreported as completed rebuild success. Only a completed cached operation whose member generation and current Matrix fact still match may return success. This additive recovery parameter does not require a Flutter client change.

Contact decision results are:

- accept confirmed: `200 status=accepted`;
- accept dispatched but not locally confirmed: `200 status=joining,error_code=matrix_join_unconfirmed`; the durable `joining` contact remains in `sync.bootstrap.contacts` and `pending.friend_requests` and may be retried with the same room/peer;
- reject a pending request: `200 status=rejected`;
- reject while current state is accepted: `200 status=accepted`;
- reject while current state is joining: `200 status=joining,error_code=matrix_join_unconfirmed`;
- repeated reject: `200 status=rejected`.

`contacts.requests.accept` does not return `200 join_failed` in v1.0.3. Old `room_id` plus the stable `peer_mxid` resolves a replacement direct room and returns that authoritative room in both `room_id` and `current_room_id`.
If `room_id` is omitted, accept/reject may resolve an existing request by the stable `peer_mxid`; a missing request returns `404 request_not_found` and never creates a contact or conversation without a Matrix room.

Channel decision replays no longer map `joining`, `join_failed`, `join/joined`, or `reject/rejected` to a synthetic 404. Recovering approval may return `approved`, local `joining + matrix_join_unconfirmed`, callback `joining + join_result_unconfirmed`, `joined`, or `join_failed + matrix_join_failed`; approving a current rejected generation returns `200 status=rejected`. Reject returns the current terminal/recoverable state: rejected, approved, joining, join_failed, or joined. If Matrix is joined and the joined member fact can be persisted but a downstream projection refresh fails, the result remains `status=joined` with `error_code=operation_recovery_failed` so clients can honor the Matrix fact while continuing background reconciliation. A total persistence failure still returns a structured error rather than claiming a durable success. Only network/5xx callback ambiguity becomes `join_result_unconfirmed`; stable remote 4xx errors remain terminal error envelopes.
The requester callback base URL is generation-scoped: a new application generation may replace the previous terminal generation's address, while an active-generation replay cannot redirect its callback. Approval and rejection always use the address persisted with that generation; a request parameter is only a compatibility fallback for legacy records with no stored address. This prevents node-address changes from stranding approval while keeping same-generation replay or a stale decision payload from hijacking the callback target.

Operation execution uses a durable database claim and revision CAS so two server instances sharing one node database cannot perform the same Matrix mutation concurrently. The operation also stores the request generation observed before a transition, avoiding process-clock ordering when a failed generation races a newer one. Member callback settlement uses the persisted `request_id` as a generation CAS: a delayed generation A callback may report generation B's current state, but cannot write over it. The requester applies the owner node's join-request response with the same generation-and-membership CAS, so a delayed `pending` or `approved` response cannot downgrade a callback or Matrix-confirmed `join`. Legacy clients that omit IDs receive a deterministic next generation after a rejected public-channel application or deleted contact; retries reuse it until persistence succeeds.

Unauthenticated `channels.public.join_request/join_result` validate their Matrix target, user and bounded request identifier before durable execution, including cached-operation replays. Their stored `operation_id` is always canonicalized from the server-owned request generation: an active member reuses its persisted `request_id`, while an initial or terminal application derives the deterministic next generation from the room, channel projection identity, user and current persisted request. Caller-supplied `operation_id` and `request_id` are validation-only inputs and never select the durable key, so concurrent callers cannot create parallel operation rows for one generation. Error envelopes likewise expose the local durable operation ID rather than trusting a nested or remote value.

## 2026-07-09 PostgreSQL-Only Storage And Postman Deprecation

Server storage is now PostgreSQL-only. SQLite/file database connection strings are rejected during configuration or storage initialization, and the monolith no longer falls back to in-memory P2P product state when the persistent store cannot open. Product read models, Matrix component storage, portal/runtime state, and local tests must use PostgreSQL-backed stores.

Postman collections are no longer maintained as contract artifacts. Current action metadata remains generated into `docs/product-action-contract.json`; contract-critical changes must update that artifact, current docs, focused tests, and project-local skills instead of Postman examples.

## 2026-07-09 Native Agent Tool Confirmation Rollback

The request-scoped `dangerous_tools_confirm` gate is deprecated and no longer controls Native Agent tool exposure. `agent.chat` and realtime `client.native_agent_stream` may expose all configured model-callable tools, including write tools, `native_agent_skills_*` mutation tools, `native_agent_mcp_servers_*` mutation tools, external MCP server tools, installed runtime CLI tools, and the built-in `runtime__shell` tool.

Clients must not send `dangerous_tools_confirm` for Native Agent chat/stream calls. The server's built-in Native Agent system prompt now treats shell, runtime CLI, skill/MCP mutation tools, external MCP tools, message sends, and channel comment writes as high-risk capabilities and instructs the Agent to warn the user and summarize the exact action/result, but this warning is not an authorization gate.

OpenAI-compatible model calls now forward non-empty `params.model_profile.reasoning_mode` as `reasoning_effort`. Empty, `none`, and `off` values are omitted so provider defaults apply.

Native Agent subprocesses no longer inherit the full message-server process environment. Runtime CLI/shell commands receive a reduced runtime environment with runtime `PATH` plus minimal OS execution variables. Stdio MCP servers receive the same reduced environment plus explicitly configured MCP server `env`. Model provider API keys and server credentials must not be inherited into those child processes.

Native Agent skill URL installation now fetches only HTTPS public hosts, rejects localhost/private/link-local targets, and does not follow redirects. Installing a skill from inline `content` is unchanged.

Realtime `client.native_agent_stream` now validates the requested stream action against `p2p/serviceapi.ActionSpecs` before entering the Native Agent runner. Non-stream or non-Agent stream actions are rejected at the WS boundary instead of relying only on the downstream runtime allowlist.

## 2026-07-09 MCP Body-Action Compatibility Removal

MCP-D is complete for the fixed Dirextalk body-action wrapper surface. The old fixed `mcp.*` actions are removed from `/_p2p/query`, `/_p2p/command`, the product action registry, and `serviceapi.AgentAction`.

`agent_token` is now accepted only for product body-action `agent.matrix_session.create` and the standard `POST /mcp` endpoint. Owner `access_token` also cannot call fixed `mcp.*` body actions through `/_p2p/query` or `/_p2p/command`; those requests are `400 unknown action`.

External MCP clients must call `POST /mcp` using MCP Streamable HTTP JSON-RPC. Native Agent built-in Dirextalk tools and `POST /mcp` continue to share the same `internal/dirextalkmcp` registry, schemas, pagination, room authorization, DTOs, errors, and p2p adapter invocation. Remaining `mcp.*` strings are internal capability action IDs, not public product body actions.

## 2026-07-08 Standard Dirextalk MCP HTTP Endpoint

External standard MCP clients can now call `POST /mcp` using MCP Streamable HTTP JSON-RPC instead of the Dirextalk `{ "action": "...", "params": ... }` body-action envelope. The first supported lifecycle is `initialize`, `tools/list`, and `tools/call`.

The endpoint requires `Authorization: Bearer <agent_token>`. Owner `access_token` is intentionally rejected on this endpoint, access tokens and agent tokens are not accepted in query strings, and the inbound bearer token is not forwarded into downstream Dirextalk MCP capability calls. The endpoint validates `Origin`; HTTP GET/SSE returns `405` while server-to-client streaming is not used.

`tools/list` is generated from the shared `internal/dirextalkmcp` registry. `tools/call` maps MCP tool names such as `dirextalk_messages_list` to the same unified capability service used by Native Agent built-in Dirextalk tools. Existing MCP rules still apply: `mcp_blocked_room_ids` hides/rejects blocked rooms, ordinary message history remains Matrix Client-Server backed, sends go through `p2p.Transport`, channel posts/comments remain separate from channel chat, pagination uses `from_time`/`to_time`/`cursor`, and old `from_ts`/`to_ts`/`ts`/`last_ts` fields remain unsupported.

## 2026-07-08 Native Agent Backend Contract

Native Agent is now exposed through first-class owner `agent.*` product actions instead of the legacy Agent plugin invoke envelope. The direct action surface includes `agent.chat`, `agent.models.list`, `agent.runtime.inspect/install/which/run`, `agent.skills.list/install/enable/disable/uninstall/registry.search`, `agent.mcp.servers.list/install/enable/disable/uninstall`, `agent.mcp.registry.search`, `agent.context.compress`, `agent.config.propose_patch`, reserved knowledge actions, and built-in Dirextalk tool actions such as `agent.contacts.list`, `agent.rooms.search`, `agent.messages.list/send`, and channel post/comment actions. These are owner-token actions; `agent_token` remains limited to product body-action `agent.matrix_session.create` and `POST /mcp`.

Native Agent streaming moved to dedicated realtime frames:

```json
{
  "type": "client.native_agent_stream",
  "id": "native-agent-stream-1",
  "action": "agent.chat",
  "params": {
    "prompt": "Summarize this conversation"
  }
}
```

The server maps `agent.chat` to the native runtime stream action `agent.chat.stream` and emits `server.native_agent_stream.event` frames for `delta`, `trace`, and `done`, `server.native_agent_stream.error` on failure, and `server.native_agent_stream.cancelled` after `client.native_agent_stream.cancel`. OpenAI-compatible reasoning streams may include explicit `reasoning_content` in `delta.data` and the final `done.data`; clients may display that provider-returned reasoning text, but must not synthesize hidden chain-of-thought. This stream is not the real Matrix `agent_room_id`; Online Agent bridge messages still use Matrix Client-Server sync/send/edit and are not mirrored through Native Agent stream frames.

`io.dirextalk.agent` is removed from plugin management. `plugins.catalog.list` and `plugins.installed.list` do not return it, and `plugins.install/enable/disable/uninstall/config/health/logs/invoke` reject it as not found or unsupported. Non-Agent plugin management is deprecated/inactive for the current product surface; retained `plugins.*` compatibility actions are for future reactivation and should not be used as current client acceptance scope.

Native Agent runtime config uses native portal Agent config storage rather than the hidden legacy Agent plugin record. On startup, any old `io.dirextalk.agent` plugin config is imported into native Agent config once in a sanitized, idempotent way. Root `api_key`/`api_key_ref` fields and model profile API key/ref fields are stripped during migration and native config save/load; model provider API keys remain request-scoped only.

## 2026-07-08 Native Agent Runtime Shell Tool

Native Agent `agent.chat` introduced a built-in `runtime__shell` Eino tool. The tool accepts `command`/`cmd` and optional `timeout_seconds`, runs inside the message-server container's Native Agent runtime directory, and returns the same observable `ok`, `stdout`, `stderr`, and `exit_code` shape as other runtime command execution. It may be model-callable whenever runtime shell is enabled; high-risk operation warnings are handled by the built-in Native Agent prompt rather than a request confirmation field.

Operators may disable the chat shell tool with Agent config `runtime_shell_enabled=false`. The final Docker runtime image now installs `bash` in addition to `/bin/sh`, so bash-based deployment/runtime scripts can run in the container when those scripts are present in the Agent runtime environment.

Native Agent ReAct execution now defaults to a 48-tool-call / 100-graph-step budget and accepts `max_tool_calls` or `max_steps` in Agent config or request params. This lets deployment-style shell and multi-skill install workflows complete multiple command/tool rounds without `[GraphRunError] exceeds max steps`; explicit `max_steps` is capped at 240 server-side to prevent unbounded loops.

## 2026-07-08 Native Agent Dialogue Management Tools

Native Agent `agent.chat` can now expose owner-scoped management tools to the model for explicit user requests to install, list, enable, disable, or uninstall native skills and MCP servers. The tool names are `native_agent_skills_*` and `native_agent_mcp_servers_*`; they call the same native runtime handlers as first-class `agent.skills.*` and `agent.mcp.*` actions.

Skills installed from dialogue are cached as static `SKILL.md` content and do not execute remote skill scripts. A newly installed skill affects the next Agent turn after the system prompt is rebuilt. MCP servers installed from dialogue may discover tools immediately, but those tools become callable on the next Agent turn after the Eino tool list is rebuilt.

Native Agent chat now prepends built-in Dirextalk product rules before any configured or request-scoped system prompt. These rules tell the model to prefer first-class Native Agent management tools over shell commands, to translate `npx skills add <repo> --skill <name>` examples into `native_agent_skills_install` calls, and to keep install/deploy workflows step-efficient. `agent.skills.install` also accepts GitHub owner/repo shorthand and, when given `repo_url` plus `name` or `id` without an explicit path, tries common skills monorepo locations before root `SKILL.md`.

## 2026-07-08 Native Agent Observable Trace

Native Agent `agent.chat` responses now include `steps` and `trace` fields. `steps` is a compact list of observable execution steps such as context loading, tool calls, tool results, assistant messages, and final output previews. `trace` wraps those steps with framework metadata, context usage, tool calls, and the final answer text.

Native Agent WS `client.native_agent_stream` for `agent.chat` emits a `server.native_agent_stream.event` with `event="trace"` before the final `done` event. The `done` payload also includes `steps` and `trace` alongside the existing `text` and `tool_calls` fields.

The trace is an auditable progress/tool/result display surface for clients. It must not be treated as hidden model chain-of-thought; the payload includes a disclaimer and only exposes observable runtime state and model/tool outputs.

## 2026-07-07 Native Agent Runtime

`io.dirextalk.agent` was temporarily represented as an embedded native message-server runtime in the plugin catalog. This plugin-surface contract is superseded by the 2026-07-08 Native Agent Backend Contract: Native Agent now uses first-class `agent.*` actions and dedicated native stream frames, and is not returned or managed as a plugin. Ops and future non-Agent plugins still depend on the Docker runner.

Model calls support request-scoped `model_profile` values for `openai`, `anthropic`, `deepseek`, and `openai_compatible`. Current clients own and persist model profile lists locally, then send the selected profile per `agent.chat`, `agent.chat.stream`, and `agent.context.compress` request. Model API keys are accepted only per request and are not persisted, returned by config APIs, or injected into plugin env.

Native Agent chat responses and `agent.chat.stream` done payloads include `native=true` and `framework="eino"` so clients and smoke tests can verify the embedded Eino runtime path is active.

Native Agent now owns dynamic skills, third-party MCP clients, runtime CLI tools, orchestration loops, server-side conversation memory, context compression, and built-in Dirextalk tools. Eino ReAct is the single orchestration path; model providers use maintained Eino OpenAI and DeepSeek components, Anthropic is direct API only through an Eino `ToolCallingChatModel` adapter, third-party MCP uses Eino official MCP, and installed runtime CLI tools are exposed as Eino tools for in-loop execution and summarization. Built-in tools proxy contacts, rooms, ordinary messages, room members, channel posts/comments, summaries, and message/comment writes through existing P2P/Matrix boundaries. Homeserver/sync DB reads are read-only; Matrix writes continue through `p2p.Transport`/roomserver.

## 2026-07-08 MCP Unified Channel Time Pagination

MCP read actions now use readable UTC RFC3339/RFC3339Nano timestamps and stable snapshot cursors. `mcp.messages.list`, `mcp.channel_posts.list`, and `mcp.channel_comments.list` accept `from_time`, `to_time`, `cursor`, and `limit`; legacy `from_ts` and `to_ts` are rejected with `400`.

The default order is newest first. Cursor pages keep the first-page snapshot fixed, so posts, comments, or messages inserted after the first page do not appear in that cursor chain. Clients must start a fresh query without `cursor` to fetch newer content.

MCP responses no longer return `ts` or `last_ts`. Message, post, comment, send, and comment-create summaries return `created_at`; room summaries return `last_message_at`; member summaries return string `joined_at`.

`mcp.channel_posts.list` post summaries now include `comment_count`, `like_count`, `favorite_count`, and `favorited_by_me`. Favorite state is owner-local message-server favorite state, not a federated/global channel count. Channel ordinary chat remains separate and is read through `mcp.messages.list`, which continues to filter out product `p2p_kind` post/comment events.

The embedded Native Agent Dirextalk tools use the same contract: `dirextalk_messages_list`, `dirextalk_channel_posts_list`, and `dirextalk_channel_comments_list` expose `from_time`, `to_time`, `cursor`, and `limit` instead of legacy millisecond timestamp fields.

## 2026-07-05 Official Ops Plugin

Added official catalog plugin `io.dirextalk.ops` for single-node private deployment operations. It uses `docker.io/dirextalk/ops-plugin:latest` and exposes owner-invoked plugin actions through existing `plugins.invoke`:

- `ops.status.get`
- `ops.containers.list`
- `ops.logs.tail`
- `ops.backups.list`
- `ops.backup.create`
- `ops.backup.status`
- `ops.backup.download_chunk`
- `ops.backup.delete`
- `ops.cleanup.plan`
- `ops.cleanup.run`
- `ops.rooms.cleanup.plan`
- `ops.rooms.cleanup.run`
- `ops.media.orphans.plan`
- `ops.migration.export`
- `ops.restore.plan`
- `ops.restore.run`

The server treats Ops as the only official plugin allowed to receive privileged Docker runner mounts. When enabled, Ops receives the Docker socket mount and a dedicated backup volume, plus `OPS_BACKUP_ROOT`, `OPS_MAX_BACKUPS`, `OPS_MESSAGE_SERVER_CONTAINER`, `OPS_POSTGRES_CONTAINER`, `OPS_POSTGRES_USER`, and `OPS_POSTGRES_PASSWORD`. Ops does not receive owner access token or `DIREXTALK_AGENT_TOKEN`. Non-Ops plugins are rejected if they request privileged mounts.

Backup creation can run asynchronously and expose progress through `ops.backup.status`; backup files are downloaded through `ops.backup.download_chunk`. `ops.restore.run` requires `confirm="restore_backup"` and restores the Postgres dump from a selected backup package. Cleanup contracts are intentionally plan-first. `ops.cleanup.plan`, `ops.rooms.cleanup.plan`, and `ops.media.orphans.plan` estimate impact before execution. `chat_purge_physical` and direct SQL deletion of Matrix event tables are not part of the first-version Ops plugin; room history cleanup is limited to cache cleanup, local hiding/archive planning, and backend-controlled safe actions.

## 2026-07-04 Official Plugin Manager And Agent MCP Boundary

Agent-specific Docker/container details in this section were superseded by the 2026-07-07 Native Agent runtime. Non-Agent Docker plugin manager details still apply.

Added protected owner-only plugin management actions on the existing body-action surface:

- `plugins.catalog.list`
- `plugins.installed.list`
- `plugins.install`
- `plugins.enable`
- `plugins.disable`
- `plugins.uninstall`
- `plugins.config.get`
- `plugins.config.update`
- `plugins.job.get`
- `plugins.health`
- `plugins.logs.tail`
- `plugins.invoke`
- `plugins.invoke.stream`

These actions require owner `access_token`. `agent_token` cannot call plugin management or plugin invoke actions. `plugins.catalog.list` returns an empty `plugins` list when the Docker plugin runner is not enabled, so clients should hide plugin entry points until catalog entries are available. Agent-specific plugin catalog/config/invoke details in this historical section are superseded by the 2026-07-08 Native Agent Backend Contract. Current plugin install/enable/disable/uninstall jobs are for non-Agent official plugins such as `io.dirextalk.ops`, and must use official catalog metadata whose Docker image belongs to the official `dirextalk` Docker Hub organization. Digest metadata is optional and is not required for first-version installs.

Native Agent action details now live on the first-class `agent.*` product action surface. `agent.models.list` uses the request-scoped `provider`, `base_url`, and `api_key` to fetch the real model list from supported providers and returns provider-reported `models[]` entries such as `id`, `name`, and any raw capability fields the provider actually supplies, for example `context_length`, `max_output_tokens`, `temperature`, `top_p`, `reasoning_modes`, or `reasoning_effort_options`; it must not persist or echo API keys, and must not invent model defaults or capabilities. Clients should render optional model parameters from returned metadata when present, keep missing values unset, allow manual model IDs when listing is unavailable, and omit unset tuning parameters from chat requests so provider defaults apply. `agent.runtime.inspect` resolves request-scoped model settings without returning API keys and reports runtime status/tool counts for configured third-party MCP servers and CLI tools; model calls can also use read-only `native_agent_runtime_inspect` without dangerous-tools confirmation. `agent.runtime.install` installs allowed runtime CLI/package-manager capabilities, such as `agent-reach`, without expanding `agent_token` permissions. Knowledge action names remain reserved for compatibility, but first-version Agent returns `supported=false`/`status=unsupported` and clients should not render knowledge UI. The Native Agent owns standard MCP client orchestration and ships package-manager launch support for third-party MCP servers installed from registry metadata (`npx` for npm packages and `uvx` for Python packages), while the message-server exposes the standard `POST /mcp` endpoint to `agent_token`.

`plugins.invoke` calls an enabled official non-Agent plugin over the first-version Docker HTTP runner and returns `{ "plugin_id", "action", "result" }`. `plugins.invoke.stream` remains registered only to return `400 action requires websocket`; Native Agent streaming uses `client.native_agent_stream`, not the legacy Agent plugin stream frame. `client.request` remains unavailable for `plugins.*`.

The backend remains the Dirextalk capability boundary for Agent/MCP access: Native Agent built-in tools and the standard `POST /mcp` endpoint share owner-scoped access control, Matrix transport writes, product projections, and `mcp_blocked_room_ids` enforcement through `internal/dirextalkmcp`. Contact list/search capabilities expose accepted direct contacts to local Agent tooling without requiring a room search fallback. Native Agent skills, model/provider request handling, MCP client wiring, and orchestration are embedded in message-server behind owner `agent.*` actions. External standard MCP clients should use `POST /mcp` JSON-RPC instead of the Dirextalk action envelope.

## 2026-07-03 Unified Channel Post+Chat

Channels are now a unified post+chat surface in one Matrix room. `channels.create` defaults missing or invalid `channel_type` to `post`, but current server behavior does not branch on legacy `chat` vs `post` values. New channel rooms, including existing-room channel bindings, write `m.room.history_visibility=shared`. Joined channel conversations expose post/comment/reaction capabilities according to room role and comments settings instead of `channel_type`.

Product post/comment/reaction events remain identified by Matrix content metadata such as `p2p_kind`; ordinary channel chat messages stay as Matrix timeline events and do not update post/comment/reaction projections. Conversation activity for channels is updated by ordinary chat messages, while post/comment projection events do not pollute ordinary conversation activity.

HTTP Push Gateway delivery is suppressed for all channel room events. This is Push Gateway suppression only; Matrix sync, room timelines, unread state, read markers, and local client navigation still operate normally.

## 2026-07-02 Owner Blocks

Added protected owner actions `blocks.add`, `blocks.list`, and `blocks.remove` for the user contact blacklist. `blocks.add` accepts `target_type: "contact"` with `peer_mxid`/`mxid`; group, channel, and room targets are not part of the current product contract. Each block stores a `display_name` and `avatar_url` display snapshot; when omitted, the server fills it from existing contact metadata or falls back to the target ID. `blocks.list` returns a `contacts` array for the user settings blacklist. `blocks.remove` cancels a contact block using the same identifiers.

When an owner tries to send a friend request to an already blocked contact, the action fails before Matrix writes with:

```json
{
  "error": "already blocked"
}
```

The HTTP/WS response status is `403`. These actions require owner `access_token`; they are not public actions and are not available to `agent_token`.

Inbound Matrix direct invites from blocked contacts are ignored by projection and do not appear as pending friend requests.

## 2026-07-01 Agent Config Avatar And MCP Room Blacklist

`agent.config.get` and `agent.config.update` now include two owner-managed fields:

```json
{
  "avatar_url": "mxc://example/agent",
  "mcp_blocked_room_ids": ["!room:example.com"]
}
```

`avatar_url` is a display-only Agent profile setting for clients. `mcp_blocked_room_ids` is a durable room blacklist under Agent config. `agent.config.update` replaces the blacklist with the supplied normalized list; omitted fields keep their previous values.

At that point, fixed MCP actions remained HTTP-only and owner-scoped. Current MCP clients use `POST /mcp`, which enforces the same room blacklist rules. Rooms in `mcp_blocked_room_ids` are not returned by MCP room search; direct MCP access to blocked rooms through ordinary message send/list, member list, channel post list, channel comment list, or channel comment creation is rejected with `403 room is blocked for MCP`.

## 2026-06-30 Owner HTTP Fallback For Product Actions

Logged-in client product actions now use ready-WS first instead of WS-only. Clients should use owner `GET /_p2p/ws` `client.request` only after the realtime transport has received `server.ready`. When WS is not ready or disconnected at click time, clients should send the same body-action envelope to `POST /_p2p/query` or `POST /_p2p/command` with `Authorization: Bearer <access_token>` immediately and let realtime WS reconnect in the background. Transport failure before a response may also use owner HTTP fallback for safe repeated actions.

Business errors returned by WS, such as permission or validation failures, should not be retried over HTTP. Clients should also de-duplicate identical in-flight user actions such as `contacts.requests.accept` or `groups.join` so duplicate taps do not send duplicate mutations or show duplicate success UI. If a WS request was already sent and the response is lost, clients should only HTTP-fallback actions that are safe to repeat, such as contact decisions, joins, read markers, and product queries.

`agent_token` permissions did not change for product body actions in this pass: it remained limited to `agent.matrix_session.create` and fixed `mcp.*` HTTP actions at that point. This was superseded by the 2026-07-09 MCP-D removal: current servers accept `agent_token` for product body-action `agent.matrix_session.create` and standard `POST /mcp`; fixed `mcp.*` body actions are unknown.

Realtime WS owner tickets now advertise `expires_in_ms: 120000` to tolerate mobile weak-network upgrade delays. A failed HTTP request to `GET /_p2p/ws?ticket=...` that never completes the WebSocket upgrade no longer consumes the ticket; accepted WebSocket upgrades remain single-use.

## 2026-06-30 Retained Room Reactivation For Rebuilt Members

Added internal public action `rooms.reactivate` for node-to-node recovery when a group or private-channel member node has been rebuilt and lost local product/Matrix projections while the owner node still retains the member in the Matrix room. It is not a normal client workflow entry.

The original implementation removed an already-joined membership before asking the target whether recovery was needed. That unsafe ordering is superseded by the 2026-07-13 contract above: ordinary repeats preserve joined state, and an explicit `rebuild_generation` must be confirmed by the target before kick plus replacement invite. After a confirmed rebuild, the target records an invite/pending room card only; it does not silently join. The user must still confirm by calling `groups.join` or `channels.join`, and joined state is recorded only after Matrix join succeeds. Public channels continue to recover through `channels.public.join_request` and the normal open/approval flow.

For rebuilt direct-contact nodes, `contacts.request` still first asks the retained peer to re-invite the old accepted direct room. If the retained room cannot be rejoined because the rebuilt node lost its old Matrix room/key state, including a missing local room version after database loss, the requester creates a replacement direct request room. The retained peer accepts that replacement only from the real Matrix invite sender and preserves local contact remarks; old direct-room history is not copied into the replacement.

## 2026-06-30 Contact Re-Request Replacement Room

When both sides of a direct contact have left the retained old direct room, or the peer node no longer retains the old relationship, a new `contacts.request` creates a replacement direct request room instead of binding the pending request to the old room. The returned contact `room_id` may therefore differ from the previous direct room. Clients should use the latest `room_id` from `contacts.request`, `contacts.list`, `sync.bootstrap.contacts`, or contact mutation responses when accepting or opening the conversation.

For historical pending requests that still point at an unrejoinable old direct room, `contacts.requests.accept` falls back to creating a replacement direct room and returns the accepted contact with the new `room_id`.

## 2026-06-30 Contact Display Name Override

`contacts.update` now marks the supplied `display_name` as a local contact remark. Contact records returned from `contacts.update`, `contacts.list`, and `sync.bootstrap.contacts` may include `display_name_override: true` when the displayed name is owner-managed.

Remote Matrix member profile updates still refresh peer avatar metadata, but they no longer overwrite an accepted contact's `display_name` while `display_name_override` is true. Contacts without a local override keep the previous Matrix-native behavior and continue to follow the peer's latest Matrix member display name.

## 2026-06-30 Agent Bridge Transport Returns To Matrix

Agent bridge online display remains Matrix-native room state in the real `agent_room_id`: event type `io.dirextalk.agent.status`, state key `@agent:<server>`, and content field `online`. The running local bridge writes `online=true/false` through its `@agent:<server>` Matrix session. The server no longer treats `agent.config.enabled=true` or an agent-token WS session as online; startup/agents-room repair and `agent.config.update enabled=false` only publish `online=false` as a fallback.

`agent_token` no longer creates realtime WS tickets. `realtime.ws_ticket.create` is owner-token only; `agent_token` remains limited to product body-action `agent.matrix_session.create` and current `POST /mcp`.

`agent.matrix_session.create` remains a retained HTTP body action and may be called with either owner `access_token` or `agent_token`. It returns a Matrix Client-Server session for the local `@agent:<server>` bridge user so dirextalk-connect can bootstrap Matrix-native Agent room sync/send/edit without owner credentials. It must not be migrated into Product WS and must not evict owner devices.

Agent room messages, previews, edits, and final replies are transported through Matrix Client-Server APIs as `@agent:<server>`. `agent_room.message`, `client.agent_stream`, and `server.agent_stream` are no longer current protocol frames/events.

No response fields change: `sync.bootstrap` still returns only `agent_room_id` for Agent room discovery and does not return `agent_online`; `agent.status`/`agents.status` remain removed.

## 2026-06-30 MCP HTTP Boundary And WS Client State Flags

At this point, fixed MCP actions remained HTTP body actions on `POST /_p2p/query` or `POST /_p2p/command`; they were not migrated into WS `client.request`. That compatibility surface was removed on 2026-07-09. `agent.matrix_session.create` remains HTTP-only. If an owner or agent WS session sends a `client.request` for `agent.matrix_session.create`, the server returns:

```json
{
  "type": "server.response",
  "id": "req-1",
  "action": "agent.matrix_session.create",
  "ok": false,
  "status": 400,
  "error": "action requires http"
}
```

WS lifecycle and focus frames now accept extra client-state fields for future push decisions while preserving the existing `foreground` and `room_id` fields:

```json
{
  "type": "client.lifecycle",
  "foreground": false,
  "state": "hidden",
  "hidden": true,
  "flags": {
    "hidden": true,
    "background": true
  }
}
```

```json
{
  "type": "client.focus",
  "room_id": "!room:server",
  "focused": true,
  "flags": {
    "focused": true
  }
}
```

Push suppression requires a fresh foreground WS session that is not hidden and has the same focused room as the push candidate. Hidden/background/disconnected/expired/different-room state keeps normal push behavior.

## 2026-06-30 WS Product API Full Migration

Logged-in Dirextalk client/product actions now use `GET /_p2p/ws` request/response frames instead of HTTP body-action calls. HTTP `/query` and `/command` remain for portal bootstrap/auth/status/password, `agent.matrix_session.create`, `realtime.ws_ticket.create`, and node-to-node public/callback actions. Standard MCP clients use `POST /mcp`.

This WS-only HTTP rejection rule was superseded later on 2026-06-30 by the owner HTTP fallback contract above. Current clients are WS-first, not WS-only.

Client request frame:

```json
{
  "type": "client.request",
  "id": "req-1",
  "action": "contacts.list",
  "params": {}
}
```

Successful response frame:

```json
{
  "type": "server.response",
  "id": "req-1",
  "action": "contacts.list",
  "ok": true,
  "result": {}
}
```

Error response frame:

```json
{
  "type": "server.response",
  "id": "req-1",
  "action": "contacts.list",
  "ok": false,
  "status": 401,
  "error": "M_UNKNOWN_TOKEN"
}
```

`client.command` was retained only as a one-release compatibility alias and mapped to the same `server.response` path. That compatibility alias is now removed; clients must send `client.request`.

`GET /_p2p/events` is removed. The P2P outbox remains durable because WS `server.event` replay and cursor recovery still use it. Cursor retention gaps are reported only through WS `server.cursor_reset`; clients must recover by issuing `sync.bootstrap` over WS.

Owner WS sessions may call protected logged-in product actions except `realtime.ws_ticket.create` and `agent.matrix_session.create`. Agent-token callers cannot create WS sessions; Agent bridge bootstrap stays on HTTP body actions, and MCP clients use `POST /mcp`. Matrix Client-Server remains the protocol source for ordinary timeline, media, history, search, redaction, local delete, and Agent bridge room traffic.

HTTP `/query` and `/command` now return an explicit error for non-retained logged-in client actions:

```json
{
  "error": "action requires websocket"
}
```

## 2026-06-30 Transitional Realtime WS Commands And Agent Stream Frames

This transitional contract was superseded later the same day by the WS Product API full migration above. During the transition, `GET /_p2p/ws` accepted owner-session `client.command` frames for lightweight product commands. The initial allowlist was:

- `sync.read_marker`
- `channels.read_marker`

Frame shape:

```json
{
  "type": "client.command",
  "id": "cmd-1",
  "action": "sync.read_marker",
  "params": {
    "room_id": "!room:server",
    "event_id": "$event",
    "origin_server_ts": 1710000000000
  }
}
```

Successful commands returned `server.command_result` with `id`, `action`, and `result`. Validation, auth, and action errors returned `server.command_error` with `id`, `status`, and `error`. Current servers reject `client.command` with `400 unsupported frame type`; clients must use `client.request`. Agent-token WS sessions cannot call owner commands.

This transitional agent stream contract was removed later the same day. Current Agent bridge previews and final replies use Matrix Client-Server messages/edits from `@agent:<server>`; current clients must not emit agent stream WS frames and current servers must not expose Agent bridge traffic on Product WS.

## 2026-06-29 WebSocket Realtime Sync

Added protected body action `realtime.ws_ticket.create`, normally sent to `POST /_p2p/query` with an empty `params` object:

```json
{
  "action": "realtime.ws_ticket.create",
  "params": {}
}
```

The action accepts owner `access_token` only. It returns:

```json
{
  "ticket": "ws_ticket_...",
  "expires_in_ms": 120000
}
```

The ticket is server-local, short-lived, and single-use. It is consumed only after `GET /_p2p/ws?ticket=<ticket>` completes WebSocket upgrade. The WS route does not accept bearer tokens directly.

The first client text frame must be `client.hello` with optional `since`, `client`, and `platform` fields. Subsequent client frames are:

- `client.lifecycle`: `{ "foreground": true|false }`
- `client.focus`: `{ "room_id": "!room:server" }`, or empty `room_id` to clear focus
- `client.ack`: `{ "seq": 123 }`
- `client.ping`

Server frames are:

- `server.ready`
- `server.event` with the existing P2P event payload in `event`
- `server.cursor_reset` with the same recovery payload shape as the SSE `p2p.cursor_reset` event
- `server.pong`
- `server.error`

Owner WS sessions receive the normal product event stream. The initial implementation also allowed agent-token WS/SSE streams for `agent_room.message`; that path was later removed in favor of Matrix Client-Server bridge sync/send/edit.

Push suppression now prefers fresh WS session state. A connected foreground WS session suppresses unread notification insertion and HTTP push gateway delivery only when its focused room matches the room that produced the push candidate. Background, disconnected, expired, no-focus, or different-room state keeps normal background push behavior. The server timestamps and expires WS session state with server time; clients do not send expiry timestamps.

## 2026-06-29 Matrix Account-Data Foreground Fallback And Agent Room Defaults

Dirextalk clients that have not established a fresh WS session may still suppress foreground system pushes by writing global Matrix account data type `io.dirextalk.push.context` through the existing Matrix account data route. The expected body is:

```json
{
  "foreground": true
}
```

The Matrix account data write path stamps foreground writes with a server-clock 60-second expiry. While the stamped foreground state is fresh and no fresh WS session exists for the user, the userapi roomserver consumer does not create an unread notification row and does not call the HTTP push gateway for matching Matrix push-rule notifications. Missing, malformed, expired, or `foreground=false` context fails open and keeps normal background push behavior. This is a migration fallback only; the server does not infer foreground/background from `/sync`, read receipts, or pusher registration.

Clients should prefer WS `client.lifecycle` and `client.focus`. During migration, clients may continue refreshing this account data every 30 seconds with `{"foreground": true}` and write `{"foreground": false}` when entering background; if that write is missed, the previous foreground state naturally expires after the server-stamped 60 seconds.

Backend startup now also ensures the portal owner has a room-level Matrix push rule for the real `agent_room_id` with empty actions, so new or repaired agents rooms default to no system push. Existing explicit room push rules for the same room are preserved.

## 2026-07-07 Owner Report Notifications

Reintroduced `reports.submit` as a public ProductCore action for owner-directed
group/channel reports only. Friend reports and official report submissions stay
on signed imadmin public APIs. The owner node validates the target
group/channel, persists a `p2p_reports` row, and sends a Matrix `m.notice` into
the durable `system_room_id` with `msg_type=report`,
`p2p_kind=system_report`, reporter metadata, target metadata, reason/body, and
Matrix media `image_urls`. Portal auth and `sync.bootstrap` now return
`system_room_id`. The system room is intentionally not given the agent room's
empty-action push rule because owner report cards should notify.

## 2026-06-29 P2P Reports Submit Removed

Removed `reports.submit` from the message-server P2P action surface. User-facing report submission remains on the signed imadmin public API, so this server no longer registers the P2P report action or persists P2P report rows.

## 2026-06-29 P2P Event Cursor Reset Signal

`GET /_p2p/events` now detects a non-zero `since` cursor that is older than the retained `p2p_events` window. The stream stays HTTP 200 and replays retained events, but it first emits an SSE control event `event: p2p.cursor_reset` without advancing the SSE event id.

The control payload contains `type`, `since`, `min_seq`, `max_seq`, `count`, and `recovery: "bootstrap_required"`. The response also sets `X-Dirextalk-P2P-Events-Cursor-Reset: true`, `X-Dirextalk-P2P-Events-Min-Seq`, `X-Dirextalk-P2P-Events-Max-Seq`, and `X-Dirextalk-P2P-Events-Count` before streaming begins.

Clients should treat this as a product cache gap: clear local product projections, call `sync.bootstrap` once, persist the newest handled event `seq`, and then continue normal WS delta consumption. SSE fallback clients continue with `GET /_p2p/events?since=<seq>`.

## 2026-06-29 MCP Room Member Identities

Added protected MCP action `mcp.room_members.list` on `POST /_p2p/query`. Owner `access_token` and fixed MCP `agent_token` could call it at that point. This body-action endpoint was removed on 2026-07-09; current clients use the standard MCP tool over `POST /mcp`. The action accepts `room_id` or `channel_id`, optional `status`/`membership`, optional `role`, and optional `limit`; it returns `room_id`, `name`, `count`, and concise member identities with `user_id`, `user_mxid`, `localpart`, `domain`, `display_name`, `avatar_url`, `membership`, `role`, and `joined_at`.

`mcp.room_members.list` is owner-scoped and only reads known Dirextalk product rooms or conversations. It may enrich stale product projections from current Matrix `m.room.member` state and Matrix profile fallback data, but it rejects unknown room IDs instead of exposing arbitrary roomserver state through the MCP surface.

`mcp.messages.list` message summaries now expose sender identity fields: `sender_mxid`, `sender_display_name`, `sender_domain`, and `sender_localpart`. The legacy `sender` field is preserved and is upgraded to a readable display name when Matrix member/profile data is available.

`mcp.rooms.search` may use current Matrix member state to display fresher group/channel member counts when product read-model counts are stale.

## 2026-06-27 MCP Owner-Scoped Message History

MCP actions remain a fixed `agent_token` allowlist, but their product behavior is owner-scoped: room search, default ordinary message send, ordinary message list, channel post/comment list, and channel comment create operate from the portal owner view instead of exposing the local Agent Matrix account as an independent product user.

`mcp.messages.list` now reuses the current owner `access_token` for Matrix history reads. It does not call `agent.matrix_session.create`, does not create a `DIREXTALK_MATRIX_HISTORY` device, and does not refresh the portal owner's Matrix session, so MCP history reads cannot evict the owner's phone or browser session.

Default owner-scoped `mcp.messages.send` now rejects the configured `agent_room_id`. Agent-room replies remain supported only through the internal gateway marker path (`agent_gateway=true` or `gateway_source`), where the local `@agent:<server>` user sends the reply and marks the event to prevent gateway loops.

## 2026-06-26 Agent Matrix Session Identity

`agent.matrix_session.create` now creates and returns a Matrix Client-Server session for the local agent user `@agent:<server>` instead of the portal owner. The response fields remain `access_token`, `device_id`, `user_id`, and `homeserver`; `user_id` is now the local agent MXID. Current servers accept either owner `access_token` or `agent_token` for this HTTP-only bootstrap action.

The helper still uses `revokeExistingDevices=false`, so creating a cc-connect or local gateway Matrix session does not evict the portal owner's phone or browser sessions.

## 2026-06-26 Agent Matrix Room State Status

Owner clients now receive Agent bridge online state from native Matrix room state in the real `agent_room_id`: event type `io.dirextalk.agent.status`, state key `@agent:<server>`, and content field `online`.

The server writes this state when creating or repairing the agents room and when `agent.config.update` changes `enabled`. This was later narrowed: the server only writes `online=false` fallbacks, while the running local bridge writes true/false through Matrix. `sync.bootstrap` still returns the real `agent_room_id` so clients can locate the room, but it no longer returns `agent_online` or any `agent_presence` mirror. `agent.status` and `agents.status` are removed.

Matrix `m.presence` is not part of the Agent online contract, and Dirextalk monolith startup no longer enables Matrix outbound presence for this path. New generated, sample, and Helm configs default both inbound and outbound presence to `false`.

## 2026-06-25 Agent Token Event Stream Access

`GET /_p2p/events` previously accepted bearer `agent_token` as well as owner `access_token` as a narrow passive gateway exception. This path was later removed with SSE and the Agent bridge returned to Matrix Client-Server transport.

Non-MCP protected body actions still reject `agent_token` except the HTTP-only `agent.matrix_session.create` bridge bootstrap action. The fixed MCP action allowlist mentioned in this historical entry was removed on 2026-07-09.

## 2026-06-25 Immutable Channel Type

`channels.update` now ignores `channel_type`. Channel type is creation-time metadata and cannot be changed after a channel exists. Requests that include `channel_type` continue to apply other mutable fields but leave the stored `channel_type` unchanged.

Clients may send `channel_type` only in `channels.create`; missing or invalid values now default to `post`. Since 2026-07-03, all new channel rooms get shared Matrix history visibility at creation or when binding an existing room as a channel, regardless of legacy `channel_type`.

## 2026-06-25 Agent Token And CLI Cleanup

Agent-token dynamic permission management is removed. `apis.list` and `apis.status` are no longer P2P actions and calls to those action names return `unknown action`.

Protected product actions require bearer `access_token`. At this point, `agent_token` was accepted only for `agent.matrix_session.create` and fixed MCP actions: `mcp.rooms.search`, `mcp.messages.send`, `mcp.messages.list`, `mcp.channel_posts.list`, `mcp.channel_comments.list`, and `mcp.channel_comments.create`. `GET /_p2p/events` was a route-level exception for passive gateway listening at the time and was later removed; other protected body actions reject `agent_token`. The later standard `POST /mcp` endpoint is recorded in the 2026-07-08 entry.

The first-party CLI module and its helper package are removed: `cmd/dirextalk-cli`, `internal/agentclient`, CLI build scripts, CLI agent-skill docs, and the project-local `dirextalk-cli` Codex skill.

## 2026-06-25 Matrix Push Gateway Metadata

Matrix event pushes sent to HTTP push gateways now include optional Dirextalk display/routing metadata when the room has Dirextalk product room state. Normal direct and group message pushes can include `notification.title`, `notification.push_type=message`, `notification.room_id`, `notification.event_id`, and short `notification.room_type` (`direct` or `group`). The gateway owns the visible body text and sets it to `Send you a new message`.

Channel rooms (`notification.room_type=channel`) are not sent to HTTP push gateways. Matrix `m.call.invite` events in Dirextalk rooms use `push_type=call` and add `call_id` plus `call_kind=voice`; product `calls.create` / `calls.incoming` actions remain P2P event/call-record flows unless represented as Matrix call invite events.

## 2026-06-24 Portal Single-Device Login

`portal.bootstrap`, `portal.auth`, and `portal.password` now create an exclusive Matrix device session for the portal owner. After the new session is created, the server deletes the owner's other Matrix devices while preserving the current `device_id`, so previous phones receive Matrix `M_UNKNOWN_TOKEN` on later authenticated requests and must ask the user to log in manually.

`agent.matrix_session.create` remains an internal Matrix session helper and does not evict the portal user's phone session. As of 2026-06-26, it returns a session for the local `@agent:<server>` user. Current servers accept either owner `access_token` or `agent_token` for this HTTP-only bridge bootstrap action.

## 2026-06-24 User Public Channel Lookup

`users.public_channels` now returns only public channels owned by the target user. Public channels where the target user is only a normal member are no longer included in the "user's channels" list.

`users.public_channels` also accepts optional `remote_node_base_url` and forwards the public query to that owner node, matching remote public channel discovery flows. The forwarded request strips `remote_node_base_url` before reaching the target node.

## 2026-06-24 Channel Room Projection Guard

Matrix room state is now treated as a channel projection source only when `io.dirextalk.room.profile.room_type` is explicitly `io.dirextalk.room.channel` and `channel_id` is an explicit product channel id. Empty profiles, group/direct room profiles, missing `channel_id`, and Matrix-room-id-shaped `channel_id` values are ignored by channel refresh logic.

`groups.join` no longer calls the channel room refresh path after Matrix join. Group member refresh still runs for the joined group, but it cannot create or update a `channels` read-model row. This prevents group chats with empty profile state from appearing in `channels.list` or `sync.bootstrap.channels`.

## 2026-06-24 Channel Reaction History Snapshots

`channels.my_reactions` still returns `{ "reactions": [...] }`, but each item is now a display history snapshot object instead of a bare reaction row. The item contains:

- `reaction`: the original reaction record with `target_type`, `target_id`, `channel_id`, `post_id`, `comment_id`, `reaction`, `user_id`, `active`, and `created_at`.
- `channel`: the current channel snapshot when available, including `name`, `avatar_url`, `channel_type`, `member_count`, and normal channel metadata.
- `post`: the parent post snapshot when available, enriched with comment/reaction counts and `reacted_by_me`.
- `comment`: the comment snapshot for comment reactions when available, enriched with reaction count and `reacted_by_me`.

Clients must not synthesize fake channel/post display data from a bare reaction row. If a snapshot is missing, show an unavailable or syncing state instead of fallback labels such as `频道`, `文字`, or `频道帖子`.

`channels.public.get`, `channels.public.search`, and `users.public_channels` refresh public channel `member_count`/`pending_join_count` from persisted ProductCore membership before returning a channel when membership rows are available. This keeps public detail and public list views aligned with the owner node's joined member facts.

## 2026-06-23 Realtime Call Lifecycle

`calls.event` now accepts `rejected` in addition to `connected`, `ended`, `missed`, and `failed`. Call records persist `answered_at`, `ended_at`, `ended_by_mxid`, `end_reason`, and `duration_ms` in `p2p_calls`, so call start/end timing survives restart.

Every `calls.create`, `calls.incoming`, and `calls.event` write appends a `call.changed` event to `GET /_p2p/events`. The event payload contains the current call record under `payload.call`, allowing clients to update active call UI immediately when the other party rejects or hangs up.

Terminal call states are not reopened by later stale `calls.create`, `calls.incoming`, or non-terminal `calls.event` writes with the same `call_id`. Clients that arrive late after `missed`, `ended`, `rejected`, or `failed` receive the terminal snapshot and must not join that call.

## 2026-06-23 Agents Room Gateway

This section records the original gateway behavior from June 23. Current behavior supersedes it: Agent bridge traffic no longer uses SSE/P2P outbox events and is transported through Matrix Client-Server sync/send/edit as `@agent:<server>`.

Backend startup now creates a real private Matrix agents room when the stored `agent_room_id` is empty or still uses the legacy pseudo form `!agent:<server>`. The real room id is persisted in portal state and written to the bootstrap credentials file as `agent_room_id`. The room contains the portal owner and the local agent user `@agent:<server>`; existing real agents rooms are repaired on startup by joining the local agent user if needed.

`portal.bootstrap`, `portal.auth`, and `sync.bootstrap` expose the current real `agent_room_id` so app and gateway clients can restore the Agent conversation from either login/session metadata or first-screen metadata.

`GET /_p2p/events` can now emit `agent_room.message` for ordinary `m.room.message` events in the configured agents room only. Payload fields are `room_id`, `event_id`, `sender_mxid`, `body`, `msgtype`, and `origin_server_ts`. Ordinary messages in other non-product rooms still do not produce P2P events or P2P message records.

`mcp.messages.send` accepts internal optional gateway marker params, including `agent_gateway=true` and `gateway_source`. Marked replies are sent by the local agent user, written as Matrix messages with `io.dirextalk.agent_gateway` metadata, and are not re-emitted as inbound `agent_room.message` events, preventing gateway reply loops. `mcp.messages.list` returns the agents room name as `Agents` and displays messages from `@agent:<server>` using the configured agent `display_name`.

## 2026-06-23 Channel Join Request Approval Retry

`channels.join_request.approve` now treats an existing `join_failed` or `approved` channel join request as retryable approval state instead of returning `404 join request not found`. This lets a channel owner retry approval after the requester-node `channels.public.join_result` callback temporarily failed. Ordinary channel invites still are not accepted by the join-request approval action.

## 2026-06-22 Dirextalk Local MCP Backend Actions

Added six protected MCP-oriented P2P actions for the local Dirextalk MCP adapter:

- `mcp.rooms.search` on `POST /_p2p/query`
- `mcp.messages.list` on `POST /_p2p/query`
- `mcp.channel_posts.list` on `POST /_p2p/query`
- `mcp.channel_comments.list` on `POST /_p2p/query`
- `mcp.messages.send` on `POST /_p2p/command`
- `mcp.channel_comments.create` on `POST /_p2p/command`

All six required bearer auth. Owner `access_token` could call them, and `agent_token` was accepted for these fixed MCP actions at that point. The fixed body-action endpoints were removed on 2026-07-09; current clients use `POST /mcp`. The response contracts are intentionally concise for MCP tooling and do not expose full `conversationView`, `channelPostRecord`, `channelCommentRecord`, Matrix event payloads, projection state, capability maps, or internal Matrix tokens.

Ordinary MCP message send/list remains separate from channel post/comment product content. `mcp.messages.send` writes a plain `m.room.message` without `p2p_kind`; `mcp.messages.list` reads ordinary Matrix timeline messages through a server-side Matrix reader and filters out events carrying product `p2p_kind`. No P2P ordinary-message store was added.

At that point, the P2P body-action count was 91. Current action metadata is generated into `docs/product-action-contract.json`.

## 2026-07-03 Account Deletion

Added protected owner HTTP-only command `portal.account.delete` on `POST /_p2p/command`.

Request:

```json
{
  "action": "portal.account.delete",
  "params": {
    "confirm": "delete_account"
  }
}
```

Behavior:

- Requires owner `access_token`; `agent_token` is rejected.
- Cannot be called through `GET /_p2p/ws` `client.request`; WS returns `action requires http`.
- Before database reset, the server publishes `io.dirextalk.room.profile` direct-room account-deleted dissolve state for accepted direct contacts so peers hide the deleted account, leaves accepted direct-contact rooms, dissolves groups/channels owned by the portal owner, leaves groups/channels where the owner is only a member, and deactivates local owner/agent Matrix accounts.
- If a critical leave/dissolve/deactivation step fails, the server returns an error and does not clear databases.
- On success, the server writes a non-secret deprovision marker to the portal credentials file, clears configured local databases, clears in-memory product/session state, and schedules local message-server shutdown. It does not destroy AWS/cloud instances.

Response includes `status: "deprovisioned"`, operation counts such as `contacts_left`, `groups_dissolved`, `channels_dissolved`, `accounts_deactivated`, and `database_reset: true`.

## 2026-06-22 Matrix-First Cleanup

This pass removes the remaining ambiguous compatibility surface from current code, examples, and skills.

Breaking removals and contract changes:

- `portal.setup` is no longer a P2P action. Portal initialization is automatic; clients use `portal.bootstrap`, `portal.auth`, `portal.status`, and `portal.password`.
- `P2P_BOOTSTRAP_CREDENTIALS_FILE` is no longer a compatibility alias. Use `P2P_PORTAL_CREDENTIALS_FILE`.
- Removed legacy Matrix product state is no longer generated, read, or projected. Current product state is `io.dirextalk.room.profile`, `io.dirextalk.member.policy`, and `io.dirextalk.join_request`.
- Public channel approval no longer exposes Matrix invite as the product workflow. Approval writes `io.dirextalk.join_request status=approved`; the requester homeserver performs Matrix join.
- New public internal action `channels.public.join_result` carries owner-node approval results to the requester node. Params: `room_id`, `channel_id`, `user_id`, `status`, `reason`, `server_names`, and `request_id`.
- Public channel join response status is one of `pending`, `rejected`, `approved`, `joining`, `joined`, or `join_failed`.
- Added protected action `agent.matrix_session.create` on `POST /_p2p/command`. It initially required bearer `access_token`; current servers accept owner `access_token` or `agent_token`. It returns a Matrix Client-Server session: `access_token`, `device_id`, `user_id`, and `homeserver`.
- `portal.bootstrap`, `portal.auth`, and `portal.password` return one setup state field: `initialized`. It is `false` while the generated initial password is still in use and becomes `true` after `portal.password` changes that password. Clients should store `access_token` and route by `initialized`; profile completion is independent.

The live P2P body-action contract is generated from `p2p/serviceapi.ActionSpecs` into `docs/product-action-contract.json`. Public actions are `portal.bootstrap`, `portal.auth`, `portal.status`, `contacts.reactivate`, `rooms.reactivate`, `reports.submit`, `channels.public.search`, `channels.public.get`, `channels.public.join_request`, `channels.public.join_result`, and `users.public_channels`. `rooms.reactivate` and `channels.public.join_result` are public HTTP-only node-to-node callbacks and are not valid WS `client.request` actions.

## Current Pass

This pass completes the Matrix-only ordinary message migration for Dirextalk product rooms. There is now one ordinary message source of truth: Matrix Client-Server event storage and timelines. P2P product APIs keep product metadata, contact/group/channel state, channel post/comment projections, calls, favorites, follows, Agent configuration, and bootstrap metadata.

Breaking removals from the P2P body-action surface:

- `sync.messages`
- `sync.unread`
- `search`
- `rooms.send`
- `rooms.send_media`
- `rooms.messages.delete`
- `rooms.messages.delete_batch`
- `rooms.messages.delete_range`
- `rooms.messages.recall`
- `contacts.export`
- `contacts.download`
- `contacts.import`

The removed actions are absent from `p2p.Service.Handle` and the dual-node smoke business flow. Calls to those names are treated as unknown P2P actions. Clients must not use them as deprecated compatibility paths.

## Matrix Message Contract

Ordinary private chat, group chat, and channel chat use Matrix Client-Server APIs:

- Send text/media: `PUT /_matrix/client/v3/rooms/{roomID}/send/m.room.message/{txnID}`
- Incremental sync and unread data: `GET /_matrix/client/v3/sync`
- Offline/history reads: `GET /_matrix/client/v3/rooms/{roomID}/messages`
- Search: `POST /_matrix/client/v3/search`
- Distributed recall: Matrix redaction routes
- Per-user local hide/clear: `POST /_matrix/client/v1/io.dirextalk/rooms/{roomID}/local_delete`

`local_delete` request forms:

```json
{ "event_ids": ["$event:server"] }
```

```json
{ "clear": true }
```

`event_ids` hides specific Matrix events from the requesting user's Matrix read paths. `clear=true` hides room events through the current sync stream position. Neither form sends a Matrix redaction or changes other users' history.

The local hide state is persisted in syncapi storage and filtered from:

- `/sync`
- `/rooms/{roomID}/messages`
- `/rooms/{roomID}/event/{eventID}`
- `/rooms/{roomID}/context/{eventID}`
- `/rooms/{roomID}/relations/...`
- `/search`

## Product Room Classification

Room classification remains a product metadata concern and is not rebuilt from message history:

- Direct/private chats: `contacts.list`, `sync.bootstrap.contacts`, pending friend requests, and Dirextalk direct room profile state.
- Groups: `groups.list`, `sync.bootstrap.groups`, pending group invites, and `io.dirextalk.room.profile` with group type.
- Channels: `channels.list`, `sync.bootstrap.channels`, pending channel notices, public channel actions, and `io.dirextalk.room.profile` with channel type.

`sync.bootstrap.rooms` was removed. `sync.bootstrap` now returns product metadata sections only; clients should combine those sections with Matrix room timelines from `/sync` instead of consuming a P2P-derived room list.

## Channel Posts And Comments

Channel post/comment product content still uses Matrix events, but carries product classification:

- `p2p_kind=channel_post` projects to `p2p_channel_posts`.
- `p2p_kind=channel_comment` projects to `p2p_channel_comments`.
- Matrix ProductPolicy enforces channel owner/comment rules before write. ProductCore group/channel roles are owner/member only.
- Channel post/comment recall uses Matrix redaction and removes the product projection.

Ordinary `m.room.message` events without channel post/comment product markers are not mirrored into P2P message tables and do not emit P2P ordinary-message SSE events.

## P2P Product Surface

The product route contract remains:

- `GET /_p2p/health`
- `POST /_p2p/query`
- `POST /_p2p/command`
- `GET /_p2p/events`
- `GET /.well-known/portal/owner.json`

At that point, protected product actions required bearer `access_token`, while `agent_token` was accepted only for fixed `mcp.*` actions and `GET /_p2p/events`. Current servers have removed `GET /_p2p/events` and accept `agent_token` only for product body-action `agent.matrix_session.create` and standard `POST /mcp`. Current public actions are generated into `docs/product-action-contract.json` and include `portal.bootstrap`, `portal.auth`, `portal.status`, `contacts.reactivate`, `rooms.reactivate`, `reports.submit`, `channels.public.search`, `channels.public.get`, `channels.public.join_request`, `channels.public.join_result`, and `users.public_channels`.

Current action metadata is generated into `docs/product-action-contract.json`.

## ProductCore Conversation Contract

`conversations.list` and `conversations.get` expose ProductCore conversation identity for clients. The response keeps the existing stable fields:

- `conversation_id`
- `matrix_room_id`
- `kind`
- `lifecycle`
- `peer_mxid`
- `title`
- `avatar_url`
- `last_event_id`
- `last_activity_at`
- `projection_state`
- `projection_reason`

This pass adds hydrated membership and relationship fields to the conversation view:

- `member_count`: direct conversations return `2`; group and channel conversations return the joined member count from ProductCore membership state when available.
- `membership`: the current owner membership in this conversation. Direct accepted contacts map to `join`; pending direct contacts map to `pending`.
- `relationship_status`: direct-contact relationship state such as `accepted`, `pending_inbound`, or `pending_outbound`.
- `role`: current owner role in the conversation, for example `member` or `owner`.
- `hydration_state`: `ready` when ProductCore has enough state to open the conversation, otherwise `pending`, `conflict`, or `failed`.
- `hydration_reason`: machine-readable reason when hydration is not ready, for example `owner_membership_missing`.
- `capabilities`: server-derived operation flags. Current keys are `open`, `send`, `send_media`, `call`, `invite`, `manage_members`, `rename`, `remove_members`, `leave`, `delete`, `post_create`, `comment_create`, `reaction_toggle`, `post_recall`, `comment_recall`, and `comments_enabled`. Group/channel management and post capabilities are true only when the current owner is joined with role `owner`.

Clients should use these ProductCore fields instead of inferring room type or owner membership from Matrix timeline shape, display names, or member-count text.

## ProductCore Create/Join Mutation Result

`groups.create`, `groups.join`, and `channels.join` now return the ProductCore conversation created or hydrated by the mutation path:

- `operation`: `{action, status, room_id, conversation_id}` for the completed mutation.
- `conversation`: the same `ConversationView` shape returned by `conversations.list/get` when a conversation record exists for the created or joined room.

Clients should open the returned `conversation.conversation_id` / `conversation.matrix_room_id` directly after a successful create or join. They should not reconstruct a chat route from group/channel names, member counts, or Matrix room aliases.

## Contact Re-Request Semantics

`contacts.request` is idempotent by `mxid`. When a non-deleted contact already exists for the same peer, the action returns the stored contact and does not create a second direct Matrix room. Existing pending contacts re-send a pending invite in the stored room. Existing accepted contacts normally return unchanged; when `remote_node_base_url` is supplied and the peer node reports that it no longer retains the relationship, the contact becomes `pending_outbound` in the stored room and waits for peer approval.

Inbound direct invite projection now treats the Matrix membership event sender as the authoritative requester identity. `io.dirextalk.room.profile` stripped-state fields such as `requester_mxid` and `domain` cannot override the projected `peer_mxid` or peer domain; if they conflict with the event sender, profile display fields from that direct profile are ignored. This prevents a third user from making a pending friend request appear to come from another Matrix user or domain.

`contacts.request` restores an existing `deleted` contact for the same peer only when the peer still retains the accepted relationship. The response preserves the original `room_id`, refreshes supplied display/domain metadata, returns `status: "accepted"`, and rejoins the original direct Matrix room through the P2P transport when transport is configured. If the requester has left the old invite-only direct room, the requester node calls the peer node `contacts.reactivate`; the peer node re-invites the requester only when it still has an accepted contact for the same `peer_mxid` and `room_id`. This lets the side that deleted a contact intentionally restore that old direct conversation without peer approval. If the peer node has an existing non-accepted contact for the same requester and old `room_id`, `contacts.reactivate` records `pending_inbound` on the peer node and returns `status: "pending_inbound"`; the requester node preserves the original `room_id`, returns `pending_outbound`, does not try to invite from a user that already left the direct room, and does not join or restore chat until the peer accepts. If the peer no longer has a matching contact record, `contacts.request` preserves the original `room_id`, returns `pending_outbound`, sends a direct invite for that old room, and waits for peer acceptance. Requests to add the local owner and self `contacts.reactivate` calls are rejected with `400`.

If a node still has an accepted contact for the real Matrix sender and receives a fresh direct invite for a different room, it does not create a new pending contact from the supplied invite metadata. Instead, it re-invites that real sender to the retained accepted `room_id`, allowing a peer whose local contact data was deleted or rebuilt to recover the old direct room. `contacts.reactivate` also ignores caller-supplied profile fields for non-accepted retained contacts; missing local display/domain values are derived from `requester_mxid`.

When `contacts.request` is called again for an existing `pending_outbound` peer, the requester node now re-sends a direct Matrix invite to the stored direct room instead of only returning the cached contact. A target node that previously stored the peer as `rejected` now accepts the new direct invite projection and changes the contact back to `pending_inbound`, so pending friend request notices can appear again.

When a direct invite projection creates or reopens a local `pending_inbound` contact, `/_p2p/events` now emits `contact.requested` with `room_id`, `peer_mxid`, `display_name`, `avatar_url`, `domain`, and `status: "pending_inbound"`. Existing pending/accepted contacts remain de-duplicated and do not emit another contact request event.

`contacts.request` accepts optional friend-request text as `remark` and also recognizes `request_message`, `message`, or `reason` for compatibility. Pending contact responses, `contacts.list`, `sync.bootstrap.contacts`, `sync.bootstrap.pending.friend_requests`, and `contact.requested` events expose the text as `remark` while the request is pending. The value is carried in native direct-room profile state for invite projection and is cleared when the contact becomes accepted so it is not reused as a contact display remark or conversation title.

`contacts.requests.accept` is idempotent for an already accepted contact, but first confirms the local owner's Matrix membership. A confirmed join returns the stored contact without another Matrix join; a stale accepted projection re-enters the recovery path and repairs the direct-room/contact/conversation state.

P2P contact persistence now enforces one row per `peer_mxid`. Existing duplicate contact rows are compacted during migration, preferring `accepted`, then `pending_inbound`, then `pending_outbound`, then rejected/deleted records.

Contact responses now expose peer avatar metadata through `avatar_url`. This applies to `contacts.list`, contact mutation responses, and the `contacts` array returned by `sync.bootstrap`. Direct-contact conversations derived from contact records also carry the same `avatar_url` so clients can render the peer avatar consistently after bootstrap or contact mutations.

Contact mutation responses now include a ProductCore `operation` object and attach the hydrated direct `conversation` when the contact has a `room_id`. This applies to `contacts.request`, `contacts.reactivate`, `contacts.requests.accept`, `contacts.requests.reject`, `contacts.requests.delete`, `contacts.delete`, and `contacts.update`. Clients should consume the returned `conversation_id` / `matrix_room_id` instead of reconstructing a direct chat route from peer display names or Matrix direct-room heuristics.

## Group Invite Reject And Stored Member Role Semantics

`groups.invite.reject` records the current local user's pending group invite as `membership: "reject"` and returns `{status: "rejected", member}`. Rejected group invites are hidden from `groups.members` and `groups.list`, matching the first-version ProductCore rule that hidden memberships (`leave`, `remove`, `reject`, `ban`) are not ordinary visible members.

Group and channel member mutations now load the existing ProductCore member record before applying leave/remove/mute/unmute/reject transitions. Owner protection is therefore based on persisted `role` and `membership`, including after a service reload backed by PostgreSQL, instead of relying on an in-memory default member record. ProductCore group/channel roles are owner/member only.

Group/channel invite and member mutation responses now include a ProductCore `operation` object and attach the hydrated `conversation` when the mutated room has a `p2p_conversations` record. This applies to `groups.invite`, `groups.invite.reject`, `groups.leave`, `groups.member.remove`, `groups.member.mute`, `groups.member.unmute`, `channels.invite`, `channels.leave`, `channels.member.remove`, `channels.member.mute`, `channels.member.unmute`, `channels.join_request.approve`, and `channels.join_request.reject`.

## Client Migration Notes

Clients should align as follows:

- Message list, offline history, search, unread, and recall use Matrix SDK calls.
- Local clear-history/delete-for-me uses the Dirextalk Matrix `local_delete` extension.
- Conversation placement still uses product metadata: contacts for private chats, groups for groups, channels for channels.
- `sync.bootstrap` is still useful for product metadata and pending notices, but no longer provides a `rooms` array.
- Agent API allow-lists must not include removed message/search/backup actions.

## Updated Artifacts

- P2P action registry and fixed Agent MCP allowlist.
- P2P storage migration dropping the legacy ordinary-message mirror table.
- Syncapi local hide storage and Matrix read-path filtering.
- Roomserver projector rules for ordinary messages, channel posts/comments, reactions, and redactions.
- Dual-node smoke script using Matrix send/history/search/redaction/local_delete.
- Manual example set with removed P2P actions deleted and `local_delete` examples added.
- Feature inventory and implementation notes.

## 2026-07-10 — Server release control v1

Added three protected owner Product actions:

- `client.version.report` over HTTP or owner WS, with required stable `client_version` and optional short `build_number`/`platform`. The server normalizes an omitted `v` prefix and stores the report only on the current portal device row.
- `release.v1.status` over HTTP or owner WS. It returns additive running build/schema and reported client fields plus updater-authoritative `available`, `release_available`, `update_available`, `discovery_status`, `compatibility`, stable `reasons`, `operations`, and release metadata. Updater connection failure is a successful parseable unavailable response.
- `release.v1.apply` over HTTP only. It accepts exactly `plan_token`, UUID `idempotency_key`, and `confirm="apply_release_change"`; all unknown and infrastructure-shaped fields are rejected with structured code `release_apply_invalid_params`.

Owner access tokens are used only at the Product API boundary. The Unix updater client reads its fixed mounted control token file, and neither owner/control/plan/job tokens are written to release storage or errors. Account deletion now sets updater desired state `deprovisioned` before destructive work and aborts if that step fails.

Hardening follow-up: client reports now carry the authenticated portal device/session from HTTP authorization or WS ticket creation and reject stale sessions with `client_session_stale`. Persistence uses a narrow device-CAS update, while same-device full portal saves preserve client build fields and device switches clear them atomically. Status always overwrites updater `current_version`/`client_version` echoes with local facts. If account deletion fails after setting `deprovisioned`, the backend best-effort restores `running`; a failed restoration returns `account_delete_watchdog_restore_failed` without upstream details.

Same-device password-rotation follow-up: `portal.password` serializes its access-token/session-generation mutation and portal persistence with `client.version.report` validation/CAS. The lock is released before Matrix-session refresh, preventing both stale-report persistence and recursive mutex acquisition without changing the public action envelope.

Watchdog follow-up: `release.v1.status` now includes an additive `watchdog` object with `status`, derived `degraded`, optional RFC3339 `cooldown_until` / `last_observed_at`, and stable `error_code`. The backend allowlists these fields from the Unix updater response, normalizes timestamps, derives `degraded` from the allowlisted status, and never forwards repair attempt history, service/image input, control data, or updater-only fields. Older or unavailable updater responses map to `watchdog.status="unknown"` with no repair operation inferred by the client.

## 2026-07-15 — Managed cloud Service lifecycle approval v1

`cloud.services.operation.plan` and `cloud.services.operation.approve` are now
enabled for owner-authenticated HTTP requests only. Plan accepts exactly
`service_id`, `expected_revision`, `operation` (`start`, `stop`, or `restart`)
and a UUID idempotency key. Approve replaces `operation` with the exact
device-signed approval returned by Plan plus a new UUID idempotency key.

The server derives the installed manifest, compiled artifact, opaque Worker
action, root requirement, timeout and checkpoint sequence; callers cannot
supply them. The approval also binds the expected Service status and exact
Service/Deployment revisions. The approve response is
`{service, operation, job}`. Job progress uses existing `cloud.job.changed`
events. Successful stop introduces `service_status: "stopped"`; successful
start/restart preserves `experimental` until explicit management acceptance
(managed Recipes return `active`), terminal failure returns `degraded`, and the
resource remains active and billable. Agent tokens and public MCP have no plan,
approve or lifecycle mutation capability.

## 2026-07-15 — Retained encrypted cloud Service backup v1

`cloud.services.operation.plan` now also accepts `operation: "backup"`.
Its confirmation carries a distinct `service_backup` approval that binds the
current Service/Deployment revisions, Connection, Recipe digest, tracked EC2
instance, complete EBS volume set and `retention_policy: "manual"`.
`cloud.services.operation.approve` accepts that exact device-signed approval
and returns `{service, operation: "backup", backup, job}`. Both actions remain
owner-authenticated and HTTP-only; Agent and public MCP permissions are
unchanged.

`cloud.services.list/get` add an optional `backups` array to each Service.
Each item contains `backup_id`, `service_id`, `deployment_id`, `status`,
`retention_policy`, optional retained `image_id`/`snapshot_ids`, its own
revision and timestamps. Backup progress is reported by the returned Job and
existing `cloud.job.changed` events. Backup success or failure does not change
the Service status or Deployment resource axis; retained AMI/snapshot cleanup
and restore are separate future approved operations.

Terminal backup completion now advances the enclosing Service revision and
emits a strict `cloud.service.changed` summary with the terminal `backups`
array; this is an aggregate projection change, not a Service maturity or
resource-axis transition. `cloud.job.changed` also accepts the existing
`kind: "backup"` Jobs. The Flutter Service detail page renders backup status,
manual retention, retained AMI and encrypted snapshot count, but deliberately
contains no restore or backup-delete control.

## 2026-07-15 — Original-instance retained-volume restore v1

Three owner-authenticated, HTTP-only actions add the separately approved
in-place restore flow: `cloud.services.restore.plan`,
`cloud.services.restore.confirmation.prepare`, and
`cloud.services.restore.approve`. The server and read-only Connection Stack
derive the current Service/Deployment/backup revisions, original EC2 instance,
Region/AZ, exact volume/snapshot/device mapping, and disclosed EBS estimate.
The client can only sign the returned five-minute deterministic approval; it
cannot submit AWS mutation parameters. Agent tokens, WebSocket requests, and
public MCP cannot call these actions.

Approval atomically records a restore Job and durable command ledger. The
typed Connection Stack mutation and Orchestrator consumer each have an
independent default-off gate. When enabled in a separately authorized stage,
the Stack creates replacement volumes idempotently, stops the original
instance, swaps exact device mappings, reads back the result, and attempts to
reattach the retained originals on failure. AWS mapping evidence remains
`verifying` until an independent Worker semantic-readiness check succeeds;
fallback and unverified recovery never become success.

`cloud.services.list/get` and strict `cloud.service.changed` summaries add an
optional `restores` array. Each entry exposes restore/plan/backup identity,
status, revision, timestamps, and the original/replacement volume IDs so all
retained billable resources remain visible. An active or blocked restore
excludes new start/stop/restart, backup, restore, and destroy approvals. The
Flutter Service detail flow performs the device signature, persists only
non-secret resumable metadata, displays progress and retained-volume charges,
and never implies automatic cleanup.

## 2026-07-15 — Experimental-to-managed cloud Service acceptance v1

Two owner-authenticated, HTTP-only actions add the separately approved
maturity transition: `cloud.services.management.plan` and
`cloud.services.management.approve`. Plan accepts exactly `service_id`, the
current `expected_revision`, and a UUID `idempotency_key`. It derives, persists
and returns a five-minute confirmation; the client cannot submit evidence or
cloud-resource parameters. Planning transitions a matching `experimental`
Service to `awaiting_management_acceptance` and transitions its Recipe only
when that Recipe is still `experimental`; an already `managed` Recipe is bound
without a revision change. This fences later lifecycle, backup, restore or
destroy work from racing the signed evidence.

The deterministic-CBOR `ServiceManagementAcceptanceApprovalV1` binds the
current Service/Deployment/Recipe revisions and Cloud Connection, installed
manifest and official source artifact digests, the exact semantic-readiness
evidence and Stack-observation digests, health/liveness/readiness and
lifecycle/upgrade/rollback contracts, volume/data/secret slots, an available
retained backup, its succeeded same-instance restore, a successful restart
after that restore, and the exact tracked instance/volume/network destroy set.
Approval requires the registered device's Ed25519 signature and unchanged
evidence. It atomically publishes `service_status: "active"`, Recipe
`maturity: "managed"`, an immutable maturity-metadata revision referencing the
same verified canonical Recipe content/digest, and a durable approved
acceptance result. If that content-addressed Recipe is already `managed`, only
the later Service advances and the Recipe revision remains unchanged. Prepare
and approve retries are independently idempotent;
stale revisions, expired challenges, changed evidence, signature/key mismatch
or conflicting work fail closed without promoting maturity. Agent tokens,
public MCP and WebSocket `client.request` cannot call either action.

## 2026-07-17 — Managed acceptance delegated to independent Agent

The existing owner HTTP-only `cloud.services.management.plan/approve` request
and response JSON remain unchanged, but Message Server now acts only as a
compatibility façade. It resolves the owner-scoped legacy
`service_id -> deployment_id/revision` mapping and active device key, then
delegates challenge creation, approval and exact operation readback to Agent.
Agent revalidates every Service, Deployment, Connection, Plan, Recipe, health,
backup, restore, restart and provider-resource binding.

Message Server reconstructs the legacy approval only from Agent's rich scope
and rejects it unless Agent's signing CBOR is byte-identical to the legacy
deterministic payload and its scope digest is that payload's SHA-256. Approval
forwards the exact signed acceptance, and a lost response recovers only the
same `acceptance_id` operation; it never replays the signature. Only a
successful Agent operation with complete compatibility Service, Recipe and
acceptance views can produce the legacy response. These actions no longer
create a local acceptance, outbox record, Job or ProductCore event.

## 2026-07-17 — Agent-owned managed preparation façade

Three owner-authenticated HTTP-only ProductCore actions add the no-store
compatibility façade `cloud.services.managed_preparation.prepare/approve/get`.
Prepare accepts exactly `service_id`, current Deployment `expected_revision`,
positive `cost_alert_amount_minor`, and a UUID `idempotency_key`. Approve
accepts the same Service/revision boundary, the returned full approval scope,
and a distinct UUID idempotency key. Get accepts exactly `service_id`,
`expected_revision`, and `operation_id`.

Message Server derives only the owner-scoped legacy Service-to-Deployment
mapping and active device signer. Agent owns the operation, fixed phase order
`restart -> backup -> restore_create -> restore_swap -> semantic_health ->
finalize`, cloud facts, health, cost and stack evidence. The façade reconstructs
the versioned deterministic-CBOR signing payload and requires byte equality
with Agent's raw payload before returning it. The full provider-bound scope is
transported only so the client can perform the same verification and signature;
provider identifiers must not be rendered, logged, published or projected.

Approve performs exact owner-bound Get before sending a signature. Only a clean
NotFound may send it once; an unknown result is recovered only through Get and
never by resending the signature. Revision gaps require a replacement prepare.
The public operation exposes de-secreted phase/step progress, and exposes
health/cost/stack digest, revision and observation time only on a succeeded
operation. Message Server persists no preparation operation, Job, outbox or
event and does not publish this façade through WebSocket or MCP.

## 2026-07-18 — Managed preparation V2 bounded snapshot scope

The same façade additionally accepts the paired Agent schemas
`dirextalk.agent.cloud.service-operation-scope/v2`,
`dirextalk.agent.cloud.service-operation-challenge/v2`, and
`dirextalk.agent.cloud.service-operation-signing-payload/v2`. Every V2 volume
binds its safe snapshot operation key, source-volume scope digest, and finite
`snapshot_max_retention_seconds` (one second through 365 days) into the exact
device-signing CBOR. Message Server rejects a mismatched schema pair, missing
or out-of-range V2 term, and recomputes the V2 payload before exposing it or
forwarding a signature. V1 remains accepted only with all three V2-only keys
absent; its deterministic CBOR projection is unchanged.

## 2026-07-15 — Selectable private Recipe and scoped service secrets v1

`cloud.goals.create` now accepts the optional pair `recipe_id` and
`expected_recipe_revision`. Both fields must be present together. The owner may
select only a current private Recipe; the server binds its digest and revision
to the Goal/Plan and revalidates them when research is claimed and committed.
Omitting both fields preserves the previous research flow. Native Agent tools
cannot supply either field. `cloud.recipes.get` now returns a strict owner-only,
de-secreted Recipe detail used by the Flutter Recipe page; list summaries and
public MCP projections remain unchanged.

The new owner-authenticated, HTTP-only `cloud.secrets.bootstrap.plan` accepts
exactly `deployment_id`, `slot_id`, `expected_revision`, and a UUID
`idempotency_key`. It derives the current Plan, Recipe, verified compiled
artifact, active Recipe task, manifest, `secret_ref`, purpose and delivery
under one transaction and returns `{confirmation, stack_base_url}`. The URL is
transient and restricted to one HTTPS origin plus an optional canonical API
Gateway stage. The confirmation contains a ten-minute
`ServiceSecretApprovalV1` for device signing; it contains no value, upload
token, encryption key, ciphertext, provider path or provider version. Agent,
MCP and WebSocket requests cannot invoke the action.

Flutter signs that proof and sends X25519/HKDF-SHA256/AES-256-GCM ciphertext
directly to fixed Connection Stack create/upload/complete routes. The upload
flow is memory-only, cancellable and response-loss idempotent. It suppresses
ProductCore/API logging, local persistence, autofill and personalized IME
learning, and clears plaintext and capability buffers on completion,
cancellation or late response. Re-entering a different value cannot silently
replay an earlier encrypted envelope.

`cloud.services.destroy.plan/approve` keeps its request shape but its returned
device approval now includes an optional canonical `secret_refs` array derived
only from the locked Plan/Recipe/artifact/manifest. Flutter displays only its
count. The signed `deployment.destroy` command and verified receipt bind the
same refs. A service with no secret slots preserves the previous JSON and CBOR
golden. Success now means the Stack has read back the approved EC2 instance,
ENIs, EBS volumes and deterministic Secrets Manager resources as absent and
has removed their non-secret binding ledger. Access denial remains blocked and
cannot become `verified_destroyed`.

## 2026-07-17 — Agent Cloud Task milestone and event relay v1

Remote `agent.chat` and `agent.chat.stream` now surface an additive
`cloud_task` milestone only for an owner-scoped Cloud Dialogue response that
has the same stable conversation ID, exactly one canonical task UUID, and no
related Plan. Its fixed four-field shape is
`{schema, task_id, conversation_id, state:"research_queued"}`; the Message
Server never fabricates a Plan ID, deep link, lifecycle state, or approval.
Ordinary remote Chat receives no milestone even if an Agent response contains
task references.

The durable Agent event relay additionally projects only
`cloud.task.changed` (`cloud_task`) and `cloud.step.changed` (`cloud_step`)
beside the existing Plan event. Each accepted v1 summary is strict and
de-secreted: schema version, owner/task/(step) IDs, fixed execution/outcome/
stage/error enums, optional canonical verified related Plan ID, matching
revision, and UTC update time. Unknown fields, raw Worker material and invalid
values fail before cursor advancement; only the reviewed fields plus the source
Agent instance UUID reach ProductCore.
