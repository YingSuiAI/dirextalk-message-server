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
  database role and uses a private, exact-HTTPS research endpoint that returns
  only typed `ResearchOutput`; the binary has no Matrix config, model key, AWS
  SDK, Docker socket, or migration capability. It is intentionally not part of
  the default compose stack until an independently authenticated private
  researcher endpoint and its least-privilege database role are deployed. It does not yet ship a Worker or
  AWS executor.
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

## Current first vertical slice

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

The outbox contains the private natural-language goal for the external
planner. ProductCore websocket events and `cloud.events.list` carry summaries
only; they never copy the goal prompt, AWS credentials, session tokens,
pairing URLs, QR payloads, or service secrets.

The implementation persists versioned plan/recipe/quote artifacts, jobs and
job steps, plus goals, plans, connections, deployments, services, recipes,
alerts, Cloud audit events, private research outbox records, and projection
outbox records. Connection/Deployment/Service writers are deliberately not
part of the message-server owner action surface.

## ProductCore actions

All actions require the owner access token. Read actions may use owner HTTP or
ready realtime `client.request`; every create, approval, pairing, service
operation, and destruction action is HTTP-only. `agent_token`, old owner
tokens on `/mcp`, and websocket `client.request` cannot invoke an HTTP-only
Cloud mutation.

| Action family | Current behavior | Transport |
| --- | --- | --- |
| `cloud.bootstrap` | returns owner projections (`goals`, `plans`, `jobs`, `connections`, `deployments`, `services`, `recipes`, `alerts`) | HTTP + WS request |
| `cloud.{connections,plans,deployments,services,recipes}.list/get` | typed owner-only projection reads | HTTP + WS request |
| `cloud.events.list` | de-secretsed durable Cloud audit events; `limit` is 1–200 | HTTP + WS request |
| `cloud.goals.create` | creates a `researching` Goal/Plan and a planner outbox request | HTTP-only |
| `cloud.connections.role_plan`, `cloud.plans.approve`, `cloud.deployments.pairing.resume`, `cloud.services.*.plan/approve` | declared high-risk contracts; return `503 cloud_orchestrator_unavailable` until the independent control plane is installed | HTTP-only |

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
only fixed `cloud.goal.changed`, `cloud.plan.changed`, and
`cloud.job.changed` schemas, and calls its local events module with
`dedupe_key=cloud-event:<cloud_event_id>`. It acknowledges only after that
append. A crash between append and acknowledgement is therefore safe to replay
without duplicating an owner event. Unknown types, extra fields, malformed
JSON, credential-shaped text, raw Worker logs, Goal prompts, and secret
material are terminally rejected from the websocket projection.

Research progress is a Job axis, not a guess derived from a Plan status. The
initial Job is `research_queued`; a fenced claim records `research_leased`, a
classified retry records `research_retry_scheduled`, and terminal outcomes are
`quote_ready` or `research_failed`. Only these de-secretsed checkpoint and
error-code values enter Job events/status; raw provider replies, prompts, and
logs stay private. A failed research Job may leave its Plan at `researching`:
the Job outcome is the durable terminal fact until a later explicit re-plan or
retry contract is introduced.

`cloud-orchestrator` uses a bounded attempt timeout shorter than each lease;
it defers a timed-out research attempt rather than accepting a late result.
It reads its PostgreSQL URL only from the regular file named by
`CLOUD_ORCHESTRATOR_DATABASE_URL_FILE`, never a CLI flag or log line. Its
research endpoint is `CLOUD_ORCHESTRATOR_RESEARCHER_URL` and must be exact
HTTPS `/v1/cloud-research` with no user info, query, fragment, or redirects.
The repository includes `Dockerfile.cloud-orchestrator`, a distinct non-root
image that contains only this binary and CA certificates; it must be given a
read-only root filesystem, its one DSN secret file, and no message-server
volumes, Docker socket, AWS credentials, Matrix configuration, or Agent data.

## Approval and lifecycle gate

The domain contract package now defines deterministic-CBOR `plan_hash` and a
signed challenge that bind all of:

`plan_hash + revision + quote_id + cloud_connection_id + recipe_digest + resource/network/secret/integration scope + expiry`.

The ProductCore approval action remains disabled until the independent
Orchestrator, device-key registry, one-time challenge storage, and Dart
golden-vector verification are wired to that contract.

The UI label is **“确认创建并开始计费”**. Price and budget fields remain
estimates/alerts: they do not promise an AWS billing hard stop. Failure,
cancellation, successful installation, and `waiting_user_pairing` retain
resources until the owner explicitly plans and approves a verified destroy.
Public ingress remains a separate plan and confirmation.

## Explicitly not enabled yet

The first slice does not upload credentials, deploy a Connection Stack, price
instances, approve a plan, create an EC2 instance, install a service, expose a
network endpoint, or destroy a resource. It ships the independently buildable
research process and its private event relay, but does not yet deploy a
researcher endpoint, Worker AMI, Broker executor, or AWS integration test.
Those transitions must be implemented through the typed Connection Stack/Broker
path; neither the Eino Agent tool, external MCP, nor the message-server gains
arbitrary AWS access.
