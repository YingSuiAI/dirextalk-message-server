# Product Agent Main Semantic Port Design

## Status

Approved in conversation on 2026-07-14. This design upgrades the previous
`codex/product-agent-bridge-20260709` behavior onto current Message Server
`main` without modifying or pushing to `main`.

## Problem

The previous Product Agent bridge is one feature commit ahead of its old base
but 175 commits behind current `main`. It modified monolithic projector and
plugin files that current main has deleted or replaced with modular packages.
Cherry-picking or merging that commit would restore obsolete ownership and risk
regressing current Matrix, ProductCore, Native Agent, Skills, and MCP behavior.

## Branch And Ownership Boundary

- Work only on `codex/agent-main-upgrade`, created from current `origin/main`.
- Keep `codex/product-agent-bridge-20260709` unchanged as the behavioral
  reference.
- Never push, merge, or commit directly to `main`.
- New Product Agent compatibility code lives in an Agent-owned internal module.
- Existing projector, transport, service, setup, and generated contract files
  receive only narrow ports, registration, or wiring changes.
- Do not restore deleted monolithic files or alter unrelated product modules.
- Do not build, publish, or deploy a Message Server image to codex1.

## Preserved Product Agent Contract

When explicitly configured, Message Server continues to:

1. Recognize a real Matrix Agent room.
2. Forward one eligible owner text/action event to Product Agent
   `/v1/message-server/new-message`.
3. Include the Matrix event id as `message_id`, the real room id as
   `conversation_id`, and only bounded Agent-authorized context/config.
4. Ignore messages from `@agent`, system senders, other rooms, unsupported event
   types, redactions, and empty bodies.
5. Send the immediate Product Agent reply as `@agent:<server>` through the
   current `dirextalktransport.Transport` boundary.
6. Preserve typed `direxio.agent_action_result.v1` content without emitting raw
   JSON as a second timeline message.
7. Keep Product Agent memory and Prompt Skill compatibility behind explicit
   owner-authenticated `agent.product.*` actions where the Flutter Online Agent
   surface needs them.

Long-task final delivery remains owned by Product Agent's PostgreSQL Outbox and
Matrix sender. Message Server must not create a second asynchronous result
delivery path.

## Component Design

### Product Agent Module

A new internal Product Agent module owns HTTP request/response types, URL
validation, timeouts, safe errors, and optional configuration. The module is
disabled when no Product Agent URL is configured. It never receives or logs
owner access tokens, Matrix access tokens, model keys, database credentials, or
AWS credentials.

The preferred configuration name uses the current `DIREXTALK_` namespace while
the existing `DIREXIO_PRODUCT_AGENT_URL` value remains a compatibility alias for
the previously deployed branch.

### Matrix Event Intake

Current projector/consumer code receives a narrow Agent-message sink port. The
sink is invoked only after the event has been identified as an eligible message
in the configured Agent room. The root service/setup layer supplies the Product
Agent implementation; non-Agent projectors do not import Product Agent HTTP
code.

The Matrix event id is the upstream turn identity. The Product Agent request is
bounded and contains no arbitrary room history. Replayed events reuse the same
identity so downstream task and tool idempotency remains stable.

### Matrix Reply

The module converts a successful Product Agent response into one
`dirextalktransport.SendMessageRequest` with the Agent MXID as sender. Typed
card fields become Matrix event content; the fallback `body` remains short and
human-readable. Empty, ignored, or failed responses do not create timeline
messages. Errors are sanitized and do not expose response bodies containing
secrets.

### Management Actions

Product Agent compatibility actions are registered through the current Agent
module/action registry and require owner auth. Their public namespace is kept
separate from Native Agent actions:

- `agent.product.memory.list`, `agent.product.memory.save`, and
  `agent.product.memory.delete` proxy the existing `/v1/agent/memory` contract.
- `agent.product.skills.list`, `agent.product.skills.create`,
  `agent.product.skills.update`, and `agent.product.skills.delete` proxy the
  existing `/v1/agent/skills` contract.

Each action validates bounded fields before calling the internal Product Agent
service. These actions do not replace `agent.skills.*`, Native Agent knowledge,
or Native Agent memory behavior; the Flutter mode/provider layer selects the
effective backend.

## Failure And Isolation Rules

- Product Agent disabled or unavailable must not break Matrix message
  projection, sync, or unrelated ProductCore actions.
- Product Agent replies never re-enter the bridge because Agent senders are
  rejected at intake.
- The bridge never handles human direct/group/channel messages.
- Product Agent HTTP calls have bounded timeouts and response sizes.
- Main's Native Agent, Online Agent, MCP, Skills, storage, policy, and realtime
  contracts remain authoritative and unchanged unless a focused compatibility
  action is explicitly added.

## Verification

Focused tests cover disabled configuration, eligible Agent-room owner message,
wrong room, Agent sender loop prevention, unsupported event type, stable event
identity, timeout/error isolation, one Matrix reply, typed card content, and
the exact `agent.product.*` memory/skill action auth and validation. Then run:

```text
go test ./p2p ./p2p/internal/projector ./p2p/internal/agent -count=1
go test ./internal/httputil ./setup -count=1
go build ./cmd/dirextalk-message-server
git diff --check
```

No image publication, CloudFormation update, Compose restart, or codex1 change
is part of this work.
