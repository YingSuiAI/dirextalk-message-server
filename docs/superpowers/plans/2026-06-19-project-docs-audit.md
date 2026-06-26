# Project Documentation And API Audit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Produce project agent guidance, a complete importable Postman collection, and an implementation audit focused on real API behavior and multi-node communication.

**Architecture:** This is a documentation and audit pass over the existing Dendrite-based Matrix/P2P monolith. The source of truth is route registration, `p2p.Service.Handle`, persistence migrations, Matrix transport, projector code, and dual-node smoke coverage.

**Tech Stack:** Go 1.26.4, gorilla/mux, Matrix Dendrite components, PostgreSQL 18, JetStream/NATS, Docker Compose, cross-platform PowerShell/Bash validation, Postman collection v2.1.

---

### Task 1: Repository And Route Inventory

**Files:**
- Read: `README.md`
- Read: `docs/p2p-integrated-as-implementation.md`
- Read: `p2p/service.go`
- Read: `p2p/routing.go`
- Read: `clientapi/routing/routing.go`
- Read: `federationapi/routing/routing.go`
- Read: `mediaapi/routing/routing.go`
- Read: `syncapi/routing/routing.go`
- Read: `relayapi/routing/routing.go`

- [x] **Step 1: Confirm project shape**

Run: `git status --short`
Expected: no output or only unrelated user changes.

- [x] **Step 2: Extract P2P actions**

Read `p2p.Service.Handle` and list every action string from its switch statement.

- [x] **Step 3: Extract HTTP routes**

Scan route registration files for `Handle`/`HandleFunc` plus `Methods`, preserving source file and method.

### Task 2: Agent Guidance

**Files:**
- Create: `AGENTS.md`

- [x] **Step 1: Document local development workflow**

Include build, test, lint, Docker, dual-node smoke, and Postman usage.

- [x] **Step 2: Document architecture boundaries**

Cover Matrix APIs, P2P body-action API, store/transport/projector boundaries, and multi-node assumptions.

- [x] **Step 3: Document engineering rules**

Include Go formatting, scoped changes, persistence-first behavior, multi-node verification requirements, and interface-change recording rules.

### Task 3: Postman Collection

**Files:**
- Create: `docs/postman/direxio-message-server.postman_collection.json`

- [x] **Step 1: Generate P2P action requests**

Create a Postman v2.1 collection request per `Service.Handle` action using `POST /_p2p/query` or `POST /_p2p/command`.

- [x] **Step 2: Generate Matrix/Dendrite route index**

Create a route-index folder for client, federation, media, admin, relay, well-known, and MSC routes extracted from mux registration.

- [x] **Step 3: Add collection variables**

Add `baseUrl`, `accessToken`, `agentToken`, `password`, `roomID`, `channelID`, `postID`, `commentID`, `eventID`, and peer/node variables.

### Task 4: Implementation And Multi-Node Audit

**Files:**
- Create: `docs/api-audit-and-optimization.md`

- [x] **Step 1: Summarize completed features**

Group features by portal/profile/sync, contacts, groups, channels, posts/comments/reactions, messages, calls, reports, favorites, follows, agent/API permissions, Matrix APIs, and federation.

- [x] **Step 2: Audit real implementation status**

Record handler, store, transport, projector, and smoke evidence for each major area.

- [x] **Step 3: Audit multi-node communication**

Cover remote public channel discovery/join requests, Matrix transport, roomserver output projection, contact invite projection, redaction projection, and dual-node smoke coverage.

- [x] **Step 4: Record optimization opportunities**

Prioritize concrete risks and improvements without changing runtime behavior in this pass.

### Task 5: Interface Change Record

**Files:**
- Create: `docs/api-interface-change-record.md`

- [x] **Step 1: Record current change status**

State that this pass does not modify API input or output contracts.

- [x] **Step 2: Record future process**

Define how future interface changes must document affected actions, parameters, response fields, compatibility, migration, and tests.

### Task 6: Verification

**Files:**
- Verify: all created Markdown and JSON files

- [x] **Step 1: Validate JSON**

Run: `python -m json.tool docs/postman/direxio-message-server.postman_collection.json`
Expected: valid JSON output with no parse error.

- [x] **Step 2: Run targeted tests**

Run: `go test ./p2p ./internal/httputil ./setup -count=1`
Expected: all packages pass.

- [x] **Step 3: Review git diff**

Run: `git diff --stat`
Expected: only planned documentation/Postman files changed.
