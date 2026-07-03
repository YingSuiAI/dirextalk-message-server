# Dirextalk P2P Product Context

This context defines product language for Dirextalk's P2P API and agent integration surface.

## Language

**Developer Operator**:
A technical user who manually configures external agent tooling with a Dirextalk node domain and Agent token.
_Avoid_: App end user, managed tenant user

**Agent Token**:
A portal-owned credential that can authorize protected P2P actions when the action is enabled for agent access.
_Avoid_: Access token, Matrix token, admin token

**Agent Tools Package**:
The first-party CLI and MCP tooling shipped from this repository as an independent tool surface for Developer Operators.
_Avoid_: App gateway, embedded MCP service

**Cross-Platform Tool Build**:
A release build of the Agent Tools Package that produces OS and architecture-specific binaries from Go source.
_Avoid_: Runtime script wrapper, platform-specific source fork

**CLI Action Command**:
A fallback CLI interface that invokes a P2P action by name with JSON params.
_Avoid_: Primary user workflow, generated command for every P2P action

**Domain CLI Command**:
A first-class CLI command grouped around a product or Matrix workflow such as contacts, channels, Matrix session initialization, message sending, or message receiving.
_Avoid_: Raw action wrapper

**P2P Action Fallback**:
The `dirextalk p2p action` command used when a P2P operation is not yet covered by a domain CLI command.
_Avoid_: Primary intelligent-agent workflow

**Agent Skill Recipe**:
A tool-specific instruction document that teaches an intelligent agent how to use Dirextalk CLI commands for common workflows.
_Avoid_: CLI replacement, hidden API contract

**Thin MCP Adapter**:
An optional MCP surface that exposes a small curated set of AI-friendly tools backed by the CLI/client contract.
_Avoid_: Full P2P API mirror, embedded MCP subsystem

**Agent Tool Credentials**:
The site domain and Agent token used by the Agent Tools Package to call a Dirextalk node.
_Avoid_: Stored profile, portal password, Matrix login, P2P path base URL

**Dirextalk Domain**:
The `DIREXTALK_DOMAIN` value used by CLI tooling as the node origin, such as `https://example.com`.
_Avoid_: Base URL, homeserver URL with route prefix

**Site Domain**:
The origin for a Dirextalk node, without a Matrix or P2P route prefix.
_Avoid_: `/_p2p` base URL, `/_matrix` base URL

**Agent Matrix Session**:
A Matrix Client-Server session issued to trusted agent tooling through Agent token authorization.
_Avoid_: Password login session, user-supplied Matrix token, printed access token

**Matrix Message Receive**:
The CLI workflow for fetching or streaming Matrix room events through Matrix sync using an internal Agent Matrix Session.
_Avoid_: P2P message action, custom polling endpoint

**Agent Tool JSON Output**:
The CLI result contract that prints successful responses as JSON for humans and automation, while writing failures to stderr with non-zero exit codes.
_Avoid_: Table-first output, mixed stdout/stderr result data
