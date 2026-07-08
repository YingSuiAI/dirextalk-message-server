# P2P Deep Refactor Phase A Audit

Scope: Phase A audit only. This document inventories the current `p2p` facade, adapter boundaries, duplicate behavior, compatibility code, ownership moves, and risks before any implementation phase. It intentionally does not prescribe production code edits as already completed.

Assumptions:

- Current public behavior is the body-action product surface documented in `AGENTS.md`, `docs/current-project-documentation.md`, and `docs/native-agent-requirements.md`.
- Compatibility can be removed in later phases only when the matching public contract and tests are updated at the same time.
- Matrix room/member/state/message/redaction facts remain Matrix-native. Product tables are projections unless a specific business rule makes a table source of truth.

Symbol verification note: this audit was revised after `rg` verification. Any remaining symbol marked as symbol drift is an audit lead, not an implementation target.

## Blocking Product Decision: reports.submit Auth Classification

Phase B must not silently classify `reports.submit`. If `reports.submit` remains inconsistent when action metadata is implemented, Codex must stop and report the inconsistency instead of choosing an auth/transport contract.

The product owner must decide whether `reports.submit` is:

- public unauthenticated ProductCore callback;
- owner-protected action;
- internal node/ProductCore-only callback with explicit auth;
- removed/deprecated.

After that decision, `AGENTS.md`, `docs/current-project-documentation.md`, `p2p/serviceapi/actions.go` action metadata, action registry tests, Postman examples, and stale test wording must be updated together. Until then, keep `reports.submit` listed as a public contract risk.

## MCP Product Decision: Unified Capability Service

Long-term MCP capability behavior must be implemented once in a neutral `internal/dirextalkmcp` package and reused by both first-class Native Agent Dirextalk tools and any external standard MCP HTTP transport. Do not keep separate `mcp.*` product action business logic and separate Native Agent tool business logic.

Resolved first-version auth decision:

- The first external MCP HTTP endpoint version reuses the existing `agent_token` as its bearer token.
- The endpoint must require `Authorization: Bearer <agent_token>` on protected JSON-RPC requests.
- It must not accept access tokens or agent tokens in query strings.
- It must not pass inbound MCP bearer tokens through to downstream services.

Resolved endpoint decision:

- The standard MCP Streamable HTTP endpoint path is `POST /mcp`.
- It is mounted at root `/mcp`, not under `/_p2p`, and does not use the Dirextalk body-action envelope.

Resolved compatibility timing for this pass:

- Do not delete old fixed `mcp.*` body actions in the current pass.
- Treat them as temporary compatibility wrappers around `internal/dirextalkmcp`.
- The next MCP-D task is deletion: remove `mcp.*` from the product action registry, `serviceapi.AgentAction`, Postman, docs, and compatibility tests unless product explicitly reopens short-term compatibility.

Before exposing the endpoint, update `AGENTS.md`, `docs/current-project-documentation.md`, `docs/native-agent-requirements.md`, `docs/api-interface-change-record.md`, Postman collections, project-local `.codex/skills`, and focused tests together. This is an intentional contract change from the previous "no URL-shaped product endpoints" rule.

## 1. P2P Wrapper/Adapter Inventory

| Area | Files and symbols | Current role | Refactor note |
| --- | --- | --- | --- |
| Body-action registry facade | `p2p/action_registry.go`: `actionHandler`, `Service.actionHandlers`; `p2p/action_registry_*.go`: `registerPortalActions`, `registerContactActions`, `registerGroupActions`, `registerChannelActions`, `registerSocialActions`, `registerAgentActions`, `registerMCPActions`, `registerPluginActions`; `p2p/service.go`: `Service.Handle` | Maps stable action strings to product service methods. | Keep a facade here, but replace scattered metadata with a single action spec in Phase B. |
| HTTP route adapter | `p2p/routing.go`: `Register`, `handle`, `httpProductActionAllowed`, `responseForRequest`, `requestBaseURL`, `setCORSHeaders`, `writeJSON`, `writeError` | Converts `/_p2p/query` and `/_p2p/command` JSON envelopes into `Service.Handle` calls. | Keep transport envelope handling in the facade; move action allow/auth metadata out of route-local helpers. |
| Realtime WS adapter | `p2p/realtime_ws.go`: `realtimeWSHandler`, `handleRealtimeWSRequest`, `createRealtimeWSTicketForToken`, `realtimeWSHTTPOnlyAction`, `updateRealtimeWSSessionFlags`, `upsertRealtimeWSSession`, `shouldSuppressPushForRoom` | Authenticates owner WS tickets, accepts `client.request`, Native Agent stream frames, and lifecycle/focus updates. | Keep frame routing in `p2p`; session-store push decisions already belong to `internal/realtime` and `userapi`. |
| Auth classification adapter | `p2p/serviceapi/actions.go`: `PublicAction`, `AgentAction`; `p2p/service.go`: `Service.Authorize`, `publicAction`; `p2p/realtime_ws.go`: `realtimeWSHTTPOnlyAction`; `scripts/p2p-three-node-regression.py`: `action_requires_http` | Duplicates public, agent-token, HTTP-only, and WS-only action classification. | Prime target for Phase B action metadata consolidation. |
| Product service facade | `p2p/service.go`: `Service`, `Store`, `NewService`, `NewServiceWithTransport`, `NewServiceWithStore`, `NewServiceWithStoreAndTransport`, `newService`, `ensureAgentRoom`, `ensureSystemRoom`, `Handle`; `p2p/service_*.go`: product action handlers | Central business orchestration across portal, contacts, groups, channels, posts, calls, reports, MCP, plugins, and Native Agent. | Keep user-visible action orchestration here, but move Matrix/session/storage mechanics into owning packages. |
| Storage adapter aliases | `p2p/storage_adapter.go`: `DatabaseStore`, `NewDatabaseStore`, `NewUnmigratedDatabaseStore`; `p2p/storage/storage.go`: `DatabaseStore`; `p2p/storage/storage_migrations.go`: `DatabaseStore.migrate`, `execMigrationStatements` | Root package alias over the real storage package plus a very broad `Store` interface in `p2p/service.go`. | Split interfaces by domain in Phase E; do not move storage tables blindly because some are product source of truth and others are projections. |
| Matrix product transport API | `internal/dirextalktransport/transport.go`: `Transport`, `CreateRoomRequest`, `RoomStateEvent`, `SendMessageRequest`, `SendStateEventRequest`, `InviteUserRequest`, `JoinRoomRequest`, `LeaveRoomRequest`, `KickUserRequest`, `RoomChannel`, `RoomMember`, `UpdateMemberProfileRequest`, `RedactEventRequest`; `p2p/transportapi/transport.go`: compatibility aliases; `internal/dirextalkdomain/types.go`: `Channel`, `MemberRecord`; `p2p/domain/types.go`: compatibility aliases | Neutral interface for Matrix room/member/state/message/redaction writes and reads. | Phase D moved the contract to `internal/dirextalktransport`; `p2p/transportapi` is now a compatibility alias package. |
| Dendrite transport wrapper | `p2p/dendrite_transport.go`: `DendriteTransport`, `NewDendriteTransport` facade aliases; `internal/dirextalktransport/dendrite/dendrite_transport.go`: `DendriteTransport`, `NewDendriteTransport`, `CreateRoom`; `internal/dirextalktransport/dendrite/dendrite_transport_membership.go`: `InviteUser`, `JoinRoom`, `LeaveRoom`, `KickUser`; `internal/dirextalktransport/dendrite/dendrite_transport_send.go`: `SendMessage`, `SendStateEvent`; `internal/dirextalktransport/dendrite/dendrite_transport_queries.go`: `GetRoomChannel`, `ListRoomMembers`, `UpdateMemberProfile`, `RedactEvent`; `internal/dirextalktransport/dendrite/dendrite_transport_build.go`: `queryAndBuildEvent` | Adapts product-originated writes into roomserver APIs and calls `internal/productpolicy` before writes. | Phase D moved implementation below the p2p facade; p2p now only constructs/adapts the transport for service wiring. |
| Matrix session/profile adapter | `p2p/matrix_session.go`: `MatrixSessionIssuer`, `MatrixProfileUpdater`, `DendriteMatrixSessionIssuer` alias, `NewDendriteMatrixSessionIssuer`, `NewDendriteAccountDeactivator`; `userapi/dirextalksession/session.go`: `Issuer`, `NewIssuer`, `EnsureMatrixSession`, `UpdateMatrixProfile`, `DeactivateAccount`; `p2p/service_auth_api.go`: `refreshMatrixSession`, `createAgentMatrixSession`; `p2p/service_profile_sync.go`: `updateMatrixProfile`, `updateOwnerMemberProfiles` | Creates Matrix accounts/devices/access tokens and updates Matrix profiles from product actions. | Phase D moved userapi/clientapi mechanics into `userapi/dirextalksession`; p2p keeps product decisions such as default portal device ID and revoke-vs-keep behavior. |
| Matrix profile resolver adapter | `internal/dirextalkmatrix/profile.go`: `Profile`, `HTTPProfileResolver`, `NewHTTPProfileResolver`, `ResolveMatrixProfile`; `p2p/matrix_profile_resolver.go`: `matrixUserProfile`, `matrixProfileResolver`, `HTTPMatrixProfileResolver`, `NewHTTPMatrixProfileResolver` compatibility aliases/wrapper; `p2p/mcp_api.go`: `currentMatrixProfileResolver`, `enrichMCPMemberSummariesWithProfiles`, `mcpResolveMatrixProfile` | Reads Matrix profile information for MCP member summaries. | Phase D moved HTTP profile reads to `internal/dirextalkmatrix`; p2p decides when enrichment is needed. |
| Matrix history adapter | `internal/dirextalkmcp/types.go`: `MessageSummary`, `Page`, `MessagePageResult`, `SortMessageSummaries`, `Event`, `InTimeRange`, `InPage`, `FormatTime`; `internal/dirextalkmatrix/message_reader.go`: `HTTPMessageReader`, `NewHTTPMessageReader`, `ListOrdinaryMessages`, `ListChannelContent`; `internal/dirextalkmatrix/types.go`: neutral aliases/helpers; `p2p/matrixhistory/types.go` and `p2p/matrixhistory/reader.go`: compatibility aliases/wrapper; `p2p/matrix_history_reader.go`: `HTTPMatrixHistoryReader`, `NewHTTPMatrixHistoryReader`; `p2p/mcp/types.go`: compatibility aliases; `syncapi/agenthistory/reader.go`: `Reader`, `ListOrdinaryMessages` | Provides HTTP and sync-backed history readers for MCP and channel backfill. | Phase D moved HTTP Matrix history reads to `internal/dirextalkmatrix`; p2p remains the service adapter. |
| Projection adapter | `p2p/projector.go`: `ProjectOutputEvent`, `ProjectRoomEvent`; `p2p/projector_messages.go`: `projectMessage`; `p2p/projector_members_contacts.go`: `projectReaction`, `projectMember`; `p2p/projector_state.go`: `projectRoomProfileState`, `projectMemberPolicyState`, `projectJoinRequestState`; `p2p/consumer.go`: roomserver consumer wiring | Projects roomserver output into product read models and product events. | Keep projection dispatch close to p2p domain unless event parsing moves into a lower-level projection package with neutral DTOs. |
| Remote public ProductCore adapter | `p2p/remote_public.go`: `newRemotePublicHTTPClient`, `matrixRoomIDQuery`, `remotePublicChannelGet`, `remoteChannelJoinRequest`, `remotePublicAction`, `remoteNodeBaseURL`, `normalizeRemoteNodeBaseURL`, `remoteNodeBaseURLParam`, `remotePublicForwardParams`, `remoteNodeBaseURLUsesPrivateHost`, `roomServerFromMatrixRoomID`, `remotePublicError` | Performs remote public channel lookup and join request callbacks through explicit `remote_node_base_url`. | Keep product workflow in `p2p`; URL/room-id validation can become a small reusable helper. |
| Plugin runtime adapter | `p2p/plugin_runner.go`: `PluginRunner`, `PluginRunnerOperation`, `PluginInvokeRequest`, `PluginStreamEvent`, `dockerPluginRunner`, `validateOfficialPluginOperation`, `validateOfficialPluginVolume`, `officialPluginImage`, `pluginImageReference`, `pluginContainerName`, `writePluginEnvFile`; `p2p/service_plugins.go`: `officialPluginCatalog`, `applyPluginAction`, `pluginInvokeRequest` | Manages non-Agent official Docker plugins. | Keep non-Agent plugin facade separate from Native Agent; remove hidden Agent plugin branches after migration isolation. |
| Native Agent adapter | `p2p/native_agent_runner.go`: `agentPluginID`, `NativeAgentRunner`, `nativeAgentInvokeAction`, `nativeAgentInvokeStreamAction`, `nativeAgentTools`, `nativeAgentSummarize`; `p2p/nativeagent/*`: `Runtime`, tool dispatch, config sanitization; `p2p/service_agent_config_native.go`: `agentConfigToNativeMap`, `agentConfigFromNativeMap`, `migrateLegacyAgentPluginConfig` | Bridges first-class `agent.*` actions and Native Agent runtime/tools. | Keep first-class action facade in `p2p`; delete Agent-as-plugin compatibility only after startup migration is isolated. |

## MCP-A Architecture: Unified Dirextalk MCP Capability Service

Phase MCP-A is documentation/design in this audit and defines future test gates. Do not modify production code, routes, action handlers, Postman, or runtime behavior in this phase.

Target package:

- Create `internal/dirextalkmcp` in Phase MCP-B.
- The package must not import `p2p`.
- It owns the Dirextalk MCP capability registry, tool schemas, tool invocation, pagination, room authorization, shared request/response DTOs, and MCP-facing errors.
- It must keep ordinary messages Matrix Client-Server backed, channel posts/comments separate from ordinary channel chat, stable `from_time`/`to_time`/`cursor` pagination, and rejection of old `from_ts`/`to_ts`/`ts`/`last_ts` fields.

Dependencies must enter through small interfaces, not through `p2p.Service`:

- contacts reader;
- rooms reader;
- message history reader;
- message writer;
- room member reader;
- channel post/comment reader and writer;
- profile resolver;
- room/blocklist authorizer.

`p2p.Service` should become an adapter that supplies `Store`, `Transport`, Matrix history reader, profile resolver, owner context, and `mcp_blocked_room_ids` behavior to `internal/dirextalkmcp`. Existing `p2p/action_registry_mcp.go` and `p2p/mcp_api.go` `mcp.*` handlers are temporary compatibility wrappers around that service and should be removed in the next MCP-D pass unless product explicitly extends compatibility.

Native Agent Dirextalk tools should be generated from the same `internal/dirextalkmcp` registry and schemas. `p2p/native_agent_runner.go:nativeAgentTools` and `p2p/nativeagent/native_agent_tools.go` should not keep duplicated Dirextalk MCP business logic after Phase MCP-B.

External standard MCP clients use `POST /mcp` as the MCP Streamable HTTP transport endpoint, not the Dirextalk `{ "action": "...", "params": ... }` body-action envelope. The endpoint contract is:

- implement JSON-RPC lifecycle sufficient for `initialize`, `tools/list`, and `tools/call`;
- support HTTP POST for client-to-server JSON-RPC messages;
- return 405 for HTTP GET/SSE unless server-to-client streaming is actually needed;
- require `Authorization: Bearer <agent_token>` in the first version;
- reject owner `access_token` on the standard MCP endpoint;
- validate `Origin` for HTTP/SSE connections;
- reject query-string tokens;
- avoid passing inbound bearer tokens to downstream services;
- keep `mcp_blocked_room_ids` hide/reject behavior;
- route message sends through `p2p.Transport` via adapter interfaces;
- preserve the model API key rule: request-scoped keys must not be persisted, logged, returned, or injected into runtime env.

## 2. Redundant Functions and Duplicate Behavior Map

| Duplicate behavior | Files and symbols | Impact | Recommended next step |
| --- | --- | --- | --- |
| Action transport/auth metadata is split across registry, serviceapi, HTTP route, WS route, and tests/scripts. | `p2p/action_registry.go`: `Service.actionHandlers`; `p2p/serviceapi/actions.go`: `PublicAction`, `AgentAction`; `p2p/service.go`: `Service.Authorize`, `publicAction`; `p2p/routing.go`: `httpProductActionAllowed`; `p2p/realtime_ws.go`: `realtimeWSHTTPOnlyAction`; `scripts/p2p-three-node-regression.py`: `action_requires_http`; `p2p/routing_ws_test.go`: `TestRealtimeWSRequestCoverageMatchesActionRegistry`; `p2p/action_registry_test.go`: `TestActionRegistryCoversPublicAndAgentActions` | New actions can be registered without matching public/agent/HTTP/WS metadata, or script behavior can drift from server behavior. | Phase B: introduce one enum-backed action spec containing handler, auth, transport, and generated route tests. |
| Native Agent model profile sanitization and lookup existed in both plugin compatibility code and Native Agent code. | `p2p/service_agent_config_native.go`: `sanitizeNativeAgentConfigMap`; `p2p/nativeagent/native_agent_util.go`: `savedAgentModelProfileByID`, `sanitizeConfig`, `sanitizeModelProfiles` | Increases risk of leaking or persisting request-scoped API keys if one copy changes and the other does not. | Phase C removed plugin-side Agent invoke/model-profile compatibility and keeps Native Agent config sanitization native-owned. |
| MCP capability business logic was duplicated between body-action handlers and Native Agent tools. | `internal/dirextalkmcp/service.go`: `Service`, `Tools`, action constants, schemas; `internal/dirextalkmcp/pagination.go`: pagination/cursor helpers; `internal/dirextalkmcp/types.go`: DTOs and shared errors; `p2p/mcp_service.go`: `p2pDirextalkMCPInvoker`, `p2pDirextalkMCPRoomAuthorizer`; `p2p/action_registry_mcp.go`: `registerMCPActions`; `p2p/native_agent_runner.go`: `nativeAgentTools`; `p2p/mcp_unified_service_test.go`: unified wrapper/tool tests | The same Dirextalk MCP capability can drift across external `mcp.*` body actions and internal Native Agent tools, including schemas, room blocking, pagination, and write-path guarantees. | Phase MCP-B creates `internal/dirextalkmcp`; current `mcp.*` body actions and Native Agent Dirextalk tools are wrappers around one service. |
| Agent summary/truncation logic is duplicated. | `p2p/native_agent_runner.go`: `nativeAgentSummarize`; `p2p/nativeagent/native_agent_tools.go`: `summarize` | Small behavior drift risk for generated summaries and test fixtures. | Keep one nativeagent-owned summary helper and call it from the p2p bridge if still needed. |
| Matrix message history filtering and summary formatting is duplicated between HTTP and sync-backed readers. | `internal/dirextalkmatrix/message_reader.go`: `HTTPMessageReader.ListOrdinaryMessages`, `HTTPMessageReader.ListChannelContent`; `syncapi/agenthistory/reader.go`: `Reader.ListOrdinaryMessages`; `internal/dirextalkmcp/types.go`: `MessageSummary`, `FormatTime`, `InTimeRange`, `InPage`; `p2p/matrixhistory/types.go`: compatibility aliases | Filtering, pagination, timestamp formatting, sender display fields, and channel content backfill can diverge. | Shared DTOs/helpers live in `internal/dirextalkmcp`; keep HTTP and sync readers on neutral packages and add parity fixtures before behavior changes. |
| Channel/group member counts are recomputed and persisted through several service methods. | `p2p/service_channels.go`: `channelWithCurrentCounts`, `refreshStoredChannelCounts`, `refreshStoredGroupCounts`, `refreshChannelCountsLocked`, `refreshGroupCountsLocked`, `memberCounts`; `p2p/service_member_persistence.go`: `saveMember`; `p2p/service_member_invites.go`: `refreshRoomMembers`; `p2p/projector.go`: `ProjectRoomEvent` | Count updates can differ depending on whether the path is command-side, projection-side, or refresh-side. | Create one projection/count updater owned by the member/channel projection layer before changing storage writes. |
| Product room state-event construction is split by product type. | `p2p/service_channels.go`: `channelStateEvent`, `publishChannelState`, `publishChannelHistoryVisibilityState`, `publishMemberPolicyState`, `publishJoinRequestState`; `p2p/service_groups.go`: `groupStateEvent`, `publishGroupState`; `p2p/service_account_delete.go`: `publishAccountDeletedDirectState` | The Matrix state event type/key/content conventions are easy to update in one business path but not the others. | Extract small builders for `io.dirextalk.room.profile`, `io.dirextalk.member.policy`, and `io.dirextalk.join_request` content while keeping the action orchestration in p2p. |
| Plugin env/secret/model-profile helpers included inactive Agent paths alongside current Ops plugin paths. | `p2p/service_plugins.go`: `sanitizePluginConfig`, `pluginRuntimeEnv`, `pluginRuntimeVolumes`; `p2p/service_agent_config_native.go`: `sanitizeNativeAgentConfigMap`; `p2p/plugin_runner.go`: `validateOfficialPluginVolume` | Agent-as-plugin cleanup was hard to reason about because non-Agent plugin runtime helpers shared file scope with hidden Agent branches. | Phase C split Native Agent config sanitization from plugin helpers and removed unused Agent plugin runtime/env code. |
| Matrix account/session refresh concerns are split between product decisions and userapi mechanics. | `p2p/service_auth_api.go`: `bootstrap`, `auth`, `changePortalPassword`, `refreshMatrixSession`, `createAgentMatrixSession`; `p2p/matrix_session.go`: constructor facade and interfaces; `userapi/dirextalksession/session.go`: `EnsureMatrixSession`, `UpdateMatrixProfile`, `DeactivateAccount`; `p2p/service_profile_sync.go`: `updateMatrixProfile` | Device eviction rules for portal sessions vs `agent.matrix_session.create` are business-critical and still depend on p2p choosing the `revokeExistingDevices` flag. | Keep the revoke/not-revoke decision in p2p; lower-level account/device/profile implementation now lives in `userapi/dirextalksession` with focused tests. |

## 3. Historical Compatibility Code That Can Be Deleted

These items should be deleted in a later implementation phase only with the noted preconditions. Some are explicit rejection tests or migration paths and should not be removed blindly.

| Compatibility item | Files and symbols | Delete condition | Public/storage risk |
| --- | --- | --- | --- |
| WS `client.command` alias for `client.request`. | `p2p/realtime_ws.go`: `realtimeWSHandler` frame switch for `"client.command"`; `p2p/routing_ws_test.go`: `TestRealtimeWSClientCommandIsRemoved`; `docs/current-project-documentation.md`: removed-alias note for `client.command` | Phase C removes alias execution and rejects it with `400 unsupported frame type`; clients must use `client.request`. | Removing early breaks old owner clients that still send `client.command`, so contract docs must stay explicit. |
| Hidden Native Agent plugin invoke/config paths. | `p2p/service_plugins.go`: former Agent invoke/env/model-profile helper area; `p2p/service_agent_config_native.go`: `sanitizeNativeAgentConfigMap`; `p2p/plugin_runner.go`: `validateOfficialPluginVolume`; `p2p/plugin_runner_test.go`: Agent plugin volume rejection tests | Phase C isolates legacy Agent config sanitization in the Native Agent config file and deletes plugin-side Agent invoke/env/model-profile helpers plus Agent data-volume allowance. | Must not delete non-Agent plugin storage/runtime support in `p2p/service_plugins.go`, `p2p/plugin_runner.go`, or `p2p/storage/*`. |
| Legacy `io.dirextalk.agent` startup config import. | `p2p/service_agent_config_native.go`: `migrateLegacyAgentPluginConfig`, `mergeLegacyAgentConfig`; `p2p/native_agent_runner.go`: `agentPluginID`, `isNativeAgentPlugin`; `p2p/native_agent_contract_test.go`: legacy config import tests | Delete only after deployments no longer need sanitized import from `p2p_plugins` rows with `ID == "io.dirextalk.agent"`, or after a durable migration marks them consumed. | Removing without a migration window can drop existing Native Agent config during upgrade. |
| Legacy pseudo agents-room id repair. | `p2p/service.go`: `needsAgentRoomCreate`; `p2p/transport_test.go`: `TestEnsureAgentRoomCreatesRealRoomForLegacyID`; `docs/current-project-documentation.md`: "not use legacy `!agent:<domain>`" note | Delete after bootstrap credentials and DB state are guaranteed to contain real private Matrix room IDs for all supported installs. | Removing early can leave upgraded nodes with unusable legacy `agent_room_id`. |
| Removed `agent.status`/`agents.status` action support. | `p2p/routing_test.go`: `TestAgentStatusActionRemoved`; `docs/current-project-documentation.md`: removed action note; `scripts/p2p-dual-smoke.ps1`: stale `agent.status` call removed in Phase C | Server support is already removed; keep old smoke script usage deleted. Keep a rejection test if contract explicitly says the action is removed. | Reintroducing script usage can make manual regression fail for the wrong reason. |
| Legacy MCP timestamp parameter names. | `p2p/mcp_pagination.go`: `mcpPageFromParams`, `rejectLegacyMCPTimeParams` rejection of `from_ts`/`to_ts`; `p2p/mcp_api_test.go`: legacy timestamp rejection cases; `docs/current-project-documentation.md`: rejects old params | Do not delete rejection logic unless the API no longer needs a precise error. Tests can remain as contract rejection tests. | Accidentally accepting old params would reintroduce obsolete MCP contract behavior. |
| Deprecated ordinary P2P room-send storage/action assumptions. | `p2p/storage_test.go`: `TestDatabaseStoreRoomSendActionRemainsDeprecated`; `p2p/action_registry_test.go`: removed action expectations if present | Keep as negative regression coverage unless the obsolete storage artifact is removed by a migration. | Ordinary Matrix messages must not move back into a P2P message store. |

## 4. Logic That Should Move Into Lower-Level Owning Packages

| Logic | Current files and symbols | Owning package direction | Why |
| --- | --- | --- | --- |
| Matrix account/device/session creation and profile writes. | `userapi/dirextalksession/session.go`: `Issuer.EnsureMatrixSession`, `UpdateMatrixProfile`, `DeactivateAccount`; `p2p/matrix_session.go`: `NewDendriteMatrixSessionIssuer`, `NewDendriteAccountDeactivator`; `p2p/service_auth_api.go`: `refreshMatrixSession`, `createAgentMatrixSession`; `p2p/service_profile_sync.go`: `updateMatrixProfile` | Implementation now lives in a userapi-owned helper; p2p passes product decision inputs such as default portal device ID and `revokeExistingDevices`. | Account creation, device deletion, access-token generation, and Matrix profile writes are lower-level Matrix user concerns. |
| Product-originated Matrix event/member/redaction execution. | `internal/dirextalktransport/dendrite/dendrite_transport.go`: `CreateRoom`; `internal/dirextalktransport/dendrite/dendrite_transport_send.go`: `SendMessage`, `SendStateEvent`; `internal/dirextalktransport/dendrite/dendrite_transport_membership.go`: `InviteUser`, `JoinRoom`, `LeaveRoom`, `KickUser`; `internal/dirextalktransport/dendrite/dendrite_transport_queries.go`: `UpdateMemberProfile`, `RedactEvent`; `internal/productpolicy/productpolicy.go`: `ValidateClientEvent`, `ValidateClientMembership`, `ValidateClientRedaction`; `internal/dirextalktransport/transport.go`: transport DTOs backed by neutral `internal/dirextalkdomain` aliases | Implementation now lives in a lower-level product Matrix adapter package. Keep `internal/productpolicy` as write-rule owner. | p2p should orchestrate product workflows, not build and authorize roomserver events by hand. |
| Matrix history DTOs and filtering. | `internal/dirextalkmcp/types.go`: `MessageSummary`, `Page`, `MessagePageResult`, `Event`; `internal/dirextalkmatrix/message_reader.go`: `ListOrdinaryMessages`, `ListChannelContent`; `p2p/matrixhistory/types.go`: compatibility aliases/wrappers; `syncapi/agenthistory/reader.go`: `Reader.ListOrdinaryMessages`; `p2p/mcp_api.go`: `mcpMessagesList` | Shared DTOs/helpers now live in `internal/dirextalkmcp`; HTTP Matrix reads live in `internal/dirextalkmatrix`; `syncapi/agenthistory` imports neutral DTOs directly. | Phase D removed both `syncapi -> p2p/matrixhistory` and HTTP reader implementation edges; keep future reader behavior changes compile-checked and fixture-backed. |
| Dirextalk MCP capability implementation. | `internal/dirextalkmcp/service.go`: registry, schemas, invocation, room authorization; `internal/dirextalkmcp/pagination.go`: cursor helpers; `internal/dirextalkmcp/types.go`: DTOs/errors; `p2p/mcp_service.go`: p2p adapter; `p2p/action_registry_mcp.go`: wrapper registration; `p2p/native_agent_runner.go`: generated Native Agent tools | Keep tool registry, schemas, invocation, pagination, room authorization, shared DTOs, and MCP errors in `internal/dirextalkmcp`. | `internal/dirextalkmcp` does not import `p2p`; `p2p.Service` adapts store/transport/history/profile/owner/blocklist dependencies through small interfaces. |
| Matrix profile reads for member enrichment. | `internal/dirextalkmatrix/profile.go`: `HTTPProfileResolver.ResolveMatrixProfile`; `p2p/matrix_profile_resolver.go`: compatibility aliases; `p2p/mcp_api.go`: `enrichMCPMemberSummariesWithProfiles`, `mcpResolveMatrixProfile` | Profile resolver now lives in a Matrix/client-facing helper. | Profile reads are Matrix data access; p2p decides when enrichment is needed. |
| Push suppression evaluation. | `internal/realtime/session_store.go`: `SessionStore.ShouldSuppressPush`, `HasFreshSession`; `userapi/consumers/roomserver.go`: `suppressPushForForegroundContext`; `p2p/realtime_ws.go`: `shouldSuppressPushForRoom`, `updateRealtimeWSSessionFlags`; `clientapi/routing/account_data.go`: `dirextalkPushContextAccountDataType` | Keep evaluation in `userapi` and `internal/realtime`; leave p2p responsible only for ingesting WS lifecycle/focus frames. | Push delivery is lower-level user notification behavior, not a p2p business action. |
| Plugin runtime execution and validation for official non-Agent plugins. | `p2p/plugin_runner.go`: `dockerPluginRunner`, `validateOfficialPluginOperation`, `validateOfficialPluginVolume`, `officialPluginImage`; `p2p/service_plugins.go`: `applyPluginAction`, `pluginRuntimeEnv`, `pluginRuntimeVolumes` | Consider an internal plugin-runtime package while leaving owner action facade in p2p. | Docker runner mechanics and image/volume validation are runtime concerns, not product action dispatch. |
| Storage implementation interfaces. | `p2p/service.go`: `Store`; `p2p/storage/storage.go`: `DatabaseStore`; `p2p/storage/storage_migrations.go`: `DatabaseStore.migrate`, `execMigrationStatements`; `p2p/service_store_queries.go`: `listContacts`, `listGroups`, `listChannels` | Move narrow interfaces to storage/domain ownership or split them near business areas. | The current single `Store` interface forces unrelated product domains and plugin/report/migration concerns through one p2p dependency. |
| Product state event content builders. | `p2p/service_channels.go`: `channelStateEvent`, `publishMemberPolicyState`, `publishJoinRequestState`; `p2p/service_groups.go`: `groupStateEvent`; `p2p/service_account_delete.go`: `publishAccountDeletedDirectState` | Move content builders into a small product-state package, with p2p still deciding when to publish. | Matrix state event schema should have one implementation source per event type. |

## 5. Logic That Should Remain In The P2P Facade

| Logic | Files and symbols | Reason to keep in p2p |
| --- | --- | --- |
| Stable product action surface and product envelope semantics. | `p2p/routing.go`: `Register`, `handle`; `p2p/action_registry.go`: `Service.actionHandlers`; `p2p/service.go`: `Service.Handle`; `p2p/action_registry_*.go`: all `register*Actions` | `/_p2p/query`, `/_p2p/command`, `/_p2p/ws`, action names, and request/response envelopes are the Dirextalk product facade. |
| Owner/public/agent-token business authorization decisions. | `p2p/service.go`: `Service.Authorize`; `p2p/serviceapi/actions.go`: `PublicAction`, `AgentAction`; future single action spec | The facade owns product auth semantics, even if metadata is centralized. |
| Portal/bootstrap/password/account-delete orchestration. | `p2p/service_auth_api.go`: `bootstrap`, `auth`, `changePortalPassword`; `p2p/service_account_delete.go`: `deleteAccount`, `leaveAccountContacts`, `leaveOrDissolveAccountRooms`, `deactivateAccountUsers`, `writeAccountDeletedCredentialsFile` | These are product lifecycle flows crossing Matrix, storage, credentials, and shutdown behavior. |
| Contact/group/channel workflow orchestration. | `p2p/service_contacts.go`: `contactRequest`, `contactMutation`, `contactReactivate`; `p2p/service_groups.go`: `ensureProductRoom`, `groupResult`, `groupUpdate`, `dissolveGroup`; `p2p/service_channels.go`: `channelResult`, `channelUpdate`, `channelPublicGet`, `channelPublicSearch`, `channelPolicyMutation`, `dissolveChannel`; `p2p/service_member_invites.go`: `inviteMembers`, `joinMember`; `p2p/service_member_mutation.go`: `memberMutation` | These methods encode Dirextalk product workflows and should call lower-level adapters rather than disappear. |
| Public channel remote workflow. | `p2p/remote_public.go`: `remotePublicChannelGet`, `remoteChannelJoinRequest`, `remotePublicAction`; `p2p/service_channel_join.go`: `channelJoinRequest`, `channelJoinResult`, `notifyRemoteChannelJoinResult`, `completeApprovedChannelJoin` | Remote public lookup and approval are product flows that combine validation, local projection, Matrix join, and callback behavior. |
| Channel posts/comments/reactions facade. | `p2p/service_channel_content.go`: `channelPost`, `channelPosts`, `channelComment`, `channelComments`, `recallChannelContent`, `channelReaction`, `redactEvent` | Product post/comment/reaction actions stay separate from ordinary Matrix chat even though Matrix events/redactions back them. |
| Reports facade. | `p2p/service_system_report.go`: `ensureSystemRoom`, `reportSubmit`, `reportNotificationContent` | Owner-directed reports are product actions that store `p2p_reports` and publish system-room notices. |
| MCP action facade and MCP room blacklist rules. | `p2p/action_registry_mcp.go`: `registerMCPActions`; `p2p/mcp_api.go`: `mcpMessagesList`, `mcpRoomMembersList`, channel/comment/post MCP actions; `p2p/service_auth_api.go`: `agentMatrixSession` | MCP remains a fixed product capability surface for owner/agent-token callers even when readers move lower. |
| Native Agent product facade. | `p2p/action_registry_agent.go`: `registerAgentActions`; `p2p/native_agent_runner.go`: `nativeAgentInvokeAction`, `nativeAgentInvokeStreamAction`, `nativeAgentTools`; `p2p/realtime_ws.go`: Native stream frame handling | Native Agent is first-class product behavior behind `agent.*` and WS stream frames; it must remain separated from plugin management. |
| Realtime product WS frame routing. | `p2p/realtime_ws.go`: `realtimeWSHandler`, `handleRealtimeWSRequest`, `createRealtimeWSTicketForToken` | The WS endpoint is part of the p2p facade; only push evaluation and lower-level session storage should stay outside. |
| Projection dispatch into product read models. | `p2p/projector.go`: `ProjectOutputEvent`, `ProjectRoomEvent`; `p2p/consumer.go`: consumer wiring | The projection maps Matrix roomserver output into Dirextalk product read models and event notifications. Builders/storage can be narrowed, but the product projection responsibility remains. |

## 6. Import-Cycle Risks

| Risk | Files and symbols | Why it matters | Mitigation |
| --- | --- | --- | --- |
| `syncapi` and Matrix HTTP readers must not import a `p2p` subpackage. | `syncapi/agenthistory/reader.go`: imports `internal/dirextalkmcp` directly for `Page`, `MessageSummary`, `MessagePageResult`, and pagination helpers; `internal/dirextalkmatrix/*`: imports only neutral/internal and stdlib packages; `p2p/matrixhistory/types.go`: compatibility aliases; `p2p/mcp_api.go`: `mcpMessagesList` | Reintroducing a `syncapi -> p2p` or `internal/dirextalkmatrix -> p2p` import would make future adapter moves cycle-prone. | Keep neutral readers on neutral DTOs/helpers only; `rg "github.com/YingSuiAI/dirextalk-message-server/p2p" syncapi internal/dirextalkmatrix` should stay empty. |
| Transport packages must stay neutral after the Phase D split. | `internal/dirextalktransport/transport.go`: imports `internal/dirextalkdomain`; `internal/dirextalktransport/dendrite/*`: imports `internal/dirextalktransport`; `p2p/transportapi/transport.go`: compatibility aliases; `p2p/domain/types.go`: compatibility aliases | Reintroducing `p2p` imports into `internal/dirextalktransport` would recreate lower-level-to-facade dependencies and future cycles. | Keep transport-shared records in `internal/dirextalkdomain`; `rg "github.com/YingSuiAI/dirextalk-message-server/p2p" internal/dirextalktransport` should stay empty. |
| Service store interface couples all domains to the root p2p package. | `p2p/service.go`: `Store`; `p2p/storage/storage.go`: `DatabaseStore`; `p2p/service_store_queries.go`: `listContacts`, `listGroups`, `listChannels` | Splitting storage by importing service helpers from storage would create cycles because `p2p` owns both interface and domain helpers. | Define narrow interfaces in consumer packages or storage-owned interfaces consumed by p2p, not reverse imports from storage into p2p service code. |
| Native Agent package should remain below the p2p facade, not import it. | `p2p/native_agent_runner.go`: imports `p2p/nativeagent`; `p2p/nativeagent/native_agent_tools.go`: `Tool`, `Runtime.enabledTools`, `nativeToolAlias`; `p2p/service_plugins.go`: `agentPluginID` compatibility branches | Pulling p2p service/plugin symbols into `p2p/nativeagent` would create `p2p -> nativeagent -> p2p`. | Keep nativeagent runtime/tool code dependency-light; pass callbacks/interfaces from p2p into nativeagent. |
| Product policy must stay below p2p and roomserver write adapters. | `internal/productpolicy/productpolicy.go`: `ValidateClientEvent`, `ValidateClientMembership`, `ValidateClientRedaction`; `internal/dirextalktransport/dendrite/dendrite_transport_send.go`: `SendMessage`, `SendStateEvent`; `internal/dirextalktransport/dendrite/dendrite_transport_membership.go`: `InviteUser`, `JoinRoom`, `LeaveRoom`; `internal/dirextalktransport/dendrite/dendrite_transport_queries.go`: `RedactEvent` | If productpolicy imports p2p domain helpers during cleanup, write validation can cycle with the transport adapter. | Keep productpolicy on neutral request structs and roomserver query interfaces. |

## 7. Public Contract Risks

| Risk | Files and symbols | Why it is risky | Required guard |
| --- | --- | --- | --- |
| `reports.submit` public classification is inconsistent between documented action lists and test wording. | `p2p/serviceapi/actions.go`: `publicActions` includes `"reports.submit"`; `p2p/action_registry_social.go`: `registerSocialActions` maps `"reports.submit"` to `Service.reportSubmit`; `p2p/service_system_report.go`: `reportSubmit`; `p2p/business_state_test.go`: stale failure text says `"expected removed reports.submit"`; `AGENTS.md`: public action list omits `reports.submit`; `docs/current-project-documentation.md`: documents owner-directed reports through public ProductCore `reports.submit` | Refactoring auth metadata can accidentally require owner auth or leave unauthenticated behavior without an explicit decision, and stale test wording makes intent harder to read. | Decide and document whether `reports.submit` is public unauthenticated ProductCore or owner-protected, then encode it in the single action spec and tests. |
| HTTP/WS availability differs by hand-maintained functions. | `p2p/routing.go`: `httpProductActionAllowed`; `p2p/realtime_ws.go`: `realtimeWSHTTPOnlyAction`, `handleRealtimeWSRequest`; `p2p/native_agent_runner.go`: `nativeAgentInvokeStreamAction`; `p2p/service_plugins.go`: `pluginInvokeStreamAction` | Stream-only and HTTP-only behavior can drift. `agent.chat.stream` and `plugins.invoke.stream` currently surface "requires websocket" through handlers instead of route metadata. | Use an enum-backed `ActionSpec` with one `ActionTransport` value; do not add separate transport booleans. |
| Agent-token scope must remain narrow. | `p2p/serviceapi/actions.go`: `AgentAction`; `p2p/service.go`: `Service.Authorize`; `p2p/routing_test.go`: `TestAgentTokenCanOnlyCallAgentBootstrapAndMCPActions`; `p2p/realtime_ws.go`: `createRealtimeWSTicketForToken` | Agent token must only call `agent.matrix_session.create` and fixed `mcp.*` HTTP actions; it must not create WS tickets or call owner product actions. | Generate tests over all registered actions from the action spec, not only selected examples. |
| Removed Native Agent/plugin names can be reintroduced during cleanup. | `p2p/routing_test.go`: `TestAgentStatusActionRemoved`, `TestSyncBootstrapOmitsDeprecatedAgentOnline`; `p2p/service_plugins.go`: `requirePlugin`, `listPluginInstances`; `p2p/native_agent_contract_test.go`: `TestNativeAgentIsNotManagedAsPlugin`; `docs/current-project-documentation.md`: `agent.status`/`agent_online` removal | Moving action/plugin registration may accidentally expose removed `agent.status`, `agents.status`, `agent_online`, or `io.dirextalk.agent` plugin surfaces. | Keep negative contract tests while removing compatibility code. |
| `client.command` removal is a WS client contract change. | `p2p/realtime_ws.go`: rejection branch for `"client.command"`; `p2p/routing_ws_test.go`: `TestRealtimeWSClientCommandIsRemoved`; `docs/current-project-documentation.md`: removed-alias note | Deleting the alias without docs/client coordination breaks older owner clients. | Phase C treats this as a contract change and documents that clients must use `client.request`. |
| MCP pagination and response field names must not regress. | `p2p/mcp_pagination.go`: `mcpPageFromParams`, `rejectLegacyMCPTimeParams`; `p2p/mcp_api.go`: `mcpMessagesList`, channel posts/comments list actions; `p2p/mcp_api_test.go`: legacy timestamp rejection cases; `docs/current-project-documentation.md`: `from_time`/`to_time`, `cursor`, no old `ts`/`last_ts` fields | Moving history readers can accidentally reintroduce `from_ts`, `to_ts`, `ts`, or `last_ts`. | Keep explicit schema tests around request rejection and response field absence. |
| Standard MCP HTTP endpoint is a deliberate product route exception. | `AGENTS.md`: no-URL-shaped-product-endpoint rule with explicit MCP exception; `docs/current-project-documentation.md`: `POST /mcp` endpoint contract; `p2p/routing_mcp.go`: standard MCP JSON-RPC transport; `p2p/action_registry_mcp.go`: current body-action `mcp.*` wrapper surface; `p2p/nativeagent/native_agent_eino_mcp.go`: existing MCP client transport use | `POST /mcp` changes the contract from body-action-only product capability access for external MCP clients. | Phase MCP-C pins endpoint path, first-version `agent_token` auth, Origin/token handling, GET 405, and no bearer forwarding. Current `mcp.*` body actions are temporary wrappers; Phase MCP-D should remove them unless product explicitly extends compatibility. |
| Remote public lookup security must survive adapter moves. | `p2p/remote_public.go`: `remoteNodeBaseURL`, `normalizeRemoteNodeBaseURL`, `remoteNodeBaseURLUsesPrivateHost`, `roomServerFromMatrixRoomID`; `p2p/service_channels.go`: `channelPublicGet`, `channelPublicSearch`; `p2p/service_channel_join.go`: `channelJoinRequest`, `notifyRemoteChannelJoinResult` | Public lookup must reject malformed Matrix IDs, URL-shaped server names, and private/internal hosts, while requiring request-provided `remote_node_base_url`. | Keep multi-node and validation tests before moving this code. |
| Matrix-native product state must remain authoritative. | `p2p/service_channels.go`: `publishChannelState`, `publishMemberPolicyState`, `publishJoinRequestState`; `p2p/service_groups.go`: `publishGroupState`; `p2p/projector.go`: `ProjectRoomEvent`; `internal/productpolicy/productpolicy.go`: validation functions | Refactoring can accidentally treat projections as source-of-truth for membership or ordinary messages. | Tests must assert Matrix membership/state events remain the final joined/dissolved/policy facts. |

## 8. Storage/Migration Risks

| Risk | Files and symbols | Why it matters | Guardrail |
| --- | --- | --- | --- |
| The root `Store` interface is too broad to move safely in one pass. | `p2p/service.go`: `Store`; `p2p/storage/storage.go`: `DatabaseStore`; `p2p/storage/storage_migrations.go`: `DatabaseStore.migrate`, `execMigrationStatements` | It mixes portal state, projections, social state, calls, plugin state, reports, invite grants, secrets, and events. | Split by product area only after action/adapter cleanup; keep migrations stable during the split. |
| In-memory fallback maps can mask missing durable storage behavior. | `p2p/service.go`: `Service` fields `contacts`, `blocks`, `groups`, `channels`, `members`, `posts`, `comments`, `reactions`, `favorites`, `follows`, `calls`, `events`, `plugins`, `pluginJobs`, `pluginSecrets`; save/list helpers in `p2p/service_*.go` | Tests may pass through memory state while restart/PostgreSQL behavior is broken. | For each moved domain, add PostgreSQL-backed restart tests or existing `DatabaseStore` tests before deleting memory paths. |
| Projection tables mix Matrix facts with product-only source-of-truth state. | `p2p/service_member_persistence.go`: `saveMember`, `mergeMemberPersistence`, `setProductMemberMute`; `p2p/service_contacts.go`: `saveContact`; `p2p/service_channels.go`: `saveChannel`, `refreshStoredChannelCounts`; `p2p/projector_members_contacts.go`: `projectMember`; `p2p/projector_state.go`: `projectRoomProfileState`, `projectMemberPolicyState` | Moving writes may corrupt the boundary between Matrix membership/state and product-local fields such as remarks, mute flags, invite metadata, favorites, follows, and calls. | Classify each table/field as projection or source-of-truth before storage moves. |
| Legacy Native Agent plugin config import depends on plugin storage. | `p2p/service_agent_config_native.go`: `migrateLegacyAgentPluginConfig`, `mergeLegacyAgentConfig`; `p2p/service_plugins.go`: `sanitizePluginConfig`; `p2p/storage/*`: plugin tables and methods `GetPlugin`, `SavePlugin` | Deleting Agent plugin compatibility without a migration plan can drop old `io.dirextalk.agent` settings. | Extract a small one-way migration sanitizer, then decide whether to keep, mark consumed, or delete legacy plugin rows. |
| Non-Agent plugin storage must remain intact. | `p2p/service_plugins.go`: `savePlugin`, `savePluginJob`, `savePluginSecrets`, `getPluginSecret`; `p2p/plugin_runner.go`: `PluginRunner`; `p2p/storage/*`: `p2p_plugins`, `p2p_plugin_jobs`, `p2p_plugin_secrets` storage methods | Native Agent cleanup must not remove current owner-only plugin manager behavior for Ops and future non-Agent plugins. | Scope deletion to `agentPluginID` branches, not plugin tables or non-Agent runner code. |
| Agents room and system room ids are durable bootstrap/runtime state. | `p2p/service.go`: `ensureAgentRoom`, `needsAgentRoomCreate`; `p2p/service_system_report.go`: `ensureSystemRoom`; `p2p/service_helpers.go`: `writePortalCredentialsFile`, `writeAccountDeletedCredentialsFile` | Refactoring startup repair can orphan real Matrix rooms or leave stale credentials. | Keep startup repair and credentials-file tests when moving setup/runtime code. |
| Channel content backfill depends on Matrix event id persistence. | `p2p/service_channel_backfill.go`: `backfillJoinedPostChannelContent`, `backfillJoinedChannelContent`, `channelContentBackfillWeight`; `p2p/service_channel_content.go`: `attachChannelPostOperation`, `attachChannelCommentOperation`, `channelPostByEventID`, `channelCommentByEventID`, `channelReactionTargetByEventID`; `internal/dirextalkmatrix/message_reader.go`: `ListChannelContent` | Moving or changing history readers can break post/comment/reaction projection recovery after join/restart. | Keep channel backfill tests and low-level `ListChannelContent` fixtures before changing reader behavior. |
| Reports storage and system-room notification are coupled. | `p2p/service_system_report.go`: `reportSubmit`, `reportNotificationContent`; `p2p/storage/*`: report store methods; `p2p/service_profile_sync.go`: `syncBootstrap` returns `system_room_id` where applicable | Changing auth or storage can lose durable reports or duplicate system notifications. | Keep a DB-backed report submission test that checks stored row and Matrix notice behavior. |
| Migrations must stay PostgreSQL and SQLite compatible if both paths remain supported. | `p2p/storage/storage_migrations.go`: `DatabaseStore.migrate`, `execMigrationStatements`; `p2p/storage/*_table.go` files; `p2p/storage_test.go`: migration/storage tests | Splitting storage files can reorder or duplicate migrations. | Avoid migration churn in early refactor phases; run storage tests with both default and PostgreSQL-backed paths where available. |

## 9. Test Gaps

| Gap | Existing files and symbols | Missing coverage |
| --- | --- | --- |
| No single generated contract test covers every action's handler/auth/HTTP/WS/stream metadata. | `p2p/action_registry_test.go`: `TestActionRegistryCoversPublicAndAgentActions`; `p2p/routing_ws_test.go`: `TestRealtimeWSRequestCoverageMatchesActionRegistry`; `p2p/routing_test.go`: `TestAgentTokenCanOnlyCallAgentBootstrapAndMCPActions` | Phase B should add table-driven tests from the single action spec for every registered action and ensure scripts/docs cannot drift silently. |
| `reports.submit` auth/public status is not explicitly pinned against the docs conflict. | `p2p/serviceapi/actions.go`: `publicActions`; `p2p/action_registry_social.go`: `registerSocialActions`; `p2p/service_system_report.go`: `reportSubmit`; `p2p/business_state_test.go`: stale `"expected removed reports.submit"` failure text; docs conflict between `AGENTS.md` and `docs/current-project-documentation.md` | Add a contract test once the intended auth is decided, and rename stale test assertions/messages to match the current action. |
| Lower-level Matrix session/device/profile behavior now has focused userapi helper coverage, but profile propagation remains mostly p2p-covered. | `userapi/dirextalksession/session_test.go`: portal single-device eviction, `agent.matrix_session.create` no-eviction behavior, default device ID; `p2p/matrix_session_test.go`: p2p constructor behavior; `p2p/service_profile_sync.go`: `updateOwnerMemberProfiles` | Add or preserve profile propagation tests before moving member-profile sync behavior further. |
| Matrix transport adapter behavior is mostly tested through p2p flows. | `internal/dirextalktransport/dendrite/dendrite_transport_*.go`: `CreateRoom`, `SendMessage`, `SendStateEvent`, `InviteUser`, `JoinRoom`, `LeaveRoom`, `KickUser`, `RedactEvent`; `p2p/dendrite_transport_policy_test.go`: facade coverage; `internal/productpolicy/productpolicy.go`: validation functions | Add direct package-local fixtures around Matrix event/state content before changing adapter behavior. |
| History reader parity is only partially enforced. | `internal/dirextalkmatrix/message_reader_test.go`: HTTP ordinary-message and channel-content fixtures; `p2p/matrixhistory/reader_test.go`: facade compatibility fixture; `syncapi/agenthistory/reader.go`: `Reader.ListOrdinaryMessages`; `p2p/mcp_api.go`: `mcpMessagesList` | Add shared fixtures proving HTTP and sync-backed readers format/filter/paginate the same ordinary messages. |
| Restart and PostgreSQL-backed projection tests are thin for refactor-sensitive paths. | `p2p/service_member_persistence.go`: `saveMember`; `p2p/service_channels.go`: `saveChannel`, `refreshStoredChannelCounts`; `p2p/service_contacts.go`: `saveContact`; `p2p/storage_test.go`: storage tests | Add DB restart tests for member roles/mutes, channel counts, contact deletion/reactivation, report submission, and channel content backfill before moving storage boundaries. |
| Compatibility deletion tests need to be inverted or retired deliberately. | `p2p/routing_ws_test.go`: `TestRealtimeWSClientCommandIsRemoved`; `p2p/plugin_runner_test.go`: Agent plugin volume rejection tests; `p2p/native_agent_contract_test.go`: legacy import and native sanitizer tests; `scripts/p2p-dual-smoke.ps1`: removed `agent.status` smoke calls | Phase C updates tests to assert current-only behavior and removes stale smoke script calls. |
| Native Agent/plugin separation needs stronger negative coverage after cleanup. | `p2p/service_plugins.go`: `requirePlugin`, `listPluginInstances`, `pluginInvokeRequest`; `p2p/native_agent_contract_test.go`: `TestNativeAgentIsNotManagedAsPlugin`; `p2p/plugin_service_test.go`: plugin manager tests | Add tests that `io.dirextalk.agent` cannot be installed, listed, invoked, logged, or configured through `plugins.*`, while Native Agent config migration still works if kept. |
| Unified MCP service parity is covered for the service boundary and still needs broader fixture parity. | `internal/dirextalkmcp/service_test.go`: registry/tool/authorization tests; `p2p/mcp_unified_service_test.go`: body-action and Native Agent tool invocation through the same service; `p2p/mcp_api_test.go`: behavior coverage for contacts, rooms, messages, members, channel posts/comments, blocked rooms, and pagination | Boundary tests prove old `mcp.*` wrappers and Native Agent Dirextalk tools invoke the same `internal/dirextalkmcp` service. | Add future fixture parity across every capability when MCP-C JSON-RPC transport is introduced. |
| Standard MCP HTTP transport contract tests now exist and should be kept focused. | `p2p/routing_mcp.go`: `handleMCP`, JSON-RPC adapters; `p2p/mcp_http_test.go`: `TestMCPHTTPInitializeAndToolsListRequireAgentToken`, `TestMCPHTTPToolsCallInvokesUnifiedService`, `TestMCPHTTPRejectsQueryTokensBadOriginAndGET` | Future changes still need broader fixture parity across every capability, but the route/auth/transport contract is pinned for `initialize`, `tools/list`, `tools/call`, first-version `agent_token`, Origin validation, GET 405, query-string token rejection, and no downstream bearer-token forwarding. |
| Remote public lookup security should be regression-tested around helper moves. | `p2p/remote_public.go`: `remoteNodeBaseURL`, `normalizeRemoteNodeBaseURL`, `remoteNodeBaseURLUsesPrivateHost`, `roomServerFromMatrixRoomID`; multi-node regression `scripts/p2p-three-node-regression.py` | Add focused unit tests for malformed room IDs, URL-shaped server names, private hosts, missing `remote_node_base_url`, and remote approval callback behavior. |
| Import-cycle safety is not represented by a small focused check. | `syncapi/agenthistory/reader.go`: neutral `internal/dirextalkmcp` import; `internal/dirextalktransport/transport.go`: neutral `internal/dirextalkdomain` import; `internal/dirextalktransport/dendrite/*`: no p2p imports | `go test ./...` catches cycles late; add package-boundary review checks or keep Phase D moves small and compile after each package move. |

## Phase Acceptance Tests

Future phases must treat these as hard gates, not optional suggestions.

Phase B must have:

- generated or table-driven tests over every `ActionSpec`;
- no duplicate action names;
- public action list exactly pinned;
- `agent_token` scope pinned;
- HTTP-only and WS `client.request` behavior pinned;
- stream-only behavior pinned;
- `reports.submit` blocked if unresolved.

Phase C must have:

- `client.command` deletion/rejection or explicit deferral covered by tests and docs;
- `agent.status` / `agents.status` negative coverage or script cleanup;
- `io.dirextalk.agent` plugin surfaces rejected;
- legacy MCP timestamp rejection preserved;
- no Native Agent API key persistence/log/response paths.

Phase D must have:

- import-cycle-free package moves;
- history reader parity tests before changing reader behavior or moving reader implementations;
- productpolicy/write-path tests before moving transport code;
- no direct Matrix SQL writes from product code.

Phase E must have:

- `Store` interface split covered by tests;
- restart or DB-backed coverage for moved durable state;
- migration idempotency preserved;
- projection vs source-of-truth field classification documented before moving storage writes.

Phase F must have:

- `AGENTS.md`, current docs, Postman, and `.codex` skills synchronized for any contract change;
- JSON validation for Postman if touched;
- `git diff --check`.

Phase MCP-A must have:

- `docs/p2p-deep-refactor-audit.md` architecture section describing `internal/dirextalkmcp` and the external MCP HTTP contract;
- product decision block for endpoint path, first-version `agent_token` auth, and old `mcp.*` removal timing;
- no production code changes.

Phase MCP-B must have:

- `internal/dirextalkmcp` created without importing `p2p`;
- MCP DTOs, pagination helpers, tool registry, schemas, invocation, room authorization, and shared errors owned by that package;
- `p2p.Service` adapters for contacts, rooms, Matrix history, message writes, room members, channel post/comment read/write, profile resolution, owner context, and `mcp_blocked_room_ids`;
- existing `mcp.*` body-action wrappers and Native Agent Dirextalk tools calling the same service;
- tests proving wrapper/tool parity and unchanged response behavior.

Phase MCP-C must have:

- chosen endpoint path implemented as `POST /mcp`;
- JSON-RPC tests for `initialize`, `tools/list`, and `tools/call`;
- first-version `Authorization: Bearer <agent_token>` tests, including owner-token rejection on the standard MCP endpoint;
- Origin validation tests;
- query-string token rejection tests;
- HTTP GET/SSE behavior pinned, returning 405 when server-to-client streaming is not needed;
- tests proving inbound MCP bearer tokens are not forwarded downstream;
- no old Agent plugin surfaces exposed.

Phase MCP-D must have:

- current deferral decision applied: do not delete old `mcp.*` body actions in this pass;
- next MCP-D deletion removes `mcp.*` from product action registry and `serviceapi.AgentAction` unless product explicitly extends compatibility;
- if compatibility is extended again, wrappers keep a clear deletion marker and tests proving they call `internal/dirextalkmcp`;
- `AGENTS.md`, `docs/current-project-documentation.md`, `docs/native-agent-requirements.md`, `docs/api-interface-change-record.md`, Postman, and `.codex/skills` synchronized.

## 10. Recommended Order For The Next Implementation Phases

Status after Phase MCP-C: Phase B, Phase C, Phase MCP-B, and Phase MCP-C have implementation coverage in this branch. Product decided not to delete old `mcp.*` body actions in this pass; they are temporary wrappers only. The next MCP-specific implementation task is Phase MCP-D deletion unless product explicitly extends compatibility.

1. Phase MCP-A: document the unified MCP capability architecture.
   - Add only design guidance to this audit document.
   - Record that `internal/dirextalkmcp` is the future owner of MCP tool registry, schemas, invocation, pagination, room authorization, and shared DTOs.
   - Record that the first external MCP HTTP endpoint version reuses existing `agent_token` bearer auth.
   - Record the resolved endpoint path `/mcp` and the temporary old `mcp.*` body-action compatibility decision.
   - Do not modify production code, Postman, routes, or behavior; this audit records test gates instead of adding tests.

2. Phase B: centralize action metadata first.
   - Create one enum-backed action spec that replaces duplicated logic in `p2p/action_registry.go`, `p2p/serviceapi/actions.go`, `p2p/routing.go:httpProductActionAllowed`, `p2p/realtime_ws.go:realtimeWSHTTPOnlyAction`, and `scripts/p2p-three-node-regression.py:action_requires_http`.
   - Use this metadata shape:

```go
type ActionAuth string

const (
	ActionAuthPublic ActionAuth = "public"
	ActionAuthOwner  ActionAuth = "owner"
	ActionAuthAgent  ActionAuth = "agent"
)

type ActionTransport string

const (
	ActionTransportHTTPOnly     ActionTransport = "http_only"
	ActionTransportHTTPAndWS    ActionTransport = "http_and_ws_request"
	ActionTransportWSStreamOnly ActionTransport = "ws_stream_only"
	ActionTransportInternalOnly ActionTransport = "internal_only"
)

type ActionSpec struct {
	Name      string
	Auth      ActionAuth
	Transport ActionTransport
	Handler   actionHandler
}
```

   - Avoid combinations of `allow_http`, `allow_ws_request`, and `stream_only` booleans.
   - Derive HTTP allow, WS `client.request` allow, stream-only behavior, `Service.Authorize`, public action lookup, agent-token action lookup, and `realtimeWSHTTPOnlyAction` from the enum-backed `ActionSpec`.
   - If an action does not fit the enum, stop and report rather than adding ad hoc booleans.
   - Add generated or table-driven tests before changing behavior.

3. Phase C: remove non-MCP compatibility code with explicit contract updates.
   - Remove `p2p/realtime_ws.go` support for `client.command` after updating `docs/current-project-documentation.md` and `p2p/routing_ws_test.go`.
   - Remove stale `scripts/p2p-dual-smoke.ps1` `agent.status` calls.
   - Isolate `p2p/service_agent_config_native.go:migrateLegacyAgentPluginConfig` from `p2p/service_plugins.go:sanitizePluginConfig`, then delete hidden Agent plugin invoke/env/model-profile helpers in `p2p/service_plugins.go` and Agent plugin volume allowances in `p2p/plugin_runner.go`.
   - Defer deleting `p2p/service.go:needsAgentRoomCreate` until a durable migration or supported-version cutoff proves no installs can still carry legacy `!agent:*` room IDs.

4. Phase MCP-B: build the unified internal Dirextalk MCP capability service.
   - Create `internal/dirextalkmcp` with small dependency interfaces and no `p2p` imports.
   - Move MCP DTOs, pagination helpers, tool registry, tool schemas, invocation logic, room authorization, and shared response DTOs into it.
   - Make current `p2p` `mcp.*` handlers temporary wrappers around `internal/dirextalkmcp`.
   - Make Native Agent Dirextalk tools generated from and invoked through the same service.
   - Preserve existing `mcp.*` response behavior for current tests.

5. Phase MCP-C: expose the standard MCP Streamable HTTP transport.
   - Add the chosen endpoint path: `POST /mcp`.
   - Implement JSON-RPC `initialize`, `tools/list`, and `tools/call`.
   - Require first-version `Authorization: Bearer <agent_token>` on protected requests.
   - Reject owner `access_token` on the standard MCP endpoint.
   - Validate `Origin`; reject query-string tokens; do not forward inbound bearer tokens downstream.
   - Return 405 for GET unless SSE/server-to-client streaming is actually needed.
   - Add tests that use JSON-RPC requests rather than Dirextalk action envelopes.

6. Phase MCP-D: remove old `mcp.*` body actions.
   - Current decision is temporary compatibility only; do not add new behavior to these wrappers.
   - Remove `mcp.*` from product action registry and `serviceapi.AgentAction` unless product explicitly extends compatibility.
   - If compatibility is extended, keep wrappers only with a renewed deletion marker and tests proving wrappers call `internal/dirextalkmcp`.
   - Update `AGENTS.md`, `docs/current-project-documentation.md`, `docs/native-agent-requirements.md`, `docs/api-interface-change-record.md`, Postman, and `.codex/skills`.

7. Phase D: move lower-level adapters in dependency-safe order.
   - Do not start Phase D until Phase B action metadata/auth/transport consolidation passes tests.
   - Do not start Phase D until Phase C compatibility deletion decisions are completed or explicitly deferred.
   - Do not start Phase D until Native Agent vs plugin separation has negative tests proving `io.dirextalk.agent` is not exposed through `plugins.*`.
   - Do not start Phase D unless MCP timestamp rejection tests still pass.
   - Resolve the `reports.submit` auth classification decision before Phase D, or explicitly scope Phase D away from `reports.submit`.
   - Do not move lower-level owning package logic while current/legacy contract classification is still ambiguous. Otherwise obsolete compatibility can be moved into lower layers and become harder to delete.
   - Completed first Phase D slice: `syncapi/agenthistory` now imports neutral `internal/dirextalkmcp` DTOs/helpers directly, removing the `syncapi -> p2p/matrixhistory` edge.
   - Completed second Phase D slice: Matrix session/profile/account deactivation mechanics now live in `userapi/dirextalksession`; `p2p/matrix_session.go` keeps facade interfaces and constructors.
   - Completed third Phase D slice: shared transport/domain records `Channel` and `MemberRecord` now live in `internal/dirextalkdomain`; `p2p/transportapi` no longer imports `p2p/domain`.
   - Completed fourth Phase D slice: transport contract now lives in `internal/dirextalktransport`, Dendrite roomserver adapter implementation now lives in `internal/dirextalktransport/dendrite`, and p2p keeps compatibility aliases/constructors.
   - Completed fifth Phase D slice: Matrix HTTP profile and history readers now live in `internal/dirextalkmatrix`, with low-level fixtures and p2p compatibility wrappers.
   - Next Phase D slice: add sync-backed history parity fixtures before changing reader behavior, then continue with narrower storage/projection boundaries.
   - Keep `internal/productpolicy` below the transport adapter and do not import p2p from it.

8. Phase E: split storage boundaries after behavior is stable.
   - Split `p2p/service.go:Store` by product area, preserving `p2p/storage.DatabaseStore` migrations first.
   - Classify fields in `p2p_members`, contacts, channels, reports, plugins, calls, favorites, follows, and read markers as projection or source-of-truth before moving writes.
   - Add PostgreSQL-backed restart tests for each moved store boundary.

9. Phase F: update docs, scripts, and broad verification.
   - Update `AGENTS.md`, `docs/current-project-documentation.md`, `docs/api-interface-change-record.md`, and Postman collections when public behavior changes.
   - Keep the multi-node regression focused on public channel lookup/join, remote callbacks, Matrix membership finality, and URL security.
   - Run focused Go tests for touched packages, `go build ./cmd/dirextalk-message-server`, JSON validation for Postman docs if touched, Docker compose config validation if deployment/runtime files changed, and `git diff --check`.
