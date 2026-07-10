# Server Version Detection, Upgrade, Rollback, and Recovery Design

Date: 2026-07-10
Status: Pending written-spec review

## V1 platform baseline

The first production release supports Ubuntu 24.04 LTS on `linux/amd64` only.
The deployer must fail closed before installation on any other operating system,
Ubuntu release, or CPU architecture. V1 publishes one updater binary
(`linux-amd64`) and one amd64 message-server image; multi-architecture release
selection is intentionally out of scope.

The updater is maintained in the independent Go repository
`YingSuiAI/dirextalk-updater`. It has its own semantic version and GitHub
Release lifecycle. Deployer pins an exact updater tag and SHA256, downloads the
single Ubuntu 24.04 amd64 artifact, and installs it as a host systemd service.
The message-server container never owns this binary and never receives the
Docker socket.

## Objective

Give Dirextalk owners a stable client-visible server version, release discovery, compatibility-aware upgrade, progress tracking during message-server downtime, one-step rollback, and automatic recovery of failed Compose services.

This design spans:

- `dirextalk-message-server`: version source, compatibility/status actions, owner authorization, updater job preparation.
- `dirextalk-flutter`: client version reporting, version UI, upgrade progress, rollback, and restart controls.
- `dirextalk-deployer`: host updater, Caddy route, Compose reconciliation, backup, health checks, and default latest-release installation.
- A project-local message-server release skill and deterministic release scripts.

## Core Decisions

1. GitHub Releases named `vX.Y.Z` are the release boundary.
2. Docker `latest` is a discovery/convenience pointer only. Production state records and runs an immutable version tag and image digest.
3. Upgrade permission comes from a machine-readable release manifest and tested upgrade edges, not from SemVer ordering alone.
4. The host updater is a root-managed systemd service independent of message-server, PostgreSQL, and the client WebSocket.
5. Caddy exposes the updater under the same domain through a Unix socket, so progress and recovery remain available while message-server is stopped.
6. Exactly one committed pre-upgrade backup is retained. A temporary replacement may coexist until validation and atomic rotation complete.
7. Rollback targets only the version and digest recorded in that committed backup. Arbitrary historical downgrade is not supported.
8. The updater continuously reconciles the intended Compose state, but never changes versions or pulls `latest` as part of crash recovery.

## Version and Release Manifest

The message-server version starts at `v1.0.0`. One build-info source supplies command output, logs, health/version responses, release validation, and image metadata.

Every GitHub Release includes `release-manifest.json` with at least:

```json
{
  "manifest_version": 1,
  "version": "v1.2.0",
  "image": "dirextalk/message-server:v1.2.0",
  "image_digest": "sha256:...",
  "upgrade_from": [">=v1.0.0 <v1.2.0"],
  "schema_version": 12,
  "schema_compat_version": 10,
  "minimum_client_version": "v1.1.0",
  "maximum_client_version_exclusive": "v2.0.0",
  "backup_required": true,
  "rollback_supported": true,
  "rollback_mode": "restore_backup",
  "release_notes_url": "https://github.com/.../releases/tag/v1.2.0"
}
```

The release index is signed or distributed from a trusted GitHub Release asset with digest verification. An updater accepts a target only when every path edge is declared, the manifest schema is supported, and the image digest matches.

If the current version cannot reach the latest release directly but a complete tested path exists, one client action may execute sequential hops. Each hop completes backup rotation, migration, startup, and health validation before the next starts. With one retained backup, failure recovers to the most recent successful hop, not necessarily the version at the beginning of the multi-hop job.

## Release Skill and Enforcement

Create a project-local model-invoked skill named `dirextalk-message-server-release`. It triggers for message-server version bumps, tags, Docker publication, GitHub Releases, compatibility changes, and release notes.

The skill always invokes deterministic repository scripts. Scripts and CI, rather than prose alone, enforce:

1. Clean and expected branch/commit state.
2. SemVer increment and unified build-info update.
3. Release notes and manifest completion.
4. Source version, Git tag, image tag, embedded version, and digest agreement.
5. Unit, build, schema, and every declared `upgrade_from` cross-version test.
6. Fixed-tag image publication before `latest` is moved to the same digest.
7. GitHub Release creation and manifest/checksum attachment only after all gates pass.

Manual commands that bypass the release workflow must not be the documented path. CI refuses inconsistent tags or manifests.

## Stable Server Contract

Use the existing Product API body-action surface. Freeze these v1 names and make future changes additive:

- `release.v1.status`: owner query over HTTP or owner WS while message-server is available.
- `release.v1.apply`: owner, HTTP-only command that creates a host updater job.
- `client.version.report`: owner command over HTTP or owner WS that records the current client build for compatibility evaluation.

`release.v1.status` returns the running server build, reported client version, latest formal release, compatibility result, validated upgrade path, backup/rollback availability, updater availability, and notification state.

`release.v1.apply` validates owner authorization and the requested server-selected plan. It does not accept arbitrary image names, digests, shell commands, or unvalidated versions. It returns an opaque job id, job-scoped bearer token, and updater status URL.

The client calls `client.version.report` after authenticated cold start/session restore and after WS `server.ready`. Reporting does not rely only on `portal.auth`, because overlay installation preserves login state.
Each report is bound to the portal device/session captured during HTTP authorization or WS ticket creation and uses a narrow device-CAS persistence update. A stale request or already-connected old WS cannot overwrite the new portal device's report. The public status always uses message-server/current-device values for `current_version` and `client_version`; updater echo fields are not authoritative local facts.

## Independent Host Updater

Install `dirextalk-updater` as a root-owned systemd service. It owns:

- Release discovery and manifest validation.
- Immutable image pull and digest verification.
- Full backup and atomic single-backup rotation.
- Targeted Compose service operations.
- Persisted job journal and recovery after updater restart.
- Health validation and automatic rollback.
- Compose desired-state reconciliation and crash recovery.

The message-server container never receives the Docker socket. The updater exposes a narrow HTTP API on `/run/dirextalk-updater/http.sock`; Caddy mounts the socket directory and routes `/_dirextalk/updater/v1/*` to it before message-server routes.

The updater never executes `docker compose down` for an upgrade. It leaves Caddy and PostgreSQL running and recreates only the required message-init/message-server services. It accepts only internally generated plans scoped to the configured Dirextalk Compose project.

## Downtime Authentication and Updater API

Before stopping message-server:

1. The client calls `release.v1.apply` with the owner access token.
2. Message-server verifies owner authorization and sends the validated plan to the updater over the internal control boundary.
3. The updater persists the plan and returns a random job token.
4. Message-server returns `job_id`, `job_token`, and `status_url` to the client.
5. The client polls the updater directly through the same public domain during downtime.

The updater stores only a hash of the job token. The token controls one job, cannot change its target, and expires after the terminal retention window. The Matrix owner access token is never stored by or forwarded to the updater.

Stable updater endpoints:

```text
GET  /_dirextalk/updater/v1/jobs/{job_id}
POST /_dirextalk/updater/v1/jobs/{job_id}/rollback
POST /_dirextalk/updater/v1/jobs/{job_id}/restart
```

Restart means message-server service restart only. Rollback is offered only when the persisted job and backup manifest declare it safe.

## Job State and Progress

Persist and expose these states:

```text
queued
validating
backing_up
pulling
stopping
migrating
starting
health_check
succeeded
rolling_back
rolled_back
failed
```

Responses include current and target versions, current step, completed/total steps, service availability, last safe version, rollback/restart availability, timestamps, and a sanitized error code/message. Progress is step-based; it must not invent precise percentages for operations whose duration is unknown.

## Backup and Rollback

Steady state keeps:

```text
/var/lib/dirextalk-updater/backup/current/backup.tar.zst
/var/lib/dirextalk-updater/backup/current/backup.json
```

An upgrade creates a protected temporary backup containing PostgreSQL plus all required persistent state, configuration/signing keys, media, message state, current version, image digest, schema information, and checksums. The updater validates readability and metadata, fsyncs the files, then atomically replaces `current`. A failed temporary backup leaves the old committed backup unchanged and aborts the upgrade.

Rollback stops message-server, restores the complete backup set, restores the recorded image digest and configuration, starts the old version, and runs the same health gates. The committed backup remains until a later upgrade successfully rotates it.

## Upgrade Success Criteria

An upgrade succeeds only when all checks pass:

1. Expected message-server container is running and Docker-healthy.
2. Internal `/_p2p/health` succeeds repeatedly.
3. Runtime server version equals the target release.
4. Runtime image digest equals the manifest.
5. Database schema version and compatibility version satisfy the manifest.
6. A minimal database read/write probe succeeds.
7. Caddy-to-message-server routing succeeds.
8. The checks remain stable for a configured confirmation window.

Failure after a committed backup triggers automatic rollback. The updater remains reachable throughout rollback and after a message-server failure.

## Compose Watchdog and Desired State

The updater combines Docker events with periodic reconciliation, initially every 30 seconds. It records one desired state:

- `running`: keep critical services healthy.
- `upgrading`: suspend ordinary repair and let the active job control services.
- `maintenance`: do not restart intentionally stopped services.
- `deprovisioned`: never resurrect the node after account deletion or destroy.

For `running`, three consecutive failed health observations trigger repair. The updater inspects Docker and Compose before acting:

1. Ensure Docker is available.
2. Ensure PostgreSQL is running/healthy without deleting or recreating its volume.
3. Start or recreate message-server with the currently pinned image digest.
4. Start Caddy if required so the updater route and public service recover.
5. Re-run the normal success health gates.

Use a restart budget and exponential backoff, initially three repair attempts in ten minutes followed by a fifteen-minute cooldown. Expose a degraded status after budget exhaustion instead of creating an infinite restart loop. Watchdog recovery never pulls a newer image, rotates backup, runs an upgrade migration plan, or changes the configured target version.

Account deletion, explicit maintenance, deployer destroy, and planned upgrade must set the desired state before stopping services. This prevents the watchdog from undoing intentional shutdown.
If account deletion fails after setting `deprovisioned`, the backend best-effort restores `running` with a fresh bounded context. A restoration failure is returned as a stable sanitized structured error so operators can distinguish watchdog recovery failure from the original deletion-stage failure.

## Client Behavior

Replace the hard-coded About page version with runtime client build information and server release state. Show:

- Client version.
- Running server version.
- Latest formal server release.
- Compatibility/upgrade status.
- Upgrade path and release notes.
- The only available rollback target, when present.

The client starts status discovery after authenticated session restore, after WS `server.ready`, and when the page is opened. The host updater performs daily release discovery and exposes the cached result to message-server; notification delivery occurs at most once per release.

After `release.v1.apply`, the client switches from Product API/WS to updater polling. It renders the persisted job stages and offers only server-authorized rollback or restart actions. On success or rollback it reconnects Matrix/WS, reports the client version again, and reloads release status.

## Installation and Legacy Nodes

New deployer installations resolve the latest formal GitHub Release, validate its manifest, and store the exact server image tag and digest. The user-facing default remains “install latest server”; runtime state never remains on a mutable `latest` reference.

Existing nodes need a one-time deployer/SSH bootstrap that installs the updater service, desired-state file, Caddy socket route, and pinned runtime state. Until bootstrap succeeds, `release.v1.status` reports updater unavailable and the client does not offer one-click upgrade.

## Source of Truth

GitHub Release manifests and updater runtime state are authoritative. An admin database table may mirror release information later for reporting, rollout cohorts, or revocation, but it is not required for the first implementation and must not become a second compatibility authority.

## Verification Plan

- Backend contract tests for auth, additive response parsing, plan validation, client version reporting, and updater-unavailable behavior.
- Updater tests with fake Docker/Compose/GitHub endpoints for manifest validation, job-token scope, persisted restart recovery, backup rotation, health success, rollback, watchdog desired states, restart budget, and no-`latest` recovery.
- Real disposable Compose tests for success, migration failure, unhealthy target, process crash, Caddy continuity, rollback, and multi-hop upgrades with retained data.
- Flutter model/client/provider/widget tests for version rendering, compatibility states, updater polling through message-server downtime, rollback/restart actions, and reconnect.
- Release skill baseline and forward tests plus deterministic script/CI tests for omission and mismatch failures.
- Separate focused commits and verification in message-server, Flutter, and deployer repositories.

## Non-Goals

- Arbitrary historical version selection or downgrade.
- Automatic unattended upgrade installation; discovery and reminders are automatic, application requires owner confirmation.
- Giving message-server direct Docker or root access.
- Replacing normal Matrix sync with a release-specific sync channel.
