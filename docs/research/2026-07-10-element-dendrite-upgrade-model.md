# Element / Dendrite upgrade model research

Date: 2026-07-10

Current-policy note (2026-07-20): the source research is retained, but the
adopted Dirextalk policy now uses the central server version record as the sole
target authority. The repository no longer publishes predecessor edges,
release metadata assets, attestations, or image-identity gates.

## Scope

Official Element sources were reviewed for server release versioning, database migrations, operator-driven upgrades, rollback constraints, and client involvement.

## Findings

### Dendrite

- Runtime version output is centralized through one version implementation used by command-line and Federation responses. Dirextalk similarly exposes one build-info source and enforces that source version, Git tag, image tag, and image labels agree in CI.
- GitHub Releases are the release boundary. The Docker workflow runs when a release is published and publishes both an exact release tag and `latest`.
- Dendrite documents forward database schema upgrades between releases. Applied migrations are recorded in `db_migrations` together with the Dendrite version.
- The migration type contains a `Down` callback, but the source explicitly says downgrade migrations are not implemented. Dendrite therefore is not a precedent for arbitrary automatic server downgrade.
- Dendrite has useful cross-version CI: releases reuse the same PostgreSQL volume and verify that earlier accounts, rooms, and messages remain usable. Dirextalk keeps retained-data migration coverage without tying publication to a predecessor allowlist.
- Its backup FAQ treats configuration/signing keys, database, JetStream, media, and search state as persistent state. A safe Dirextalk rollback backup must capture the complete required set, not only PostgreSQL.
- Release notifications are delivered through the `#dendrite-alerts:matrix.org` room. There is no official Element client flow that upgrades the homeserver.
- Dendrite is currently in maintenance mode and still describes itself as beta, so its deployment examples should not be copied as a production stability policy without hardening.

### Element Server Suite

- ESS Community uses operator-side Helm installation and upgrade (`helm upgrade --install`).
- ESS Classic requires an operator to review every intervening upgrade note and recommends upgrading to the latest patch of the current LTS before moving to the next LTS.
- ESS troubleshooting allows Helm rollback only after verifying that the target is compatible. It explicitly warns against rollback to an incompatible version.
- Element's server products separate server administration from Matrix clients. No official evidence was found for a normal user/client-triggered homeserver image upgrade.

### Synapse

- Synapse maintains both a database schema version and a schema compatibility version. A backward-incompatible database change raises the compatibility version so a too-old binary cannot start accidentally.
- Destructive schema changes are staged across releases: introduce the new representation, temporarily keep compatibility writes, then remove old data only after the rollback window closes.
- Synapse publishes explicit rollback-compatible version ranges and warns that deploying a new release does not imply automatic rollback is safe.

### Matrix protocol boundary

- `/_matrix/client/versions` negotiates Matrix Client-Server API features, not compatibility between a particular app build and a homeserver software release.
- `/_matrix/federation/v1/version` exposes homeserver implementation information but is not an upgrade-control protocol. Dirextalk therefore needs its own stable release-control contract.

## Dirextalk recommendations

1. Use formal GitHub Releases (`v1.0.0`, `v1.0.1`, ...) and matching canonical image tags; publish `latest` only after the version image and Release succeed.
2. Use the fixed central `appId=1`, `channelId=server` record as the only target authorization. Any older canonical server may install that target without a predecessor graph.
3. Add `schema_version` and `schema_compat_version` checks before starting a server binary. Treat rollback as a separately approved recovery transition, not as reverse SemVer comparison.
4. Keep server upgrade execution in a privileged host updater. The message-server exposes a stable versioned control API and never receives the Docker socket.
5. The client may display status and let an owner request the centrally approved transition, while the host updater remains responsible for disk space, backup, Compose execution, and health checks.
6. Keep exactly one committed rollback backup. Build a new backup under a temporary name, validate checksum/manifest and database readability, then atomically replace the committed backup. Never delete the old committed backup before the new one is valid.
7. Rollback is only to the version recorded in the committed backup manifest. Do not provide arbitrary version selection. After a successful rollback, retain that same committed backup unless a later successful upgrade atomically replaces it.
8. Use daily release discovery plus a once-per-version client notification, similar in spirit to Dendrite's release-alert room, but do not auto-apply the update.

## Backup state model

Steady-state retained files:

- `current/backup.tar.zst`
- `current/backup.json`

Temporary upgrade files:

- `pending/<job-id>.tar.zst.part`
- `pending/<job-id>.json.part`

The updater creates and validates `pending`, then atomically swaps it into `current`. A failed backup leaves the old `current` untouched. Two copies can temporarily exist during creation; steady state retains one. Avoiding even this temporary overlap would risk destroying the only recovery point before a replacement is proven valid.

## Official sources

- https://github.com/element-hq/dendrite
- https://github.com/element-hq/dendrite/blob/main/.github/workflows/docker.yml
- https://github.com/element-hq/dendrite/blob/main/internal/sqlutil/migrate.go
- https://github.com/element-hq/ess-helm/blob/main/README.md
- https://docs.element.io/latest/element-server-suite-classic/installing-element-server-suite/
- https://docs.element.io/latest/element-server-suite-classic/change-logs-and-upgrade-notes/
- https://docs.element.io/latest/element-server-suite-pro/troubleshooting/
- https://element-hq.github.io/synapse/develop/development/database_schema.html
- https://element-hq.github.io/synapse/latest/upgrade.html
