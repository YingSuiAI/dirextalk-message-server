# Dirextalk Message Server release notes

## Unreleased

## v1.0.4

1. Establish a fresh trusted-release baseline with a verified, exact `v1.0.3` upgrade source.
2. Add metadata-only unread recovery snapshots for new devices.
3. Keep read-marker ordering server-authoritative across retries, restarts, and concurrent updates.

## v1.0.3

1. Make group joins, contact decisions, and channel approvals recoverable after retries and restarts.
2. Add durable operation leases to prevent duplicate concurrent actions.
3. Add optional recovery status fields to ProductCore HTTP and WebSocket responses.

## v1.0.2

This release keeps the exact `v1.0.1` formal image as its stable upgrade
source. Server schema, updater API, Product actions, and client compatibility
remain unchanged. Legacy hosts must first be moved to a formal baseline under
an operator-controlled backup; unindexed legacy images remain fail closed.

## v1.0.1

This security patch updates `golang.org/x/crypto` to `v0.52.0`. It does not
change the server schema, updater API, Product action contract, or supported
client-version range.

The trusted release index permits only the exact tested `v1.0.0` image digest
to upgrade to `v1.0.1`. Other source images continue to fail closed.

## v1.0.0

This is the first formal, immutable server release. The release version is
reported as `v1.0.0`; its source commit and build time remain separate build
metadata.

### Compatibility

- Server schema version: `1`.
- Oldest readable server schema version: `1`.
- Client compatibility is declared by each published release manifest using
  an inclusive minimum and exclusive maximum version.
- `upgrade_from` is an explicit allowlist of SemVer constraints. An absent
  upgrade path must be rejected instead of guessed.

### Backup and rollback

An upgrade requires a backup. Rollback restores the single retained backup
created before the current deployment attempt; it does not reuse an arbitrary
older backup.

### Publishing the manifest

`release-manifest.template.json` contains substitution placeholders and must
not be published as-is. Replace every placeholder, resolve the image to a
lowercase `sha256` digest, and validate the rendered JSON before attaching it
to the matching GitHub Release.

The image tag, manifest version, release-notes tag, and GitHub tag must all be
identical. Production upgrades must pull the immutable digest from the
manifest, never a mutable `latest` tag.

The matching GitHub Release also carries `release-index.json` and its checksum.
The index is the only authority for ordered upgrade paths: each edge names the
source version, exact tested source image digest, and target manifest. For the
initial release, the legacy `v0.15.2` edge is restricted to the recorded
pre-release image digest; other `v0.15.2` builds are not assumed compatible.
The recorded legacy identity is not available from Docker Hub. Its upgrade
edge additionally requires a canonical retained-data attestation produced from
an explicitly imported image whose local image ID matches exactly; missing
evidence disables publication rather than falling back to a tag.

Run the project-local `dirextalk-message-server-release` Skill and
`scripts/release/{prepare,verify,publish}.sh`. The scripts publish the fixed
version image and verified GitHub assets before they move `latest` to the same
digest.
