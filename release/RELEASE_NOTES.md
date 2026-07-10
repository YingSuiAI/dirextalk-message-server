# Dirextalk Message Server release notes

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
