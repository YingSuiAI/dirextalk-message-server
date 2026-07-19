# Stable release contract

A release binds one canonical `vX.Y.Z`, one pushed `main` commit and its RFC3339 commit timestamp, one fixed Docker tag, one immutable registry digest, one annotated Git tag, and one non-draft GitHub Release without assets.

## Repository metadata

- `internal/version.go` contains the target default version.
- `internal/version_test.go` asserts that target.
- `release/RELEASE_NOTES.md` contains the matching release section.
- Dockerfile `ARG VERSION` defaults to `v1.0.0` for ordinary builds; formal release scripts always pass the target version explicitly.
- `go.mod` resolves every dependency from a published module version. Local
  filesystem replacements are forbidden because the production Docker context
  contains only this repository; `scripts/release/contract_test.sh` enforces
  this before release preparation.

Do not create per-release manifest/index/config/checksum/attestation files. The middle-platform `appId=1`, `channelId=server` record selects the published target version and owns the minimum compatible client version.

## Publication order

1. Pass focused Go tests, production build, Compose validation, Docker build, and embedded-version probe.
2. Push `dirextalk/message-server:vX.Y.Z` and resolve its immutable registry digest.
3. Verify the pulled fixed image has the locally verified image ID, version, commit, and build-time labels.
4. Create or verify the annotated Git tag and asset-free formal GitHub Release for the same commit.
5. Move `dirextalk/message-server:latest` to the fixed digest and verify equality.

The fixed image or Git tag may remain after a partial failure and can be retried. Restore the previous `latest` digest if latest movement fails. Never overwrite a fixed version with different content and never attach release assets.
