---
name: dirextalk-message-server-release
description: Use when preparing, verifying, publishing, retrying, or auditing a stable Dirextalk message-server release, Docker image, GitHub Release, manifest/index, compatibility edge, attestation, or latest-tag movement.
---

# Dirextalk Message Server Release

Use the repository-owned release scripts; do not reconstruct Git, Docker, manifest, index, attestation, or GitHub steps by hand.

## Prepare

1. Read `references/release-contract.md`, the target `release/vX.Y.Z.json`, and the current release scripts.
2. Separate inspection/preparation from external publication. Pushing commits/tags/images or creating Releases requires explicit authorization.
3. Update the canonical version, release notes, compatibility/schema metadata, and exact source-image identities as one reviewed change.
4. Declare only upgrade edges proven by the retained-data harness. A stable release must leave a tested path from the previous stable release.

## Verify

Run:

```text
bash scripts/release/contract_test.sh
go test ./internal/releasecontrol ./internal/httputil ./setup ./p2p ./internal/productpolicy -count=1
go test -tags=dendrite_upgrade_tests ./cmd/dendrite-upgrade-tests -count=1
go build ./cmd/dirextalk-message-server
git diff --check
```

Then use the scripts in order:

```text
bash scripts/release/prepare.sh vX.Y.Z
bash scripts/release/verify.sh vX.Y.Z
bash scripts/release/publish.sh vX.Y.Z
```

`publish.sh` is authorized only after the tree is clean, `HEAD` equals the reviewed `origin/main`, and `verify.sh` produced the complete commit-bound evidence. Fix a faulty gate in code; never bypass it.

## Stop And Report

Stop when version/notes/config disagree, an immutable digest is unknown, an edge lacks retained-data evidence, an attestation is incomplete/non-canonical, or the previous stable release has no tested path. Keep `latest` unchanged after any partial failure.

After publication, verify the Git tag, GitHub assets/checksums, embedded version, fixed image digest, trusted index entry, and fixed/`latest` digest equality. Record non-secret evidence and any retryable partial state.
