---
name: dirextalk-message-server-release
description: Use when preparing, verifying, publishing, retrying, or auditing a stable Dirextalk message-server Docker image, Git tag, GitHub Release, or latest-tag movement.
---

# Dirextalk Message Server Release

Use `scripts/release/{prepare,verify,publish}.sh`; do not reconstruct release steps by hand.

## Prepare

1. Read `references/release-contract.md` and the target section in `release/RELEASE_NOTES.md`.
2. Confirm the canonical `vX.Y.Z` in `internal/version.go` and its focused test.
3. Keep client compatibility out of repository release metadata. The middle-platform `server` record owns the minimum client version.
4. Commit and push the reviewed release preparation to `main` before running the scripts.

## Verify And Publish

Run:

```text
bash scripts/release/contract_test.sh
bash scripts/release/prepare.sh vX.Y.Z
bash scripts/release/verify.sh vX.Y.Z
bash scripts/release/publish.sh vX.Y.Z
```

`verify.sh` runs focused tests, builds the production binary and Compose contract, builds the fixed Docker image, and probes its embedded version. `publish.sh` pushes the fixed image, creates the annotated Git tag and asset-free GitHub Release, then moves `latest` to the same digest.

Never upload manifest, index, checksum, compatibility, upgrade-edge, or attestation assets. Stop when the tree is dirty, `HEAD` differs from `origin/main`, a fixed tag already belongs to another image or commit, the GitHub Release has assets, or any version/digest check disagrees. Do not move `latest` before the fixed image, tag, and GitHub Release are verified.

After publication, verify the fixed image digest, embedded version, Git tag commit, formal asset-free GitHub Release, and fixed/`latest` digest equality. Give the operator a concise two- or three-sentence release description that summarizes the main changes and their user-visible or operational effect; do not substitute a commit list. Also report whether the code since the previous stable version contains a client-incompatible contract change so the operator can set the middle-platform minimum client version.
