---
name: dirextalk-message-server-release
description: Use when preparing, verifying, publishing, retrying, or auditing a stable Dirextalk message-server release, Docker tag, GitHub Release, release manifest, trusted release index, compatibility edge, or latest-tag movement.
---

# Dirextalk Message Server Release

Run the repository-owned fail-closed scripts. Do not reproduce their Git,
Docker, manifest, index, or GitHub commands by hand.

## Workflow

1. Read `references/release-contract.md` and the target
   `release/vX.Y.Z.json` before changing release metadata.
2. Confirm publication is explicitly authorized. Inspection, preparation, and
   verification do not imply permission to push tags, images, or Releases.
3. Update the canonical source version, release notes, compatibility config,
   schema versions, and every exact source-image digest as one reviewed change.
   A claimed edge needs a retained-data upgrade test; otherwise omit the edge.
   A new stable release must retain one tested path from the previous stable
   release; do not publish a stable release that strands the prior stable.
   Every exact source identity also declares `registry` or `offline_import` in
   `source_test_modes`. Never relabel an unavailable registry artifact as tested.
4. Run these repository checks:

   ```bash
   bash scripts/release/contract_test.sh
   go test ./internal/releasecontrol ./internal/httputil ./setup ./p2p ./internal/productpolicy -count=1
    go test -tags=dendrite_upgrade_tests ./cmd/dendrite-upgrade-tests -count=1
   go build ./cmd/dirextalk-message-server
   git diff --check
   ```

   Fix the workflow scripts when a gate is wrong; never bypass a failing gate.
5. Merge and push the reviewed commit to `main`. The release scripts require a
   clean tree whose `HEAD` exactly equals `origin/main`.
6. Run, in order:

   ```bash
   bash scripts/release/prepare.sh vX.Y.Z
   bash scripts/release/verify.sh vX.Y.Z
   bash scripts/release/publish.sh vX.Y.Z
   ```

   `verify.sh` owns every exact-identity retained-data cross-version gate,
   production image build, embedded-version probe, and Compose validation. Its
   canonical `verified.json` binds the commit, local image ID, and complete
   attestation set and is required by `publish.sh`.

   Prefer the `Stable server release` GitHub Actions workflow for registry-
   reproducible sources. The bootstrap `v0.15.2` image identity is not present
   in Docker Hub: import the authorized `docker save` artifact on Ubuntu 24.04,
   tag that exact local image as `dirextalk/message-server:v0.15.2`, run
   `retained-upgrade.sh` against the final target commit, and retain the
   generated external `.release-attestations/v1.0.0` input. Hosted CI must fail
   closed unless that exact commit-bound evidence is supplied through the
   `offline_attestations_json` workflow input (identity to base64 canonical JSON
   map); it validates the evidence in both isolated jobs and must not claim to
   have pulled or retested the unavailable source.
7. Verify the Git tag, GitHub assets/checksums, embedded server version, fixed
   image digest, release-index latest entry, and equality of fixed/`latest`
   digests. Record non-secret evidence in the task ledger.

## Stop Conditions

- Stop before `publish.sh` without explicit publication authorization.
- Stop if the target commit is not pushed `main`, the tree is dirty, notes or
  version metadata differ, an exact source digest is unknown, or an upgrade
  edge has not passed retained-data tests.
- Stop if GitHub Release creation or asset verification fails. `latest` must
  remain unchanged; retry the idempotent fixed-tag/Release steps after fixing
  the cause.
- Stop if the previous stable release has no unique tested path to the target.
  Omitting an untested optional legacy edge is safe; omitting the only path from
  the immediately previous stable release is not a releasable stable state.
- Never use Docker `latest` as the compatibility source of truth. The GitHub
  asset checksum, embedded manifest digest, immutable image digest, and
  explicit index edges are the release authority.
- Stop if an attestation is absent, non-canonical, has a bad checksum, or does
  not bind the current target commit, release config, source image identity,
  source mode, local target image ID, and checked-in harness bytes.
