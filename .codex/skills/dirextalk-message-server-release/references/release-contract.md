# Stable release contract

The formal version is canonical `vX.Y.Z`. A release uses one Git commit, its
RFC3339 commit timestamp, one fixed Docker tag, and one immutable image digest.

## Required repository metadata

- `internal/version.go`: target default version.
- `release/RELEASE_NOTES.md`: matching `## vX.Y.Z` section.
- `release/vX.Y.Z.json`: client/schema compatibility plus tested upgrade edges.
- `previous_version` pins the exact formal Release that supplies release-index
  history. `source_test_modes` covers every source identity with `registry` or
  `offline_import`; it is release-process metadata and is not copied into the
  public index.
- Every edge binds `from_version`, one or more exact
  `from_image_digests`, and `to_version`. A formal source release must bind
  exactly its indexed manifest image digest.

## Required Release assets

- `release-manifest.json`
- `release-manifest.json.sha256`
- `release-index.json`
- `release-index.json.sha256`
- One `release-attestation-<from>-<identity>.json` plus checksum per declared
  exact source identity.

Checksum files use `<64 lowercase hex>  <asset-name>\n`. The manifest and index
assets are canonical Go-equivalent compact JSON without BOM, surrounding
whitespace, or a trailing LF. The indexed `manifest_digest` covers those exact
manifest bytes. The release-index checksum covers the complete index bytes.

The index contains strictly SemVer-ordered manifests and strictly ordered,
non-duplicated edges. Its final manifest equals `latest_version`. Updater plans
persist the exact ordered manifest/digest/edge chain; discovery changes cannot
rewrite an accepted plan.

An attestation is canonical compact JSON generated only after the Ubuntu 24.04
Compose harness proves portal login, profile persistence, Matrix room/message
persistence, and the target health version. It binds the exact source image ID,
source mode, target commit, release-config SHA256, checked-in runner SHA256, and
local target image ID. `verified.json` binds the complete attestation set.

The initial d1 identity
`sha256:d57a0b7830f7248e29fe7c45c0848cb1167454709fd33effe07ff074415f571c`
is not available from Docker Hub. It is `offline_import`; a release must use an
authorized `docker save`/`docker load` transfer and verify the loaded image ID.
Hosted CI must not substitute a tag, a nearby version, or a fabricated pass.
The stable-release workflow stages offline evidence only from its explicit
`offline_attestations_json` input. The staging script requires every and only
the config-declared offline identity, bounds decoded size, derives canonical
asset names/checksums, and leaves final semantic binding to `verify.sh`.

## Publication order

1. Pass repository tests, build-tagged retained-data upgrade coverage, image
   build, and embedded-version probe.
2. Push the fixed `dirextalk/message-server:vX.Y.Z` image.
3. Resolve its registry digest and render/validate assets against that digest.
4. Create or verify the matching Git tag and GitHub Release assets.
5. Only then move `dirextalk/message-server:latest` to the same digest and
   verify digest equality.

The scripts may leave an immutable fixed image or Git tag after a partial
failure. This is safe and retryable. They must never leave `latest` pointing at
an artifact whose GitHub Release and trusted index are incomplete.
