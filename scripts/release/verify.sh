#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

release_init "$@"
release_preflight
release_require_context "$RELEASE_VERSION"
release_require_tools go docker
cd "$RELEASE_REPO_ROOT"

go test ./internal/releasecontrol ./internal/httputil ./setup ./p2p ./internal/productpolicy -count=1
go test -tags=dendrite_upgrade_tests ./cmd/dendrite-upgrade-tests -count=1
go build ./cmd/dirextalk-message-server

docker build \
  --build-arg "VERSION=$RELEASE_VERSION" \
  --build-arg "COMMIT=$RELEASE_COMMIT" \
  --build-arg "BUILD_TIME=$RELEASE_BUILD_TIME" \
  --label "org.opencontainers.image.version=$RELEASE_VERSION" \
  --label "org.opencontainers.image.revision=$RELEASE_COMMIT" \
  --label "org.opencontainers.image.created=$RELEASE_BUILD_TIME" \
  --tag "$RELEASE_IMAGE" .

probe="$(docker run --rm --entrypoint /usr/bin/dirextalk-message-server "$RELEASE_IMAGE" version)"
[[ "$probe" == "$RELEASE_VERSION" ]] || release_die "image version probe returned $probe"
local_image_id="$(docker image inspect "$RELEASE_IMAGE" --format '{{.Id}}')"
[[ "$local_image_id" =~ ^sha256:[0-9a-f]{64}$ ]] || release_die 'local release image ID is invalid'

retained_runner="$RELEASE_REPO_ROOT/scripts/release/retained-upgrade.sh"
attestation_tool="$RELEASE_REPO_ROOT/scripts/release/retained_upgrade_attestation.py"
if [[ "${RELEASE_CONTRACT_TEST:-0}" == 1 ]]; then
  retained_runner="${RELEASE_RETAINED_UPGRADE_RUNNER:-$retained_runner}"
  attestation_tool="${RELEASE_ATTESTATION_TOOL:-$attestation_tool}"
elif [[ -n "${RELEASE_RETAINED_UPGRADE_RUNNER:-}" ]]; then
  release_die 'retained upgrade runner override is allowed only in contract tests'
elif [[ -n "${RELEASE_ATTESTATION_TOOL:-}" ]]; then
  release_die 'attestation tool override is allowed only in contract tests'
fi
[[ -x "$retained_runner" ]] || release_die 'exact-digest retained upgrade runner is unavailable'
[[ -f "$attestation_tool" ]] || release_die 'retained-upgrade attestation tool is unavailable'
mkdir -p "$RELEASE_ATTESTATION_DIR"
while IFS=$'\t' read -r from_version source_digest source_mode; do
  [[ -n "$from_version" && -n "$source_digest" && -n "$source_mode" ]] || release_die 'release config produced an empty upgrade edge'
  attestation="$RELEASE_ATTESTATION_DIR/release-attestation-${from_version#v}-${source_digest#sha256:}.json"
  if [[ ! -f "$attestation" ]]; then
    source_image="dirextalk/message-server:$from_version@$source_digest"
    if [[ "$source_mode" == offline_import ]]; then
      source_image="dirextalk/message-server:$from_version"
    fi
    "$retained_runner" \
      --from-version "$from_version" \
      --from-image "$source_image" \
      --source-identity "$source_digest" \
      --source-mode "$source_mode" \
      --target-version "$RELEASE_VERSION" \
      --target-image "$RELEASE_IMAGE" \
      --target-image-id "$local_image_id" \
      --target-commit "$RELEASE_COMMIT" \
      --release-config "$RELEASE_CONFIG" \
      --attestation "$attestation"
  fi
  python3 "$attestation_tool" verify \
    --attestation "$attestation" \
    --from-version "$from_version" \
    --source-identity "$source_digest" \
    --source-mode "$source_mode" \
    --release-version "$RELEASE_VERSION" \
    --target-commit "$RELEASE_COMMIT" \
    --target-image "$RELEASE_IMAGE" \
    --target-image-id "$local_image_id" \
    --release-config "$RELEASE_CONFIG" \
    --runner "$retained_runner"
done < <(python3 - "$RELEASE_CONFIG" <<'PY'
import json, sys
config = json.load(open(sys.argv[1], encoding='utf-8'))
for edge in config['upgrade_edges']:
    for digest in edge['from_image_digests']:
        print(f"{edge['from_version']}\t{digest}\t{config['source_test_modes'][digest]}")
PY
)
docker compose -f docker-compose.p2p.yml config >/dev/null
release_write_verified "$local_image_id" "$(release_attestation_set_digest)"
printf 'release verify passed for %s\n' "$RELEASE_VERSION"
