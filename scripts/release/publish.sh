#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

release_init "$@"
release_preflight
release_require_context
release_require_tools docker gh python3
release_require_verified
cd "$RELEASE_REPO_ROOT"
verified_local_image_id="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["local_image_id"])' "$RELEASE_VERIFIED")"

verify_published_image() {
  local digest="$1" ref actual_image_id identity probe
  ref="$RELEASE_IMAGE@$digest"
  docker pull "$ref" >/dev/null
  actual_image_id="$(docker image inspect "$ref" --format '{{.Id}}')"
  [[ "$actual_image_id" == "$verified_local_image_id" ]] || release_die 'fixed image was not the image that passed local verification'
  identity="$(docker image inspect "$ref" --format '{{index .Config.Labels "org.opencontainers.image.version"}}|{{index .Config.Labels "org.opencontainers.image.revision"}}|{{index .Config.Labels "org.opencontainers.image.created"}}')"
  [[ "$identity" == "$RELEASE_VERSION|$RELEASE_COMMIT|$RELEASE_BUILD_TIME" ]] || release_die 'fixed image belongs to different release metadata'
  probe="$(docker run --rm --entrypoint /usr/bin/dirextalk-message-server "$ref" --version)"
  [[ "$probe" == "$RELEASE_VERSION" ]] || release_die 'fixed image reports a different version'
}

assert_formal_release() {
  local metadata asset_count
  metadata="$(gh release view "$RELEASE_VERSION" --repo YingSuiAI/dirextalk-message-server --json tagName,isDraft,isPrerelease --jq '[.tagName,.isDraft,.isPrerelease]|@tsv')"
  [[ "$metadata" == "$RELEASE_VERSION"$'\tfalse\tfalse' ]] || release_die 'GitHub Release is draft, prerelease, or bound to another tag'
  asset_count="$(gh release view "$RELEASE_VERSION" --repo YingSuiAI/dirextalk-message-server --json assets --jq '.assets|length')"
  [[ "$asset_count" == 0 ]] || release_die 'simplified GitHub Release must not contain assets'
}

existing_tag="$(git tag --list "$RELEASE_VERSION")"
if [[ -n "$existing_tag" ]]; then
  [[ "$(git rev-list -n 1 "$RELEASE_VERSION")" == "$RELEASE_COMMIT" ]] || release_die 'release tag already points to another commit'
  [[ "$(git cat-file -t "$RELEASE_VERSION")" == tag ]] || release_die 'release tag must be annotated'
fi

image_digest="$(release_remote_digest "$RELEASE_IMAGE" 2>/dev/null || true)"
if [[ -z "$image_digest" ]]; then
  docker push "$RELEASE_IMAGE"
  image_digest="$(release_remote_digest "$RELEASE_IMAGE")"
fi
[[ "$image_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || release_die 'fixed image did not resolve to one immutable sha256 digest'
verify_published_image "$image_digest"

if [[ -z "$existing_tag" ]]; then
  git tag -a "$RELEASE_VERSION" -m "Dirextalk Message Server $RELEASE_VERSION"
fi
git push origin "refs/tags/$RELEASE_VERSION"
remote_tag_line="$(git ls-remote --exit-code origin "refs/tags/$RELEASE_VERSION^{}")"
[[ "$remote_tag_line" =~ ^([0-9a-f]{40})[[:space:]]refs/tags/$RELEASE_VERSION\^\{\}$ && "${BASH_REMATCH[1]}" == "$RELEASE_COMMIT" ]] || release_die 'remote release tag does not resolve to the release commit'

if gh release view "$RELEASE_VERSION" --repo YingSuiAI/dirextalk-message-server >/dev/null 2>&1; then
  assert_formal_release
else
  notes_file="$RELEASE_OUTPUT_DIR/release-notes.md"
  python3 - release/RELEASE_NOTES.md "$RELEASE_VERSION" "$notes_file" <<'PY'
import pathlib, re, sys
source, version, destination = sys.argv[1:]
text = pathlib.Path(source).read_text(encoding="utf-8")
match = re.search(rf"(?ms)^##[ \t]+{re.escape(version)}[ \t]*\n.*?(?=^##[ \t]+v|\Z)", text)
if not match:
    raise SystemExit("release notes section is missing")
pathlib.Path(destination).write_text(match.group(0).rstrip() + "\n", encoding="utf-8", newline="\n")
PY
  gh release create "$RELEASE_VERSION" --repo YingSuiAI/dirextalk-message-server --title "Dirextalk Message Server $RELEASE_VERSION" --notes-file "$notes_file" --verify-tag
fi
assert_formal_release

fixed_ref="$RELEASE_IMAGE@$image_digest"
old_latest_digest="$(release_remote_digest dirextalk/message-server:latest 2>/dev/null || true)"
[[ "$old_latest_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || release_die 'an existing valid latest digest is required for recoverable publication'
move_status=0
docker buildx imagetools create --prefer-index=false --tag dirextalk/message-server:latest "$fixed_ref" || move_status=$?
latest_digest="$(release_remote_digest dirextalk/message-server:latest 2>/dev/null || true)"
if (( move_status != 0 )) || [[ "$latest_digest" != "$image_digest" ]]; then
  restore_status=0
  docker buildx imagetools create --prefer-index=false --tag dirextalk/message-server:latest "dirextalk/message-server:latest@$old_latest_digest" || restore_status=$?
  restored_digest="$(release_remote_digest dirextalk/message-server:latest 2>/dev/null || true)"
  (( restore_status == 0 )) && [[ "$restored_digest" == "$old_latest_digest" ]] || release_die 'latest movement failed and previous latest restoration failed'
  release_die 'latest digest does not equal the fixed release digest'
fi
printf 'release publish passed for %s at %s\n' "$RELEASE_VERSION" "$image_digest"
