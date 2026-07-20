#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

release_init "$@"
release_preflight
release_require_context "$RELEASE_VERSION"
release_require_tools docker gh python3
release_require_verified
cd "$RELEASE_REPO_ROOT"

verify_image() {
  local ref="$1" identity probe
  identity="$(docker image inspect "$ref" --format '{{index .Config.Labels "org.opencontainers.image.version"}}|{{index .Config.Labels "org.opencontainers.image.revision"}}|{{index .Config.Labels "org.opencontainers.image.created"}}')"
  [[ "$identity" == "$RELEASE_VERSION|$RELEASE_COMMIT|$RELEASE_BUILD_TIME" ]] || release_die 'release image metadata does not match the verified release'
  probe="$(docker run --rm --entrypoint /usr/bin/dirextalk-message-server "$ref" --version)"
  [[ "$probe" == "$RELEASE_VERSION" ]] || release_die 'release image reports a different version'
}

formal_release_exists() {
  gh release view "$1" --repo YingSuiAI/dirextalk-message-server >/dev/null 2>&1
}

assert_formal_release() {
  local tag="$1" metadata
  metadata="$(gh release view "$tag" --repo YingSuiAI/dirextalk-message-server --json tagName,isDraft,isPrerelease --jq '[.tagName,.isDraft,.isPrerelease]|@tsv')"
  [[ "$metadata" == "$tag"$'\tfalse\tfalse' ]] || release_die "GitHub Release $tag is draft, prerelease, or bound to another tag"
}

existing_tag="$(git tag --list "$RELEASE_VERSION")"
if [[ -n "$existing_tag" ]]; then
  [[ "$(git rev-list -n 1 "$RELEASE_VERSION")" == "$RELEASE_COMMIT" ]] || release_die 'release tag already points to another commit'
  [[ "$(git cat-file -t "$RELEASE_VERSION")" == tag ]] || release_die 'release tag must be annotated'
fi

verify_image "$RELEASE_IMAGE"
docker push "$RELEASE_IMAGE"
docker pull "$RELEASE_IMAGE" >/dev/null
verify_image "$RELEASE_IMAGE"

if [[ -z "$existing_tag" ]]; then
  git tag -a "$RELEASE_VERSION" -m "Dirextalk Message Server $RELEASE_VERSION"
fi
git push origin "refs/tags/$RELEASE_VERSION"
remote_tag_line="$(git ls-remote --exit-code origin "refs/tags/$RELEASE_VERSION^{}")"
[[ "$remote_tag_line" =~ ^([0-9a-f]{40})[[:space:]]refs/tags/$RELEASE_VERSION\^\{\}$ && "${BASH_REMATCH[1]}" == "$RELEASE_COMMIT" ]] || release_die 'remote release tag does not resolve to the release commit'

if formal_release_exists "$RELEASE_VERSION"; then
  assert_formal_release "$RELEASE_VERSION"
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
  gh release create "$RELEASE_VERSION" \
    --repo YingSuiAI/dirextalk-message-server \
    --title "Dirextalk Message Server $RELEASE_VERSION" \
    --notes-file "$notes_file" \
    --verify-tag
fi
assert_formal_release "$RELEASE_VERSION"

docker tag "$RELEASE_IMAGE" dirextalk/message-server:latest
docker push dirextalk/message-server:latest
docker pull dirextalk/message-server:latest >/dev/null
verify_image dirextalk/message-server:latest

printf 'release publish passed for %s\n' "$RELEASE_VERSION"
