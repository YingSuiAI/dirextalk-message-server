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
  local tag="$1" notes_file="$2" metadata_file
  metadata_file="$RELEASE_OUTPUT_DIR/github-release.json"
  gh release view "$tag" \
    --repo YingSuiAI/dirextalk-message-server \
    --json tagName,name,body,isDraft,isPrerelease,assets >"$metadata_file"
  python3 - "$metadata_file" "$tag" "Dirextalk Message Server $tag" "$notes_file" <<'PY'
import json, pathlib, sys

metadata_path, expected_tag, expected_title, notes_path = sys.argv[1:]
try:
    metadata = json.loads(pathlib.Path(metadata_path).read_text(encoding="utf-8"))
except Exception as exc:
    raise SystemExit(f"invalid GitHub Release metadata: {exc}")

expected_notes = pathlib.Path(notes_path).read_text(encoding="utf-8")
if metadata.get("tagName") != expected_tag:
    raise SystemExit("GitHub Release is bound to another tag")
if metadata.get("isDraft") is not False or metadata.get("isPrerelease") is not False:
    raise SystemExit("GitHub Release must be formal")
if metadata.get("name") != expected_title:
    raise SystemExit("GitHub Release title does not match the checked-in contract")
if metadata.get("body") != expected_notes:
    raise SystemExit("GitHub Release notes do not match the checked-in contract")
assets = metadata.get("assets")
if not isinstance(assets, list) or assets:
    raise SystemExit("GitHub Release must not contain assets")
PY
}

write_expected_release_notes() {
  local notes_file="$1"
  python3 - release/RELEASE_NOTES.md "$RELEASE_VERSION" "$notes_file" <<'PY'
import pathlib, re, sys
source, version, destination = sys.argv[1:]
text = pathlib.Path(source).read_text(encoding="utf-8")
match = re.search(rf"(?ms)^##[ \t]+{re.escape(version)}[ \t]*\n.*?(?=^##[ \t]+v|\Z)", text)
if not match:
    raise SystemExit("release notes section is missing")
pathlib.Path(destination).write_text(match.group(0).rstrip() + "\n", encoding="utf-8", newline="\n")
PY
}

validate_remote_tag() {
  local output line hash ref direct="" peeled=""
  output="$(git ls-remote --tags origin "refs/tags/$RELEASE_VERSION" "refs/tags/$RELEASE_VERSION^{}")"
  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    [[ "$line" =~ ^([0-9a-f]{40})[[:space:]](.+)$ ]] || release_die 'remote release tag response is invalid'
    hash="${BASH_REMATCH[1]}"
    ref="${BASH_REMATCH[2]}"
    if [[ "$ref" == "refs/tags/$RELEASE_VERSION" ]]; then
      [[ -z "$direct" ]] || release_die 'remote release tag response contains duplicates'
      direct="$hash"
    elif [[ "$ref" == "refs/tags/$RELEASE_VERSION^{}" ]]; then
      [[ -z "$peeled" ]] || release_die 'remote release tag response contains duplicates'
      peeled="$hash"
    else
      release_die 'remote release tag response is invalid'
    fi
  done <<<"$output"

  if [[ -z "$direct" && -z "$peeled" ]]; then
    REMOTE_TAG_EXISTS=0
    return
  fi
  [[ -n "$direct" && -n "$peeled" ]] || release_die 'remote release tag must be annotated'
  [[ "$peeled" == "$RELEASE_COMMIT" ]] || release_die 'remote release tag already points to another commit'
  REMOTE_TAG_EXISTS=1
}

notes_file="$RELEASE_OUTPUT_DIR/release-notes.md"
write_expected_release_notes "$notes_file"

existing_tag="$(git tag --list "$RELEASE_VERSION")"
if [[ -n "$existing_tag" ]]; then
  [[ "$(git rev-list -n 1 "$RELEASE_VERSION")" == "$RELEASE_COMMIT" ]] || release_die 'release tag already points to another commit'
  [[ "$(git cat-file -t "$RELEASE_VERSION")" == tag ]] || release_die 'release tag must be annotated'
fi
validate_remote_tag

if formal_release_exists "$RELEASE_VERSION"; then
  [[ "$REMOTE_TAG_EXISTS" == 1 ]] || release_die 'existing GitHub Release requires its remote annotated tag'
  assert_formal_release "$RELEASE_VERSION" "$notes_file"
fi

verify_image "$RELEASE_IMAGE"
docker push "$RELEASE_IMAGE"
docker pull "$RELEASE_IMAGE" >/dev/null
verify_image "$RELEASE_IMAGE"

if [[ "$REMOTE_TAG_EXISTS" == 0 ]]; then
  if [[ -z "$existing_tag" ]]; then
    git tag -a "$RELEASE_VERSION" -m "Dirextalk Message Server $RELEASE_VERSION"
  fi
  git push origin "refs/tags/$RELEASE_VERSION"
fi
validate_remote_tag
[[ "$REMOTE_TAG_EXISTS" == 1 ]] || release_die 'remote release tag is missing after publication'

if formal_release_exists "$RELEASE_VERSION"; then
  assert_formal_release "$RELEASE_VERSION" "$notes_file"
else
  gh release create "$RELEASE_VERSION" \
    --repo YingSuiAI/dirextalk-message-server \
    --title "Dirextalk Message Server $RELEASE_VERSION" \
    --notes-file "$notes_file" \
    --verify-tag
fi
assert_formal_release "$RELEASE_VERSION" "$notes_file"

docker tag "$RELEASE_IMAGE" dirextalk/message-server:latest
docker push dirextalk/message-server:latest
docker pull dirextalk/message-server:latest >/dev/null
verify_image dirextalk/message-server:latest

printf 'release publish passed for %s\n' "$RELEASE_VERSION"
