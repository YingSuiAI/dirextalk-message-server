#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

release_init "$@"
release_preflight
release_require_context "$RELEASE_VERSION"
release_require_tools go docker gh python3
release_require_verified
cd "$RELEASE_REPO_ROOT"
verified_local_image_id="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["local_image_id"])' "$RELEASE_VERIFIED")"

verify_published_image() {
  local digest="$1" expected_image_id="$2" ref actual_image_id identity probe
  ref="$RELEASE_IMAGE@$digest"
  docker pull "$ref" >/dev/null
  actual_image_id="$(docker image inspect "$ref" --format '{{.Id}}')"
  [[ "$actual_image_id" == "$expected_image_id" ]] || release_die 'fixed image was not the image that passed local verification'
  identity="$(docker image inspect "$ref" --format '{{index .Config.Labels "org.opencontainers.image.version"}}|{{index .Config.Labels "org.opencontainers.image.revision"}}|{{index .Config.Labels "org.opencontainers.image.created"}}')"
  [[ "$identity" == "$RELEASE_VERSION|$RELEASE_COMMIT|$RELEASE_BUILD_TIME" ]] || release_die 'fixed image belongs to different release metadata'
  probe="$(docker run --rm --entrypoint /usr/bin/dirextalk-message-server "$ref" --version)"
  [[ "$probe" == "$RELEASE_VERSION" ]] || release_die 'fixed image reports a different version'
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

image_digest="$(release_remote_digest "$RELEASE_IMAGE" 2>/dev/null || true)"
if [[ -n "$image_digest" ]]; then
  [[ "$image_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || release_die 'existing fixed image has an invalid digest'
else
  docker push "$RELEASE_IMAGE"
  image_digest="$(release_remote_digest "$RELEASE_IMAGE")"
fi
[[ "$image_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || release_die 'fixed image did not resolve to one immutable sha256 digest'
verify_published_image "$image_digest" "$verified_local_image_id"

previous_version="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["previous_version"])' "$RELEASE_CONFIG")"
previous_index=''
if [[ -n "$previous_version" ]]; then
  latest_formal_version="$(gh release list --repo YingSuiAI/dirextalk-message-server --exclude-drafts --exclude-pre-releases --limit 1 --json tagName --jq '.[0].tagName // ""')"
  [[ "$latest_formal_version" == "$previous_version" ]] || release_die 'previous_version is not the latest formal GitHub Release'
  history_dir="$RELEASE_OUTPUT_DIR/history"
  rm -rf "$history_dir"
  mkdir -p "$history_dir"
  assert_formal_release "$previous_version"
  gh release download "$previous_version" --repo YingSuiAI/dirextalk-message-server --pattern release-index.json --pattern release-index.json.sha256 --dir "$history_dir"
  previous_index="$history_dir/release-index.json"
  release_verify_checksum "$previous_index" "$history_dir/release-index.json.sha256" release-index.json
fi
[[ "$RELEASE_VERSION" == v1.0.0 || -s "$previous_index" ]] || release_die 'a pinned previous trusted release index is required'

python3 - "$RELEASE_CONFIG" "$RELEASE_OUTPUT_DIR" "$image_digest" "$previous_index" <<'PY'
import hashlib, json, pathlib, re, sys
config_path, output_dir, image_digest, previous_path = sys.argv[1:]
config = json.load(open(config_path, encoding='utf-8'))
output = pathlib.Path(output_dir)
output.mkdir(parents=True, exist_ok=True)
version = config['version']
manifest = {
    'manifest_version': 1,
    'version': version,
    'image': f'dirextalk/message-server:{version}',
    'image_digest': image_digest,
    'upgrade_from': config['upgrade_from'],
    'schema_version': config['schema_version'],
    'schema_compat_version': config['schema_compat_version'],
    'minimum_client_version': config['minimum_client_version'],
    'maximum_client_version_exclusive': config['maximum_client_version_exclusive'],
    'backup_required': True,
    'rollback_supported': True,
    'rollback_mode': 'restore_backup',
    'release_notes_url': f'https://github.com/YingSuiAI/dirextalk-message-server/releases/tag/{version}',
}
def go_json(value):
    encoded = json.dumps(value, ensure_ascii=True, separators=(',', ':'))
    encoded = encoded.replace('&', r'\u0026').replace('<', r'\u003c').replace('>', r'\u003e')
    return encoded.encode()

manifest_compact = go_json(manifest)
manifest_file = output / 'release-manifest.json'
manifest_file.write_bytes(manifest_compact)
manifest_digest = 'sha256:' + hashlib.sha256(manifest_compact).hexdigest()

if previous_path:
    previous = json.load(open(previous_path, encoding='utf-8'))
    if set(previous) != {'release_index_version', 'latest_version', 'releases', 'upgrade_edges'} or previous['release_index_version'] != 1:
        raise SystemExit('previous release index has an invalid contract')
    if previous['latest_version'] != config['previous_version']:
        raise SystemExit('previous release index latest_version does not match the pinned previous tag')
    releases = previous['releases']
    edges = previous['upgrade_edges']
    if not releases or releases[-1].get('manifest', {}).get('version') != config['previous_version']:
        raise SystemExit('previous release index final manifest does not match the pinned previous tag')
else:
    releases, edges = [], []

def semver(value):
    match = re.fullmatch(r'v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)', value)
    if not match:
        raise SystemExit(f'invalid canonical version: {value}')
    return tuple(map(int, match.groups()))

if releases and semver(releases[-1]['manifest']['version']) >= semver(version):
    raise SystemExit('new version must be greater than previous latest')
releases = releases + [{'manifest': manifest, 'manifest_digest': manifest_digest}]
edges = edges + config['upgrade_edges']
edges.sort(key=lambda edge: (semver(edge['from_version']), semver(edge['to_version'])))
edge_keys = [(edge['from_version'], edge['to_version']) for edge in edges]
if len(edge_keys) != len(set(edge_keys)):
    raise SystemExit('duplicate release index upgrade edge')
index_file = output / 'release-index.json'
release_values = []
for release in releases:
    release_values.append(b'{"manifest":' + go_json(release['manifest']) + b',"manifest_digest":' + go_json(release['manifest_digest']) + b'}')
index_compact = (
    b'{"release_index_version":1,"latest_version":' + go_json(version)
    + b',"releases":[' + b','.join(release_values)
    + b'],"upgrade_edges":' + go_json(edges) + b'}'
)
index_file.write_bytes(index_compact)
for name in ('release-manifest.json', 'release-index.json'):
    data = (output / name).read_bytes()
    (output / (name + '.sha256')).write_text(f"{hashlib.sha256(data).hexdigest()}  {name}\n", encoding='ascii', newline='\n')
PY

go run ./cmd/release-validate \
  --manifest "$RELEASE_OUTPUT_DIR/release-manifest.json" \
  --manifest-checksum "$RELEASE_OUTPUT_DIR/release-manifest.json.sha256" \
  --index "$RELEASE_OUTPUT_DIR/release-index.json" \
  --index-checksum "$RELEASE_OUTPUT_DIR/release-index.json.sha256"

if [[ -z "$existing_tag" ]]; then
  git tag -a "$RELEASE_VERSION" -m "Dirextalk Message Server $RELEASE_VERSION"
fi
git push origin "refs/tags/$RELEASE_VERSION"
remote_tag_line="$(git ls-remote --exit-code origin "refs/tags/$RELEASE_VERSION^{}")"
[[ "$remote_tag_line" =~ ^([0-9a-f]{40})[[:space:]]refs/tags/$RELEASE_VERSION\^\{\}$ && "${BASH_REMATCH[1]}" == "$RELEASE_COMMIT" ]] || release_die 'remote release tag does not resolve to the release commit'

assets=(
  "$RELEASE_OUTPUT_DIR/release-manifest.json"
  "$RELEASE_OUTPUT_DIR/release-manifest.json.sha256"
  "$RELEASE_OUTPUT_DIR/release-index.json"
  "$RELEASE_OUTPUT_DIR/release-index.json.sha256"
)
mapfile -t attestation_assets < <(find "$RELEASE_ATTESTATION_DIR" -maxdepth 1 -type f \( -name 'release-attestation-*.json' -o -name 'release-attestation-*.json.sha256' \) -print | sort)
(( ${#attestation_assets[@]} > 0 )) || release_die 'retained-upgrade attestation assets are missing'
assets+=("${attestation_assets[@]}")
if formal_release_exists "$RELEASE_VERSION"; then
  assert_formal_release "$RELEASE_VERSION"
  existing_dir="$RELEASE_OUTPUT_DIR/existing-release"
  rm -rf "$existing_dir"
  mkdir -p "$existing_dir"
  gh release download "$RELEASE_VERSION" --repo YingSuiAI/dirextalk-message-server --pattern 'release-*.json*' --dir "$existing_dir"
  missing_assets=()
  for asset in "${assets[@]}"; do
    name="$(basename "$asset")"
    if [[ -f "$existing_dir/$name" ]]; then
      cmp -s "$asset" "$existing_dir/$name" || release_die "published asset $name differs from the prepared immutable asset"
    else
      missing_assets+=("$asset")
    fi
  done
  if (( ${#missing_assets[@]} > 0 )); then
    gh release upload "$RELEASE_VERSION" "${missing_assets[@]}" --repo YingSuiAI/dirextalk-message-server
  fi
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
  gh release create "$RELEASE_VERSION" "${assets[@]}" --repo YingSuiAI/dirextalk-message-server --title "Dirextalk Message Server $RELEASE_VERSION" --notes-file "$notes_file" --verify-tag
fi
assert_formal_release "$RELEASE_VERSION"
verified_assets="$RELEASE_OUTPUT_DIR/verified-release"
rm -rf "$verified_assets"
mkdir -p "$verified_assets"
gh release download "$RELEASE_VERSION" --repo YingSuiAI/dirextalk-message-server --pattern 'release-*.json*' --dir "$verified_assets"
for asset in "${assets[@]}"; do
  name="$(basename "$asset")"
  [[ -f "$verified_assets/$name" ]] && cmp -s "$asset" "$verified_assets/$name" || release_die "GitHub Release asset verification failed for $name"
done

fixed_ref="dirextalk/message-server:$RELEASE_VERSION@$image_digest"
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
