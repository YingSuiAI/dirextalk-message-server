#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
prepare="$repo_root/scripts/release/prepare.sh"
verify="$repo_root/scripts/release/verify.sh"
publish="$repo_root/scripts/release/publish.sh"

fail() {
  printf 'release contract test failed: %s\n' "$*" >&2
  exit 1
}

for script in "$prepare" "$verify" "$publish"; do
  [[ -x "$script" ]] || fail "missing executable ${script#$repo_root/}"
done

grep -F 'dirextalk-message-server-release' "$repo_root/AGENTS.md" >/dev/null || fail 'AGENTS does not route stable releases to the release Skill'
grep -Eq '^[[:space:]]+tags:' "$repo_root/.github/workflows/ci.yml" || fail 'CI does not validate pushed version tags'
grep -F 'persist-credentials: false' "$repo_root/.github/workflows/release.yml" >/dev/null || fail 'release checkout persists repository credentials'
grep -Eq '^[[:space:]]+verify:$' "$repo_root/.github/workflows/release.yml" || fail 'release workflow has no isolated verify job'
grep -Eq '^[[:space:]]+publish:$' "$repo_root/.github/workflows/release.yml" || fail 'release workflow has no isolated publish job'
grep -F 'needs: verify' "$repo_root/.github/workflows/release.yml" >/dev/null || fail 'release publication does not depend on isolated verification'
grep -F 'offline_attestations_json:' "$repo_root/.github/workflows/release.yml" >/dev/null || fail 'hosted release workflow cannot receive offline exact-image attestations'
grep -F 'stage_offline_attestations.py' "$repo_root/.github/workflows/release.yml" >/dev/null || fail 'hosted release workflow does not stage offline attestations before verification'

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
test_go="${RELEASE_TEST_GO:-$(command -v go || true)}"
if [[ -z "$test_go" && -x /home/adam/.local/bin/go ]]; then
  test_go=/home/adam/.local/bin/go
fi
[[ -n "$test_go" ]] || fail 'Go is required to build the real metadata validator'
"$test_go" build -o "$tmp/release-validate" ./cmd/release-validate

make_fixture() {
  local name="$1"
  local fixture="$tmp/$name"
  mkdir -p "$fixture/bin" "$fixture/repo/release" "$fixture/repo/internal"
  cp "$prepare" "$verify" "$publish" "$repo_root/scripts/release/lib.sh" "$fixture/repo/"
  printf '%s\n' '## v1.0.0' >"$fixture/repo/release/RELEASE_NOTES.md"
  cat >"$fixture/repo/release/v1.0.0.json" <<'EOF'
{"version":"v1.0.0","previous_version":"","upgrade_from":["=0.15.2"],"source_test_modes":{"sha256:d57a0b7830f7248e29fe7c45c0848cb1167454709fd33effe07ff074415f571c":"offline_import"},"minimum_client_version":"v1.0.0","maximum_client_version_exclusive":"v2.0.0","schema_version":1,"schema_compat_version":1,"upgrade_edges":[{"from_version":"v0.15.2","from_image_digests":["sha256:d57a0b7830f7248e29fe7c45c0848cb1167454709fd33effe07ff074415f571c"],"to_version":"v1.0.0"}]}
EOF
  printf '%s\n' 'version = "v1.0.0"' >"$fixture/repo/internal/version.go"
  printf '%s\n' 'module example.test/release-fixture' >"$fixture/repo/go.mod"
  : >"$fixture/commands.log"

  cat >"$fixture/bin/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'git %s\n' "$*" >>"$RELEASE_TEST_LOG"
case "$1 ${2:-}" in
  'status --porcelain') printf '%s' "${FAKE_GIT_DIRTY:-}" ;;
  'branch --show-current') printf '%s\n' "${FAKE_GIT_BRANCH:-main}" ;;
  'rev-parse HEAD') printf '%s\n' "${FAKE_GIT_HEAD:-1111111111111111111111111111111111111111}" ;;
  'rev-parse origin/main') printf '%s\n' "${FAKE_GIT_REMOTE_HEAD:-${FAKE_GIT_HEAD:-1111111111111111111111111111111111111111}}" ;;
  'ls-remote --exit-code')
    if [[ "$*" == *'refs/tags/'* ]]; then
      tag="${*: -1}"
      printf '%s\t%s\n' "${FAKE_GIT_REMOTE_TAG_HEAD:-${FAKE_GIT_HEAD:-1111111111111111111111111111111111111111}}" "$tag"
    else
      printf '%s\trefs/heads/main\n' "${FAKE_GIT_LS_REMOTE_HEAD:-${FAKE_GIT_HEAD:-1111111111111111111111111111111111111111}}"
    fi
    ;;
  'show -s') printf '%s\n' '2026-07-10T00:00:00Z' ;;
  'tag --list') printf '%s' "${FAKE_GIT_TAG:-}" ;;
  'rev-list -n') printf '%s\n' "${FAKE_GIT_TAG_HEAD:-${FAKE_GIT_HEAD:-1111111111111111111111111111111111111111}}" ;;
  'cat-file -t') printf '%s\n' "${FAKE_GIT_TAG_TYPE:-tag}" ;;
  *) ;;
esac
EOF

  cat >"$fixture/bin/retained-upgrade" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'retained-upgrade %s\n' "$*" >>"$RELEASE_TEST_LOG"
[[ "${FAKE_RETAINED_FAIL_PATTERN:-}" == '' || "$*" != *"$FAKE_RETAINED_FAIL_PATTERN"* ]]
declare -A values=()
while [[ $# -gt 0 ]]; do
  values["${1#--}"]="${2:-}"
  shift 2
done
python3 "$REAL_ATTESTATION_TOOL" create \
  --attestation "${values[attestation]}" \
  --from-version "${values[from-version]}" \
  --source-identity "${values[source-identity]}" \
  --source-mode "${values[source-mode]}" \
  --release-version "${values[target-version]}" \
  --target-commit "${values[target-commit]}" \
  --target-image "${values[target-image]}" \
  --target-image-id "${values[target-image-id]}" \
  --release-config "${values[release-config]}" \
  --runner "$0"
EOF

  cat >"$fixture/bin/go" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'go %s\n' "$*" >>"$RELEASE_TEST_LOG"
if [[ "${1:-} ${2:-}" == 'run ./cmd/release-validate' ]]; then
  shift 2
  exec "$REAL_RELEASE_VALIDATOR" "$@"
fi
[[ "${FAKE_GO_FAIL_PATTERN:-}" == '' || "$*" != *"$FAKE_GO_FAIL_PATTERN"* ]]
EOF

  cat >"$fixture/bin/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'docker %s\n' "$*" >>"$RELEASE_TEST_LOG"
if [[ "${FAKE_DOCKER_FAIL_PATTERN:-}" != '' && "$*" == *"$FAKE_DOCKER_FAIL_PATTERN"* ]]; then
  exit 1
fi
if [[ "$1" == push ]]; then
  : >"$RELEASE_TEST_DOCKER_STATE.fixed"
  : >"$RELEASE_TEST_DOCKER_STATE.fresh"
elif [[ "$*" == *'imagetools create'*'message-server:latest'* ]]; then
  digest='sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
  if [[ "$*" == *'message-server:latest@sha256:'* ]]; then
    digest="${*: -1}"
    digest="${digest##*@}"
  elif [[ "${FAKE_LATEST_MISMATCH:-0}" == 1 && ! -f "$RELEASE_TEST_DOCKER_STATE.mismatch-used" ]]; then
    digest='sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'
    : >"$RELEASE_TEST_DOCKER_STATE.mismatch-used"
  fi
  printf '%s\n' "$digest" >"$RELEASE_TEST_DOCKER_STATE.latest"
elif [[ "$*" == *'imagetools inspect'*'message-server:v'* && "$*" != *'message-server:latest'* ]]; then
  [[ -f "$RELEASE_TEST_DOCKER_STATE.fixed" ]] || exit 1
  printf '%s\n' '{"digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}'
elif [[ "$*" == *'imagetools inspect'*'message-server:latest'* ]]; then
  if [[ -f "$RELEASE_TEST_DOCKER_STATE.latest" ]]; then
    digest="$(cat "$RELEASE_TEST_DOCKER_STATE.latest")"
  elif [[ -n "${FAKE_EXISTING_LATEST_DIGEST:-}" ]]; then
    digest="$FAKE_EXISTING_LATEST_DIGEST"
  else
    exit 1
  fi
  printf '{"digest":"%s"}\n' "$digest"
elif [[ "$*" == *'image inspect'*'{{.Id}}'* ]]; then
  printf '%s\n' "${FAKE_LOCAL_IMAGE_ID:-sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb}"
elif [[ "$*" == *'image inspect'* ]]; then
  revision="${FAKE_IMAGE_REVISION:-1111111111111111111111111111111111111111}"
  if [[ -f "$RELEASE_TEST_DOCKER_STATE.fresh" ]]; then
    revision="${FAKE_FRESH_IMAGE_REVISION:-$revision}"
  fi
  printf '%s\n' "$RELEASE_VERSION|$revision|2026-07-10T00:00:00Z"
elif [[ "$*" == *'--entrypoint /usr/bin/dirextalk-message-server'* && "$*" == *' version' ]]; then
  printf '%s\n' "$RELEASE_VERSION"
fi
EOF

  cat >"$fixture/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'gh %s\n' "$*" >>"$RELEASE_TEST_LOG"
if [[ "${FAKE_GH_FAIL:-0}" == '1' ]]; then
  exit 1
fi
if [[ "${1:-} ${2:-}" == 'release view' ]]; then
  [[ -f "$RELEASE_TEST_GH_STATE.release" ]] || exit 1
  if [[ "$*" == *'--json'* ]]; then
    printf '%s\t%s\t%s\n' "${FAKE_GH_TAG:-${3:-}}" "${FAKE_GH_DRAFT:-false}" "${FAKE_GH_PRERELEASE:-false}"
  fi
elif [[ "${1:-} ${2:-}" == 'release create' ]]; then
  : >"$RELEASE_TEST_GH_STATE.release"
elif [[ "${1:-} ${2:-}" == 'release download' ]]; then
  release_tag="${3:-}"
  [[ -f "$RELEASE_TEST_GH_STATE.release" ]] || exit 1
  destination=''
  while [[ $# -gt 0 ]]; do
    if [[ "$1" == --dir ]]; then
      destination="$2"
      break
    fi
    shift
  done
  [[ -n "$destination" ]] || exit 1
  mkdir -p "$destination"
  if [[ "$release_tag" == v1.0.0 && -f "${RELEASE_TEST_PREVIOUS_DIR:-}/release-index.json" ]]; then
    cp "$RELEASE_TEST_PREVIOUS_DIR"/release-index.json "$RELEASE_TEST_PREVIOUS_DIR"/release-index.json.sha256 "$destination/"
  else
    cp "$RELEASE_OUTPUT_DIR"/release-manifest.json "$RELEASE_OUTPUT_DIR"/release-manifest.json.sha256 "$RELEASE_OUTPUT_DIR"/release-index.json "$RELEASE_OUTPUT_DIR"/release-index.json.sha256 "$destination/"
    cp "$RELEASE_ATTESTATION_DIR"/release-attestation-*.json "$RELEASE_ATTESTATION_DIR"/release-attestation-*.json.sha256 "$destination/"
  fi
  if [[ -n "${FAKE_GH_TAMPER_ASSET:-}" && -f "$destination/$FAKE_GH_TAMPER_ASSET" ]]; then
    printf 'tampered' >>"$destination/$FAKE_GH_TAMPER_ASSET"
  fi
fi
EOF

  chmod +x "$fixture/bin/"*
  printf '%s\n' "$fixture"
}

run_script() {
  local fixture="$1"
  local script="$2"
  shift 2
  run_script_version "$fixture" "$script" v1.0.0 "$@"
}

run_script_version() {
  local fixture="$1"
  local script="$2"
  local version="$3"
  shift 3
  (
    cd "$fixture/repo"
    PATH="$fixture/bin:$PATH" \
      RELEASE_REPO_ROOT="$fixture/repo" \
      RELEASE_OUTPUT_DIR="$fixture/out" \
      RELEASE_TEST_LOG="$fixture/commands.log" \
      RELEASE_CONTRACT_TEST=1 \
      REAL_RELEASE_VALIDATOR="$tmp/release-validate" \
      REAL_ATTESTATION_TOOL="$repo_root/scripts/release/retained_upgrade_attestation.py" \
      RELEASE_ATTESTATION_TOOL="$repo_root/scripts/release/retained_upgrade_attestation.py" \
      RELEASE_TEST_LOCAL_IMAGE_ID="sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" \
      RELEASE_TEST_DOCKER_STATE="$fixture/docker-state" \
      RELEASE_TEST_GH_STATE="$fixture/gh-state" \
      RELEASE_TEST_PREVIOUS_DIR="$fixture/previous-release" \
      RELEASE_ATTESTATION_DIR="$fixture/attestations" \
      RELEASE_RETAINED_UPGRADE_RUNNER="$fixture/bin/retained-upgrade" \
      "$@" "$fixture/repo/$script" "$version"
  )
}

make_next_fixture() {
  local name="$1"
  local previous_latest="$2"
  local fixture
  fixture="$(make_fixture "$name")"
  cat >>"$fixture/repo/release/RELEASE_NOTES.md" <<'EOF'

## v1.1.0

Next stable release.
EOF
  cat >"$fixture/repo/release/v1.1.0.json" <<'EOF'
{"version":"v1.1.0","previous_version":"v1.0.0","upgrade_from":["=1.0.0"],"source_test_modes":{"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa":"registry"},"minimum_client_version":"v1.0.0","maximum_client_version_exclusive":"v2.0.0","schema_version":1,"schema_compat_version":1,"upgrade_edges":[{"from_version":"v1.0.0","from_image_digests":["sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],"to_version":"v1.1.0"}]}
EOF
  printf '%s\n' 'version = "v1.1.0"' >"$fixture/repo/internal/version.go"
  mkdir -p "$fixture/previous-release"
  python3 - "$fixture/previous-release" "$previous_latest" <<'PY'
import hashlib, json, pathlib, sys
output = pathlib.Path(sys.argv[1])
latest = sys.argv[2]
manifest = {"manifest_version":1,"version":"v1.0.0","image":"dirextalk/message-server:v1.0.0","image_digest":"sha256:"+"a"*64,"upgrade_from":["=0.15.2"],"schema_version":1,"schema_compat_version":1,"minimum_client_version":"v1.0.0","maximum_client_version_exclusive":"v2.0.0","backup_required":True,"rollback_supported":True,"rollback_mode":"restore_backup","release_notes_url":"https://github.com/YingSuiAI/dirextalk-message-server/releases/tag/v1.0.0"}
manifest_bytes = json.dumps(manifest, separators=(",", ":")).encode()
index = {"release_index_version":1,"latest_version":latest,"releases":[{"manifest":manifest,"manifest_digest":"sha256:"+hashlib.sha256(manifest_bytes).hexdigest()}],"upgrade_edges":[{"from_version":"v0.15.2","from_image_digests":["sha256:"+"d"*64],"to_version":"v1.0.0"}]}
data = json.dumps(index, separators=(",", ":")).encode()
(output / "release-index.json").write_bytes(data)
(output / "release-index.json.sha256").write_text(hashlib.sha256(data).hexdigest()+"  release-index.json\n", encoding="ascii")
PY
  : >"$fixture/gh-state.release"
  printf '%s\n' "$fixture"
}

fixture="$(make_fixture dirty)"
if run_script "$fixture" prepare.sh env FAKE_GIT_DIRTY=' M file.go'; then
  fail 'prepare accepted a dirty tree'
fi

fixture="$(make_fixture branch)"
if run_script "$fixture" prepare.sh env FAKE_GIT_BRANCH=feature; then
  fail 'prepare accepted a non-main branch'
fi

fixture="$(make_fixture remote)"
if run_script "$fixture" prepare.sh env FAKE_GIT_LS_REMOTE_HEAD=2222222222222222222222222222222222222222; then
  fail 'prepare accepted an unpushed HEAD'
fi

fixture="$(make_fixture output-boundary)"
if run_script "$fixture" prepare.sh env RELEASE_CONTRACT_TEST=0; then
  fail 'prepare accepted an output directory override outside formal repo output'
fi

fixture="$(make_next_fixture previous-tag-mismatch v0.9.0)"
run_script_version "$fixture" prepare.sh v1.1.0 env
run_script_version "$fixture" verify.sh v1.1.0 env
if run_script_version "$fixture" publish.sh v1.1.0 env; then
  fail 'publish accepted a previous release index whose latest_version did not match the pinned previous tag'
fi
if grep -E 'docker .*latest' "$fixture/commands.log" >/dev/null; then
  fail 'latest moved after previous release index/tag mismatch'
fi

fixture="$(make_next_fixture previous-checksum v1.0.0)"
printf '%064d  release-index.json\n' 0 >"$fixture/previous-release/release-index.json.sha256"
run_script_version "$fixture" prepare.sh v1.1.0 env
run_script_version "$fixture" verify.sh v1.1.0 env
if run_script_version "$fixture" publish.sh v1.1.0 env; then
  fail 'publish accepted a previous release index checksum mismatch'
fi

fixture="$(make_fixture notes)"
printf '%s\n' '# no matching release section' >"$fixture/repo/release/RELEASE_NOTES.md"
if run_script "$fixture" prepare.sh env; then
  fail 'prepare accepted missing release notes'
fi

fixture="$(make_fixture version)"
printf '%s\n' 'version = "v9.9.9"' >"$fixture/repo/internal/version.go"
if run_script "$fixture" prepare.sh env; then
  fail 'prepare accepted a mismatched source version'
fi

fixture="$(make_fixture gates)"
run_script "$fixture" prepare.sh env
if run_script "$fixture" verify.sh env FAKE_GO_FAIL_PATTERN='dendrite_upgrade_tests'; then
  fail 'verify ignored a failing cross-version upgrade gate'
fi

fixture="$(make_fixture injected-evidence)"
run_script "$fixture" prepare.sh env
printf '\ntouch %q\n' "$fixture/injected" >>"$fixture/out/release-context.json"
if run_script "$fixture" verify.sh env; then
  fail 'verify accepted tampered release context evidence'
fi
[[ ! -e "$fixture/injected" ]] || fail 'verify executed release context as shell'

fixture="$(make_fixture injected-verified)"
run_script "$fixture" prepare.sh env
run_script "$fixture" verify.sh env
printf '\ntouch %q\n' "$fixture/injected" >>"$fixture/out/verified.json"
if run_script "$fixture" publish.sh env; then
  fail 'publish accepted tampered verification evidence'
fi
[[ ! -e "$fixture/injected" ]] || fail 'publish executed verification evidence as shell'

fixture="$(make_fixture changed-local-image)"
run_script "$fixture" prepare.sh env
run_script "$fixture" verify.sh env
if run_script "$fixture" publish.sh env FAKE_LOCAL_IMAGE_ID=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc; then
  fail 'publish accepted a local image changed after verification'
fi

fixture="$(make_fixture changed-attestation)"
run_script "$fixture" prepare.sh env
run_script "$fixture" verify.sh env
printf ' ' >>"$(find "$fixture/attestations" -type f -name 'release-attestation-*.json' | head -1)"
if run_script "$fixture" publish.sh env; then
  fail 'publish accepted a retained-upgrade attestation changed after verification'
fi

fixture="$(make_fixture every-digest)"
python3 - "$fixture/repo/release/v1.0.0.json" <<'PY'
import json, pathlib, sys
path = pathlib.Path(sys.argv[1])
value = json.loads(path.read_text())
value['upgrade_edges'][0]['from_image_digests'].append('sha256:' + 'e' * 64)
value['source_test_modes']['sha256:' + 'e' * 64] = 'registry'
path.write_text(json.dumps(value, separators=(',', ':')) + '\n')
PY
run_script "$fixture" prepare.sh env
run_script "$fixture" verify.sh env
[[ "$(grep -c '^retained-upgrade ' "$fixture/commands.log" || true)" == 2 ]] || fail 'verify did not run every exact source digest'
grep -F -- '--from-image dirextalk/message-server:v0.15.2 --source-identity sha256:d57a0b7830f7248e29fe7c45c0848cb1167454709fd33effe07ff074415f571c --source-mode offline_import' "$fixture/commands.log" >/dev/null || fail 'verify omitted the exact offline d1 source image identity'
[[ "$(find "$fixture/attestations" -type f -name '*.json' | wc -l)" == 2 ]] || fail 'verify did not persist one versioned attestation per exact source identity'

fixture="$(make_fixture tampered-attestation)"
mkdir -p "$fixture/attestations"
printf '%s' '{"attestation_version":1,"target_commit":"wrong"}' >"$fixture/attestations/release-attestation-0.15.2-d57a0b7830f7248e29fe7c45c0848cb1167454709fd33effe07ff074415f571c.json"
run_script "$fixture" prepare.sh env
if run_script "$fixture" verify.sh env; then
  fail 'verify accepted a tampered retained-upgrade attestation'
fi

fixture="$(make_fixture retained-failure)"
run_script "$fixture" prepare.sh env
if run_script "$fixture" verify.sh env FAKE_RETAINED_FAIL_PATTERN=d57a0b; then
  fail 'verify ignored a failing exact-digest retained-data upgrade'
fi

fixture="$(make_fixture probe)"
run_script "$fixture" prepare.sh env
if run_script "$fixture" verify.sh env FAKE_DOCKER_FAIL_PATTERN='--entrypoint /usr/bin/dirextalk-message-server'; then
  fail 'verify ignored a failing image version probe'
fi

fixture="$(make_fixture tag)"
run_script "$fixture" prepare.sh env
run_script "$fixture" verify.sh env
if run_script "$fixture" publish.sh env FAKE_GIT_TAG=v1.0.0 FAKE_GIT_TAG_HEAD=2222222222222222222222222222222222222222; then
  fail 'publish accepted a tag bound to another commit'
fi

fixture="$(make_fixture latest)"
run_script "$fixture" prepare.sh env
run_script "$fixture" verify.sh env
if run_script "$fixture" publish.sh env FAKE_GH_FAIL=1; then
  fail 'publish unexpectedly succeeded when GitHub Release failed'
fi
if grep -E 'docker .*latest' "$fixture/commands.log" >/dev/null; then
  fail 'latest moved before GitHub Release succeeded'
fi

fixture="$(make_fixture immutable-fixed)"
run_script "$fixture" prepare.sh env
run_script "$fixture" verify.sh env
: >"$fixture/docker-state.fixed"
if run_script "$fixture" publish.sh env FAKE_IMAGE_REVISION=2222222222222222222222222222222222222222; then
  fail 'publish accepted an existing fixed image from another commit'
fi
if grep -E 'docker .*latest' "$fixture/commands.log" >/dev/null; then
  fail 'latest moved after fixed image identity mismatch'
fi

fixture="$(make_fixture immutable-fresh)"
run_script "$fixture" prepare.sh env
run_script "$fixture" verify.sh env
if run_script "$fixture" publish.sh env FAKE_FRESH_IMAGE_REVISION=2222222222222222222222222222222222222222; then
  fail 'publish accepted a newly pushed fixed image from another commit'
fi
if grep -E 'docker .*latest' "$fixture/commands.log" >/dev/null; then
  fail 'latest moved after fresh fixed image identity mismatch'
fi

fixture="$(make_fixture remote-tag)"
run_script "$fixture" prepare.sh env
run_script "$fixture" verify.sh env
if run_script "$fixture" publish.sh env FAKE_GIT_REMOTE_TAG_HEAD=2222222222222222222222222222222222222222; then
  fail 'publish accepted a remote release tag bound to another commit'
fi
if grep -E 'docker .*latest' "$fixture/commands.log" >/dev/null; then
  fail 'latest moved for a mismatched remote release tag'
fi
fixture="$(make_fixture draft-release)"
run_script "$fixture" prepare.sh env
run_script "$fixture" verify.sh env
: >"$fixture/gh-state.release"
if run_script "$fixture" publish.sh env FAKE_GH_DRAFT=true; then
  fail 'publish accepted an existing draft GitHub Release'
fi
if grep -E 'docker .*latest' "$fixture/commands.log" >/dev/null; then
  fail 'latest moved for a draft GitHub Release'
fi

fixture="$(make_fixture prerelease)"
run_script "$fixture" prepare.sh env
run_script "$fixture" verify.sh env
: >"$fixture/gh-state.release"
if run_script "$fixture" publish.sh env FAKE_GH_PRERELEASE=true; then
  fail 'publish accepted an existing prerelease GitHub Release'
fi
if grep -E 'docker .*latest' "$fixture/commands.log" >/dev/null; then
  fail 'latest moved for a prerelease GitHub Release'
fi

fixture="$(make_fixture release-tag-metadata)"
run_script "$fixture" prepare.sh env
run_script "$fixture" verify.sh env
: >"$fixture/gh-state.release"
if run_script "$fixture" publish.sh env FAKE_GH_TAG=v9.9.9; then
  fail 'publish accepted GitHub Release metadata for another tag'
fi

fixture="$(make_fixture immutable-assets)"
run_script "$fixture" prepare.sh env
run_script "$fixture" verify.sh env
if run_script "$fixture" publish.sh env FAKE_GH_TAMPER_ASSET=release-manifest.json; then
  fail 'publish accepted changed GitHub Release assets'
fi
if grep -E 'docker .*latest' "$fixture/commands.log" >/dev/null; then
  fail 'latest moved after GitHub Release asset verification failed'
fi
fixture="$(make_fixture latest-restore)"
run_script "$fixture" prepare.sh env
run_script "$fixture" verify.sh env
old_latest='sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd'
if run_script "$fixture" publish.sh env FAKE_EXISTING_LATEST_DIGEST="$old_latest" FAKE_LATEST_MISMATCH=1; then
  fail 'publish succeeded after latest resolved to the wrong digest'
fi
[[ "$(cat "$fixture/docker-state.latest")" == "$old_latest" ]] || fail 'publish did not restore the previous latest digest'
[[ "$(grep -c 'docker buildx imagetools create --prefer-index=false --tag dirextalk/message-server:latest' "$fixture/commands.log")" == 2 ]] || fail 'latest mismatch did not perform one move and one restoration'

fixture="$(make_fixture order)"
run_script "$fixture" prepare.sh env
run_script "$fixture" verify.sh env
run_script "$fixture" publish.sh env
fixed_push_line="$(grep -nE 'docker push .*:v1\.0\.0$' "$fixture/commands.log" | cut -d: -f1)"
release_line="$(grep -nE '^gh release (create|upload) v1\.0\.0' "$fixture/commands.log" | tail -1 | cut -d: -f1)"
latest_line="$(grep -nE 'docker .*latest' "$fixture/commands.log" | tail -1 | cut -d: -f1)"
[[ -n "$fixed_push_line" && -n "$release_line" && -n "$latest_line" ]] || fail 'publish omitted fixed image, GitHub Release, or latest movement'
(( fixed_push_line < release_line && release_line < latest_line )) || fail 'publish order is not fixed image -> GitHub Release -> latest'
grep -E 'docker buildx imagetools create .*--prefer-index=false.*--tag dirextalk/message-server:latest' "$fixture/commands.log" >/dev/null || fail 'latest movement can rewrap the fixed manifest'

for asset in release-manifest.json release-manifest.json.sha256 release-index.json release-index.json.sha256; do
  [[ -s "$fixture/out/$asset" ]] || fail "publish omitted $asset"
done

printf 'release contract tests passed\n'
