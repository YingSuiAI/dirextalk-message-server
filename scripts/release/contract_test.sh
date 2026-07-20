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

if grep -En 'release-(manifest|index)|attestation|previous_version|upgrade_from|upgrade_edges|source_test_modes|image_digest|imagetools|release download' \
  "$repo_root/scripts/release/lib.sh" "$verify" "$publish" "$repo_root/.github/workflows/release.yml"; then
  fail 'active release automation still depends on predecessor, digest, GitHub assets, or attestations'
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

make_fixture() {
  local name="$1"
  local version="${2:-v1.0.0}"
  local fixture="$tmp/$name"
  mkdir -p "$fixture/bin" "$fixture/repo/release" "$fixture/repo/internal"
  cp "$prepare" "$verify" "$publish" "$repo_root/scripts/release/lib.sh" "$fixture/repo/"
  printf '## %s\n\nStable release.\n' "$version" >"$fixture/repo/release/RELEASE_NOTES.md"
  python3 - "$fixture/repo/release/$version.json" "$version" <<'PY'
import json, pathlib, sys
path = pathlib.Path(sys.argv[1])
version = sys.argv[2]
path.write_text(json.dumps({
    "version": version,
    "minimum_client_version": "v1.0.0",
    "maximum_client_version_exclusive": "v2.0.0",
    "schema_version": 2,
    "schema_compat_version": 1,
}, separators=(",", ":")) + "\n", encoding="utf-8")
PY
  printf 'version = "%s"\n' "$version" >"$fixture/repo/internal/version.go"
  printf '%s\n' 'module example.test/release-fixture' >"$fixture/repo/go.mod"
  : >"$fixture/commands.log"

  apply_fixture_tools "$fixture"
  printf '%s\n' "$fixture"
}

apply_fixture_tools() {
  local fixture="$1"
  cat >"$fixture/bin/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'git %s\n' "$*" >>"$RELEASE_TEST_LOG"
case "$1 ${2:-}" in
  'status --porcelain') printf '%s' "${FAKE_GIT_DIRTY:-}" ;;
  'branch --show-current') printf '%s\n' "${FAKE_GIT_BRANCH:-main}" ;;
  'rev-parse HEAD') printf '%s\n' "${FAKE_GIT_HEAD:-1111111111111111111111111111111111111111}" ;;
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

  cat >"$fixture/bin/go" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'go %s\n' "$*" >>"$RELEASE_TEST_LOG"
[[ "${FAKE_GO_FAIL_PATTERN:-}" == '' || "$*" != *"$FAKE_GO_FAIL_PATTERN"* ]]
EOF

  cat >"$fixture/bin/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'docker %s\n' "$*" >>"$RELEASE_TEST_LOG"
if [[ "${FAKE_DOCKER_FAIL_PATTERN:-}" != '' && "$*" == *"$FAKE_DOCKER_FAIL_PATTERN"* ]]; then
  exit 1
fi
if [[ "$*" == *'image inspect'* ]]; then
  printf '%s\n' "$RELEASE_VERSION|${FAKE_IMAGE_REVISION:-1111111111111111111111111111111111111111}|2026-07-10T00:00:00Z"
elif [[ "$*" == *'--entrypoint /usr/bin/dirextalk-message-server'* && "$*" == *' --version' ]]; then
  printf '%s\n' "$RELEASE_VERSION"
fi
EOF

  cat >"$fixture/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'gh %s\n' "$*" >>"$RELEASE_TEST_LOG"
if [[ "${FAKE_GH_FAIL:-0}" == 1 ]]; then
  exit 1
fi
if [[ "${1:-} ${2:-}" == 'release view' ]]; then
  [[ -f "$RELEASE_TEST_GH_STATE.release" ]] || exit 1
  if [[ "$*" == *'--json'* ]]; then
    printf '%s\t%s\t%s\n' "${FAKE_GH_TAG:-${3:-}}" "${FAKE_GH_DRAFT:-false}" "${FAKE_GH_PRERELEASE:-false}"
  fi
elif [[ "${1:-} ${2:-}" == 'release create' ]]; then
  : >"$RELEASE_TEST_GH_STATE.release"
fi
EOF

  chmod +x "$fixture/bin/"*
}

run_script() {
  local fixture="$1"
  local script="$2"
  local version="${3:-v1.0.0}"
  shift 3 || true
  (
    cd "$fixture/repo"
    PATH="$fixture/bin:$PATH" \
      RELEASE_REPO_ROOT="$fixture/repo" \
      RELEASE_OUTPUT_DIR="$fixture/out" \
      RELEASE_TEST_LOG="$fixture/commands.log" \
      RELEASE_TEST_GH_STATE="$fixture/gh-state" \
      RELEASE_CONTRACT_TEST=1 \
      "$@" "$fixture/repo/$script" "$version"
  )
}

fixture="$(make_fixture dirty)"
if run_script "$fixture" prepare.sh v1.0.0 env FAKE_GIT_DIRTY=' M file.go'; then
  fail 'prepare accepted a dirty tree'
fi

fixture="$(make_fixture branch)"
if run_script "$fixture" prepare.sh v1.0.0 env FAKE_GIT_BRANCH=feature; then
  fail 'prepare accepted a non-main branch'
fi

fixture="$(make_fixture remote)"
if run_script "$fixture" prepare.sh v1.0.0 env FAKE_GIT_LS_REMOTE_HEAD=2222222222222222222222222222222222222222; then
  fail 'prepare accepted an unpushed HEAD'
fi

fixture="$(make_fixture output-boundary)"
if run_script "$fixture" prepare.sh v1.0.0 env RELEASE_CONTRACT_TEST=0; then
  fail 'prepare accepted an output directory override outside formal repo output'
fi

fixture="$(make_fixture notes)"
printf '%s\n' '# no matching release section' >"$fixture/repo/release/RELEASE_NOTES.md"
if run_script "$fixture" prepare.sh v1.0.0 env; then
  fail 'prepare accepted missing release notes'
fi

fixture="$(make_fixture version)"
printf '%s\n' 'version = "v9.9.9"' >"$fixture/repo/internal/version.go"
if run_script "$fixture" prepare.sh v1.0.0 env; then
  fail 'prepare accepted a mismatched source version'
fi

fixture="$(make_fixture obsolete-config)"
python3 - "$fixture/repo/release/v1.0.0.json" <<'PY'
import json, pathlib, sys
path = pathlib.Path(sys.argv[1])
value = json.loads(path.read_text())
value["previous_version"] = "v0.9.0"
path.write_text(json.dumps(value) + "\n")
PY
if run_script "$fixture" prepare.sh v1.0.0 env; then
  fail 'prepare accepted obsolete predecessor metadata'
fi

fixture="$(make_fixture arbitrary v9.4.2)"
run_script "$fixture" prepare.sh v9.4.2 env
run_script "$fixture" verify.sh v9.4.2 env
run_script "$fixture" publish.sh v9.4.2 env
grep -F 'docker push dirextalk/message-server:v9.4.2' "$fixture/commands.log" >/dev/null || fail 'arbitrary canonical version image was not published'
grep -F 'gh release create v9.4.2' "$fixture/commands.log" >/dev/null || fail 'arbitrary canonical version GitHub Release was not created'
grep -F 'docker push dirextalk/message-server:latest' "$fixture/commands.log" >/dev/null || fail 'latest tag was not published'

fixture="$(make_fixture gates)"
run_script "$fixture" prepare.sh v1.0.0 env
if run_script "$fixture" verify.sh v1.0.0 env FAKE_GO_FAIL_PATTERN='dendrite_upgrade_tests'; then
  fail 'verify ignored a failing retained-data upgrade test suite'
fi

fixture="$(make_fixture probe)"
run_script "$fixture" prepare.sh v1.0.0 env
if run_script "$fixture" verify.sh v1.0.0 env FAKE_DOCKER_FAIL_PATTERN='--entrypoint /usr/bin/dirextalk-message-server'; then
  fail 'verify ignored a failing image version probe'
fi

fixture="$(make_fixture injected-context)"
run_script "$fixture" prepare.sh v1.0.0 env
printf '\ntouch %q\n' "$fixture/injected" >>"$fixture/out/release-context.json"
if run_script "$fixture" verify.sh v1.0.0 env; then
  fail 'verify accepted tampered release context evidence'
fi
[[ ! -e "$fixture/injected" ]] || fail 'verify executed release context as shell'

fixture="$(make_fixture injected-verified)"
run_script "$fixture" prepare.sh v1.0.0 env
run_script "$fixture" verify.sh v1.0.0 env
printf '\ntouch %q\n' "$fixture/injected" >>"$fixture/out/verified.json"
if run_script "$fixture" publish.sh v1.0.0 env; then
  fail 'publish accepted tampered verification evidence'
fi
[[ ! -e "$fixture/injected" ]] || fail 'publish executed verification evidence as shell'

fixture="$(make_fixture changed-local-image)"
run_script "$fixture" prepare.sh v1.0.0 env
run_script "$fixture" verify.sh v1.0.0 env
if run_script "$fixture" publish.sh v1.0.0 env FAKE_IMAGE_REVISION=2222222222222222222222222222222222222222; then
  fail 'publish accepted a local image built from another commit'
fi

fixture="$(make_fixture tag)"
run_script "$fixture" prepare.sh v1.0.0 env
run_script "$fixture" verify.sh v1.0.0 env
if run_script "$fixture" publish.sh v1.0.0 env FAKE_GIT_TAG=v1.0.0 FAKE_GIT_TAG_HEAD=2222222222222222222222222222222222222222; then
  fail 'publish accepted a tag bound to another commit'
fi
if grep -F 'docker push dirextalk/message-server:v1.0.0' "$fixture/commands.log" >/dev/null; then
  fail 'version image moved after tag mismatch'
fi

fixture="$(make_fixture remote-tag)"
run_script "$fixture" prepare.sh v1.0.0 env
run_script "$fixture" verify.sh v1.0.0 env
if run_script "$fixture" publish.sh v1.0.0 env FAKE_GIT_REMOTE_TAG_HEAD=2222222222222222222222222222222222222222; then
  fail 'publish accepted a remote release tag bound to another commit'
fi
if grep -F 'docker push dirextalk/message-server:latest' "$fixture/commands.log" >/dev/null; then
  fail 'latest moved for a mismatched remote release tag'
fi

fixture="$(make_fixture github-failure)"
run_script "$fixture" prepare.sh v1.0.0 env
run_script "$fixture" verify.sh v1.0.0 env
if run_script "$fixture" publish.sh v1.0.0 env FAKE_GH_FAIL=1; then
  fail 'publish unexpectedly succeeded when GitHub Release failed'
fi
if grep -F 'docker push dirextalk/message-server:latest' "$fixture/commands.log" >/dev/null; then
  fail 'latest moved before GitHub Release succeeded'
fi

fixture="$(make_fixture draft-release)"
run_script "$fixture" prepare.sh v1.0.0 env
run_script "$fixture" verify.sh v1.0.0 env
: >"$fixture/gh-state.release"
if run_script "$fixture" publish.sh v1.0.0 env FAKE_GH_DRAFT=true; then
  fail 'publish accepted an existing draft GitHub Release'
fi

fixture="$(make_fixture order)"
run_script "$fixture" prepare.sh v1.0.0 env
run_script "$fixture" verify.sh v1.0.0 env
run_script "$fixture" publish.sh v1.0.0 env
fixed_push_line="$(grep -nF 'docker push dirextalk/message-server:v1.0.0' "$fixture/commands.log" | tail -1 | cut -d: -f1)"
release_line="$(grep -nF 'gh release create v1.0.0' "$fixture/commands.log" | tail -1 | cut -d: -f1)"
latest_push_line="$(grep -nF 'docker push dirextalk/message-server:latest' "$fixture/commands.log" | tail -1 | cut -d: -f1)"
[[ -n "$fixed_push_line" && -n "$release_line" && -n "$latest_push_line" ]] || fail 'publish omitted version image, GitHub Release, or latest update'
(( fixed_push_line < release_line && release_line < latest_push_line )) || fail 'publish order is not version image -> GitHub Release -> latest'

if grep -E 'gh release (create|upload).*\.json|gh release download|docker buildx imagetools|sha256:' "$fixture/commands.log"; then
  fail 'simplified publication still transfers release assets or validates digests'
fi

printf 'release contract tests passed\n'
