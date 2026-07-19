#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
prepare="$repo_root/scripts/release/prepare.sh"
verify="$repo_root/scripts/release/verify.sh"
publish="$repo_root/scripts/release/publish.sh"
lib="$repo_root/scripts/release/lib.sh"
workflow="$repo_root/.github/workflows/release.yml"

fail() {
  printf 'release contract test failed: %s\n' "$*" >&2
  exit 1
}

for script in "$prepare" "$verify" "$publish" "$lib"; do
  [[ -x "$script" ]] || fail "release script is not executable: ${script#$repo_root/}"
  bash -n "$script"
done

python3 - "$repo_root/go.mod" <<'PY' || fail 'go.mod contains a local filesystem replacement'
import json
import subprocess
import sys

module = json.loads(subprocess.check_output(["go", "mod", "edit", "-json", sys.argv[1]], text=True))
for replacement in module.get("Replace") or []:
    if not (replacement.get("New") or {}).get("Version"):
        raise SystemExit(1)
PY

for removed in release-index release-manifest attestation retained-upgrade minimum_client_version previous_version upgrade_edges; do
  if grep -F "$removed" "$lib" "$prepare" "$verify" "$publish" "$workflow" >/dev/null; then
    fail "current release flow still references obsolete $removed metadata"
  fi
done

python3 - "$publish" <<'PY'
import pathlib, sys
text = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
required = [
    'docker push "$RELEASE_IMAGE"',
    'git push origin "refs/tags/$RELEASE_VERSION"',
    'gh release create "$RELEASE_VERSION"',
    'docker buildx imagetools create --prefer-index=false --tag dirextalk/message-server:latest',
]
positions = []
for value in required:
    position = text.find(value)
    if position < 0:
        raise SystemExit(f"missing publication step: {value}")
    positions.append(position)
if positions != sorted(positions):
    raise SystemExit("publication order must be fixed image, tag, GitHub Release, then latest")
create_line = next(line for line in text.splitlines() if 'gh release create "$RELEASE_VERSION"' in line)
if 'release-' in create_line or 'assets' in create_line:
    raise SystemExit("GitHub Release creation must not upload assets")
PY

grep -F '[[ "$asset_count" == 0 ]]' "$publish" >/dev/null || fail 'published Release assets are not rejected'
grep -F -- '--build-arg "VERSION=$RELEASE_VERSION"' "$verify" >/dev/null || fail 'formal Docker build does not receive the canonical release version'
grep -F '[[ "$probe" == "$RELEASE_VERSION" ]]' "$verify" >/dev/null || fail 'formal Docker image version is not probed'
grep -F 'local release image changed after verification' "$lib" >/dev/null || fail 'publish is not bound to the verified local image'
grep -F 'HEAD must exactly match the pushed release branch' "$lib" >/dev/null || fail 'release is not bound to pushed main'
grep -F 'latest movement failed and previous latest restoration failed' "$publish" >/dev/null || fail 'latest rollback gate is missing'

printf 'release contract tests passed\n'
