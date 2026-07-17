#!/usr/bin/env bash
set -euo pipefail

release_die() {
  printf 'release gate failed: %s\n' "$*" >&2
  exit 1
}

release_init() {
  [[ $# -eq 1 ]] || release_die 'usage: <script> vX.Y.Z'
  RELEASE_VERSION="$1"
  [[ "$RELEASE_VERSION" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || release_die 'version must be canonical vX.Y.Z'
  local discovered_root expected_output
  discovered_root="$(cd "$(dirname "${BASH_SOURCE[1]}")/../.." && pwd)"
  RELEASE_REPO_ROOT="$discovered_root"
  expected_output="$RELEASE_REPO_ROOT/.release/$RELEASE_VERSION"
  [[ -z "${RELEASE_OUTPUT_DIR:-}" || "$RELEASE_OUTPUT_DIR" == "$expected_output" ]] || release_die 'release output must be the repository .release/version directory'
  RELEASE_OUTPUT_DIR="$expected_output"
  RELEASE_EXPECTED_BRANCH="${RELEASE_EXPECTED_BRANCH:-main}"
  RELEASE_CONTEXT="$RELEASE_OUTPUT_DIR/release-context.json"
  RELEASE_VERIFIED="$RELEASE_OUTPUT_DIR/verified.json"
  RELEASE_IMAGE="dirextalk/message-server:$RELEASE_VERSION"
  export RELEASE_VERSION RELEASE_REPO_ROOT RELEASE_OUTPUT_DIR RELEASE_EXPECTED_BRANCH RELEASE_CONTEXT RELEASE_VERIFIED RELEASE_IMAGE
}

release_require_tools() {
  local tool
  for tool in "$@"; do
    command -v "$tool" >/dev/null 2>&1 || release_die "required tool is unavailable: $tool"
  done
}

release_preflight() {
  release_require_tools git python3
  cd "$RELEASE_REPO_ROOT"
  [[ -z "$(git status --porcelain)" ]] || release_die 'working tree must be clean'
  local branch head remote_line remote_head source_version
  branch="$(git branch --show-current)"
  [[ "$branch" == "$RELEASE_EXPECTED_BRANCH" ]] || release_die "release branch must be $RELEASE_EXPECTED_BRANCH"
  head="$(git rev-parse HEAD)"
  remote_line="$(git ls-remote --exit-code origin "refs/heads/$RELEASE_EXPECTED_BRANCH")"
  [[ "$remote_line" =~ ^([0-9a-f]{40})[[:space:]]refs/heads/$RELEASE_EXPECTED_BRANCH$ ]] || release_die 'remote release branch response is invalid'
  remote_head="${BASH_REMATCH[1]}"
  [[ "$head" =~ ^[0-9a-f]{40}$ && "$head" == "$remote_head" ]] || release_die 'HEAD must exactly match the pushed release branch'
  grep -Eq "^##[[:space:]]+$RELEASE_VERSION([[:space:]]|$)" release/RELEASE_NOTES.md || release_die 'matching release notes section is required'
  source_version="$(python3 - internal/version.go <<'PY'
import re, sys
text = open(sys.argv[1], encoding='utf-8').read()
match = re.search(r'(?m)^\s*version\s*=\s*"([^"]+)"', text)
print(match.group(1) if match else '')
PY
)"
  [[ "$source_version" == "$RELEASE_VERSION" ]] || release_die 'source default version does not match release version'
  RELEASE_COMMIT="$head"
  RELEASE_BUILD_TIME="$(git show -s --format=%cI HEAD)"
  [[ "$RELEASE_BUILD_TIME" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T ]] || release_die 'commit build time is invalid'
  export RELEASE_COMMIT RELEASE_BUILD_TIME
}

release_write_context() {
  mkdir -p "$RELEASE_OUTPUT_DIR"
  python3 - "$RELEASE_CONTEXT" "$RELEASE_VERSION" "$RELEASE_COMMIT" "$RELEASE_BUILD_TIME" "$RELEASE_IMAGE" <<'PY'
import json, os, pathlib, sys, tempfile
path = pathlib.Path(sys.argv[1])
value = dict(zip(("version", "commit", "build_time", "image"), sys.argv[2:]))
data = json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
fd, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
try:
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
finally:
    if os.path.exists(temporary):
        os.unlink(temporary)
PY
}

release_require_context() {
  [[ -f "$RELEASE_CONTEXT" ]] || release_die 'prepare must complete first'
  local values current_head
  current_head="$(cd "$RELEASE_REPO_ROOT" && git rev-parse HEAD)"
  values="$(python3 - "$RELEASE_CONTEXT" <<'PY'
import json, re, sys
raw = open(sys.argv[1], "rb").read()
value = json.loads(raw)
required = {"version", "commit", "build_time", "image"}
if set(value) != required or raw != json.dumps(value, separators=(",", ":"), sort_keys=True).encode():
    raise SystemExit("release context is not canonical")
patterns = {
    "version": r"^v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$",
    "commit": r"^[0-9a-f]{40}$",
    "build_time": r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[^\r\n]+$",
    "image": r"^dirextalk/message-server:v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$",
}
if any(not isinstance(value[key], str) or not re.fullmatch(pattern, value[key]) for key, pattern in patterns.items()):
    raise SystemExit("release context value is invalid")
for key in ("version", "commit", "build_time", "image"):
    print(value[key])
PY
)" || release_die 'prepared context is invalid'
  mapfile -t context_values <<<"$values"
  [[ "${#context_values[@]}" == 4 && "${context_values[0]}" == "$RELEASE_VERSION" && "${context_values[1]}" == "$current_head" && "${context_values[1]}" == "$RELEASE_COMMIT" && "${context_values[2]}" == "$RELEASE_BUILD_TIME" && "${context_values[3]}" == "$RELEASE_IMAGE" ]] || release_die 'prepared context does not match current release inputs'
}

release_write_verified() {
  local local_image_id="$1"
  [[ "$local_image_id" =~ ^sha256:[0-9a-f]{64}$ ]] || release_die 'local verified image ID is invalid'
  python3 - "$RELEASE_VERIFIED" "$RELEASE_VERSION" "$RELEASE_COMMIT" "$RELEASE_IMAGE" "$local_image_id" <<'PY'
import json, os, pathlib, sys, tempfile
path = pathlib.Path(sys.argv[1])
value = {"version": sys.argv[2], "commit": sys.argv[3], "image": sys.argv[4], "local_image_id": sys.argv[5], "tests": "passed", "image_probe": "passed"}
data = json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
fd, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
try:
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
finally:
    if os.path.exists(temporary):
        os.unlink(temporary)
PY
}

release_require_verified() {
  [[ -f "$RELEASE_VERIFIED" ]] || release_die 'verify must complete first'
  local values current_image_id
  values="$(python3 - "$RELEASE_VERIFIED" <<'PY'
import json, re, sys
raw = open(sys.argv[1], "rb").read()
value = json.loads(raw)
required = {"version", "commit", "image", "local_image_id", "tests", "image_probe"}
if set(value) != required or raw != json.dumps(value, separators=(",", ":"), sort_keys=True).encode():
    raise SystemExit("verification evidence is not canonical")
if value["tests"] != "passed" or value["image_probe"] != "passed" or not re.fullmatch(r"sha256:[0-9a-f]{64}", value["local_image_id"]):
    raise SystemExit("verification evidence is invalid")
for key in ("version", "commit", "image", "local_image_id"):
    print(value[key])
PY
)" || release_die 'verification evidence is invalid'
  mapfile -t verified_values <<<"$values"
  [[ "${#verified_values[@]}" == 4 && "${verified_values[0]}" == "$RELEASE_VERSION" && "${verified_values[1]}" == "$RELEASE_COMMIT" && "${verified_values[2]}" == "$RELEASE_IMAGE" ]] || release_die 'verification evidence does not match release context'
  current_image_id="$(docker image inspect "$RELEASE_IMAGE" --format '{{.Id}}')"
  [[ "$current_image_id" == "${verified_values[3]}" ]] || release_die 'local release image changed after verification'
}

release_remote_digest() {
  docker buildx imagetools inspect "$1" --format '{{json .Manifest}}' \
    | python3 -c 'import json,sys; value=json.load(sys.stdin); digest=value.get("digest", ""); print(digest) if isinstance(digest, str) else sys.exit(1)'
}
