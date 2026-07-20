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
  local discovered_root configured_root configured_output expected_output
  discovered_root="$(cd "$(dirname "${BASH_SOURCE[1]}")/../.." && pwd)"
  configured_root="${RELEASE_REPO_ROOT:-$discovered_root}"
  if [[ "$configured_root" != "$discovered_root" && "${RELEASE_CONTRACT_TEST:-0}" != 1 ]]; then
    release_die 'RELEASE_REPO_ROOT override is allowed only in contract tests'
  fi
  RELEASE_REPO_ROOT="$(cd "$configured_root" && pwd -P)"
  expected_output="$RELEASE_REPO_ROOT/.release/$RELEASE_VERSION"
  configured_output="${RELEASE_OUTPUT_DIR:-$expected_output}"
  if [[ "$configured_output" != "$expected_output" && "${RELEASE_CONTRACT_TEST:-0}" != 1 ]]; then
    release_die 'release output must be the repository .release/version directory'
  fi
  RELEASE_OUTPUT_DIR="$configured_output"
  RELEASE_EXPECTED_BRANCH="${RELEASE_EXPECTED_BRANCH:-main}"
  RELEASE_CONFIG="$RELEASE_REPO_ROOT/release/$RELEASE_VERSION.json"
  RELEASE_CONTEXT="$RELEASE_OUTPUT_DIR/release-context.json"
  RELEASE_VERIFIED="$RELEASE_OUTPUT_DIR/verified.json"
  RELEASE_ATTESTATION_DIR="${RELEASE_ATTESTATION_DIR:-$RELEASE_REPO_ROOT/.release-attestations/$RELEASE_VERSION}"
  RELEASE_IMAGE="dirextalk/message-server:$RELEASE_VERSION"
  export RELEASE_VERSION RELEASE_REPO_ROOT RELEASE_OUTPUT_DIR RELEASE_EXPECTED_BRANCH RELEASE_CONFIG RELEASE_CONTEXT RELEASE_VERIFIED RELEASE_ATTESTATION_DIR RELEASE_IMAGE
}

release_require_tools() {
  local tool
  for tool in "$@"; do
    command -v "$tool" >/dev/null 2>&1 || release_die "required tool is unavailable: $tool"
  done
}

release_validate_config() {
  [[ -f "$RELEASE_CONFIG" ]] || release_die "missing release config release/$RELEASE_VERSION.json"
  python3 - "$RELEASE_CONFIG" "$RELEASE_VERSION" <<'PY'
import json, re, sys
path, expected = sys.argv[1:]
try:
    raw = open(path, 'rb').read()
    config = json.loads(raw)
except Exception as exc:
    raise SystemExit(f"invalid release config: {exc}")
required = {
    "version", "previous_version", "baseline_reset_from_version", "upgrade_from", "source_test_modes", "minimum_client_version",
    "maximum_client_version_exclusive", "schema_version",
    "schema_compat_version", "upgrade_edges",
}
optional = {"baseline_reset_from_version"}
if set(config) not in (required - optional, required):
    raise SystemExit("release config fields do not match the fixed contract")
version_re = re.compile(r"^v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$")
digest_re = re.compile(r"^sha256:[0-9a-f]{64}$")
if config["version"] != expected or not version_re.fullmatch(expected):
    raise SystemExit("release config version mismatch")
if not isinstance(config["previous_version"], str) or (config["previous_version"] and not version_re.fullmatch(config["previous_version"])):
    raise SystemExit("previous_version must be empty or canonical")
baseline = config.get("baseline_reset_from_version", "")
if not isinstance(baseline, str) or (baseline and not version_re.fullmatch(baseline)):
    raise SystemExit("baseline_reset_from_version must be empty or canonical")
if config["previous_version"]:
    if baseline:
        raise SystemExit("normal releases must not declare a baseline reset")
    if tuple(map(int, config["previous_version"][1:].split('.'))) >= tuple(map(int, expected[1:].split('.'))):
        raise SystemExit("previous_version must identify an earlier formal release")
elif expected == "v1.0.0":
    if baseline:
        raise SystemExit("v1.0.0 must not declare a baseline reset")
elif not baseline or tuple(map(int, baseline[1:].split('.'))) >= tuple(map(int, expected[1:].split('.'))):
    raise SystemExit("a reset baseline must name an earlier exact source version")
if not isinstance(config["upgrade_from"], list) or not config["upgrade_from"] or not all(isinstance(v, str) and v.strip() == v and v for v in config["upgrade_from"]):
    raise SystemExit("upgrade_from must be a non-empty string list")
for field in ("minimum_client_version", "maximum_client_version_exclusive"):
    if not isinstance(config[field], str) or not version_re.fullmatch(config[field]):
        raise SystemExit(f"{field} must be canonical")
schema = config["schema_version"]
compat = config["schema_compat_version"]
if isinstance(schema, bool) or isinstance(compat, bool) or not isinstance(schema, int) or not isinstance(compat, int) or compat < 1 or schema < compat:
    raise SystemExit("schema compatibility is invalid")
edges = config["upgrade_edges"]
if not isinstance(edges, list) or not edges:
    raise SystemExit("at least one exact upgrade edge is required")
for edge in edges:
    if set(edge) != {"from_version", "from_image_digests", "to_version"}:
        raise SystemExit("upgrade edge fields do not match the fixed contract")
    if not version_re.fullmatch(edge["from_version"]) or edge["to_version"] != expected:
        raise SystemExit("upgrade edge version mismatch")
    digests = edge["from_image_digests"]
    if not isinstance(digests, list) or not digests or len(digests) != len(set(digests)) or not all(digest_re.fullmatch(v) for v in digests):
        raise SystemExit("upgrade edge requires unique exact source image digests")
    if digests != sorted(digests):
        raise SystemExit("upgrade edge source image digests must be sorted")
edge_sources = sorted({edge["from_version"] for edge in edges}, key=lambda value: tuple(map(int, value[1:].split('.'))))
expected_constraints = ["=" + value[1:] for value in edge_sources]
if config["upgrade_from"] != expected_constraints:
    raise SystemExit("upgrade_from must exactly match declared upgrade edge source versions")
if not config["previous_version"] and expected != "v1.0.0" and edge_sources != [baseline]:
    raise SystemExit("a reset baseline must declare only its exact source version")
all_digests = sorted(digest for edge in edges for digest in edge["from_image_digests"])
modes = config["source_test_modes"]
if not isinstance(modes, dict) or sorted(modes) != all_digests or any(mode not in {"registry", "offline_import"} for mode in modes.values()):
    raise SystemExit("source_test_modes must map every exact source digest to registry or offline_import")
PY
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
  release_validate_config
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
  local current_head values
  current_head="$(cd "$RELEASE_REPO_ROOT" && git rev-parse HEAD)"
  values="$(python3 - "$RELEASE_CONTEXT" <<'PY'
import json, re, sys
try:
    raw = open(sys.argv[1], "rb").read()
    value = json.loads(raw)
except Exception as exc:
    raise SystemExit(f"invalid release context: {exc}")
if set(value) != {"version", "commit", "build_time", "image"}:
    raise SystemExit("invalid release context fields")
patterns = {
    "version": r"^v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$",
    "commit": r"^[0-9a-f]{40}$",
    "build_time": r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[^\r\n]+$",
    "image": r"^dirextalk/message-server:v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$",
}
if any(not isinstance(value[key], str) or not re.fullmatch(pattern, value[key]) for key, pattern in patterns.items()):
    raise SystemExit("invalid release context value")
canonical = json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
if raw != canonical:
    raise SystemExit("release context is not canonical")
print(value["version"])
print(value["commit"])
print(value["build_time"])
print(value["image"])
PY
)" || release_die 'prepared context is invalid'
  mapfile -t context_values <<<"$values"
  [[ "${#context_values[@]}" == 4 ]] || release_die 'prepared context is incomplete'
  [[ "${context_values[0]}" == "$1" && "${context_values[0]}" == "$RELEASE_VERSION" && "${context_values[1]}" == "$current_head" && "${context_values[1]}" == "$RELEASE_COMMIT" && "${context_values[2]}" == "$RELEASE_BUILD_TIME" && "${context_values[3]}" == "$RELEASE_IMAGE" ]] || release_die 'prepared context does not match current HEAD/version/build/image'
}

release_write_verified() {
  local local_image_id="$1"
  local attestation_set_digest="$2"
  [[ "$local_image_id" =~ ^sha256:[0-9a-f]{64}$ ]] || release_die 'local verified image ID is invalid'
  [[ "$attestation_set_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || release_die 'attestation set digest is invalid'
  python3 - "$RELEASE_VERIFIED" "$RELEASE_VERSION" "$RELEASE_COMMIT" "$RELEASE_IMAGE" "$local_image_id" "$attestation_set_digest" <<'PY'
import json, os, pathlib, sys, tempfile
path = pathlib.Path(sys.argv[1])
value = {"version": sys.argv[2], "commit": sys.argv[3], "image": sys.argv[4], "local_image_id": sys.argv[5], "attestation_set_digest": sys.argv[6], "retained_upgrade_tests": "passed", "image_probe": "passed"}
data = json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
fd, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
try:
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data); stream.flush(); os.fsync(stream.fileno())
    os.replace(temporary, path)
finally:
    if os.path.exists(temporary): os.unlink(temporary)
PY
}

release_require_verified() {
  [[ -f "$RELEASE_VERIFIED" ]] || release_die 'verify must complete first'
  local values current_image_id
  values="$(python3 - "$RELEASE_VERIFIED" <<'PY'
import json, re, sys
raw = open(sys.argv[1], "rb").read()
value = json.loads(raw)
required = {"version", "commit", "image", "local_image_id", "attestation_set_digest", "retained_upgrade_tests", "image_probe"}
if set(value) != required or raw != json.dumps(value, separators=(",", ":"), sort_keys=True).encode():
    raise SystemExit("verification evidence is not canonical")
if value["retained_upgrade_tests"] != "passed" or value["image_probe"] != "passed":
    raise SystemExit("verification gates did not pass")
if not re.fullmatch(r"sha256:[0-9a-f]{64}", value["local_image_id"]):
    raise SystemExit("local image ID is invalid")
if not re.fullmatch(r"sha256:[0-9a-f]{64}", value["attestation_set_digest"]):
    raise SystemExit("attestation set digest is invalid")
for key in ("version", "commit", "image", "local_image_id", "attestation_set_digest"):
    print(value[key])
PY
)" || release_die 'verification evidence is invalid'
  mapfile -t verified_values <<<"$values"
  [[ "${#verified_values[@]}" == 5 && "${verified_values[0]}" == "$RELEASE_VERSION" && "${verified_values[1]}" == "$RELEASE_COMMIT" && "${verified_values[2]}" == "$RELEASE_IMAGE" ]] || release_die 'verification evidence does not match release context'
  current_image_id="$(docker image inspect "$RELEASE_IMAGE" --format '{{.Id}}')"
  [[ "$current_image_id" == "${verified_values[3]}" ]] || release_die 'local release image changed after verification'
  [[ "$(release_attestation_set_digest)" == "${verified_values[4]}" ]] || release_die 'retained-upgrade attestations changed after verification'
}

release_attestation_set_digest() {
  python3 - "$RELEASE_ATTESTATION_DIR" <<'PY'
import hashlib, pathlib, sys
root = pathlib.Path(sys.argv[1])
files = sorted(root.glob("release-attestation-*.json"))
if not files:
    raise SystemExit("no retained-upgrade attestations")
digest = hashlib.sha256()
for path in files:
    checksum = pathlib.Path(str(path) + ".sha256")
    if not checksum.is_file():
        raise SystemExit(f"missing attestation checksum: {path.name}")
    for item in (path, checksum):
        digest.update(item.name.encode())
        digest.update(b"\0")
        digest.update(item.read_bytes())
        digest.update(b"\0")
print("sha256:" + digest.hexdigest())
PY
}

release_sha256_file() {
  python3 - "$1" "$2" <<'PY'
import hashlib, pathlib, sys
path = pathlib.Path(sys.argv[1])
name = sys.argv[2]
print(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {name}")
PY
}

release_remote_digest() {
  docker buildx imagetools inspect "$1" --format '{{json .Manifest}}' \
    | python3 -c 'import json,sys; value=json.load(sys.stdin); digest=value.get("digest", ""); print(digest) if isinstance(digest, str) else sys.exit(1)'
}

release_verify_checksum() {
  local data_path="$1" checksum_path="$2" expected_name="$3"
  python3 - "$data_path" "$checksum_path" "$expected_name" <<'PY'
import hashlib, pathlib, re, sys
data_path, checksum_path, expected_name = map(pathlib.Path, sys.argv[1:])
raw = checksum_path.read_bytes()
match = re.fullmatch(rb"([0-9a-f]{64})  ([a-z0-9.-]+)\n", raw)
if not match or match.group(2).decode() != expected_name.name:
    raise SystemExit("checksum file is not canonical")
if match.group(1).decode() != hashlib.sha256(data_path.read_bytes()).hexdigest():
    raise SystemExit("checksum mismatch")
PY
}
