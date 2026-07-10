#!/usr/bin/env bash
set -euo pipefail

die() {
  printf 'retained upgrade failed: %s\n' "$*" >&2
  exit 1
}

from_version=''
from_image=''
target_version=''
target_image=''
source_identity=''
source_mode=''
target_commit=''
target_image_id=''
release_config=''
attestation=''
while [[ $# -gt 0 ]]; do
  case "$1" in
    --from-version) from_version="${2:-}"; shift 2 ;;
    --from-image) from_image="${2:-}"; shift 2 ;;
    --target-version) target_version="${2:-}"; shift 2 ;;
    --target-image) target_image="${2:-}"; shift 2 ;;
    --source-identity) source_identity="${2:-}"; shift 2 ;;
    --source-mode) source_mode="${2:-}"; shift 2 ;;
    --target-commit) target_commit="${2:-}"; shift 2 ;;
    --target-image-id) target_image_id="${2:-}"; shift 2 ;;
    --release-config) release_config="${2:-}"; shift 2 ;;
    --attestation) attestation="${2:-}"; shift 2 ;;
    *) die "unknown argument: $1" ;;
  esac
done

version_pattern='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'
digest_pattern='sha256:[0-9a-f]{64}'
[[ "$from_version" =~ $version_pattern && "$target_version" =~ $version_pattern ]] || die 'versions must be canonical'
[[ "$source_identity" =~ ^$digest_pattern$ ]] || die 'source image identity must be exact'
[[ "$source_mode" == registry || "$source_mode" == offline_import ]] || die 'source mode is invalid'
if [[ "$source_mode" == registry ]]; then
  [[ "$from_image" =~ ^dirextalk/message-server:$from_version@$digest_pattern$ ]] || die 'registry source must bind its version tag and digest'
else
  [[ "$from_image" == "dirextalk/message-server:$from_version" ]] || die 'offline source must use the locally imported version tag'
fi
[[ "$target_image" == "dirextalk/message-server:$target_version" ]] || die 'target image must equal the target version tag'
[[ "$target_commit" =~ ^[0-9a-f]{40}$ && "$target_image_id" =~ ^$digest_pattern$ ]] || die 'target commit/image identity is invalid'
[[ -f "$release_config" && -n "$attestation" ]] || die 'release config and attestation path are required'
[[ -r /etc/os-release ]] || die '/etc/os-release is unavailable'
# shellcheck disable=SC1091
source /etc/os-release
[[ "${ID:-}" == ubuntu && "${VERSION_ID:-}" == 24.04 && "$(uname -m)" == x86_64 ]] || die 'Ubuntu 24.04 x86_64 is required'
for tool in docker python3; do
  command -v "$tool" >/dev/null 2>&1 || die "$tool is required"
done
docker compose version >/dev/null

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
probe="$script_dir/retained_upgrade_probe.py"
attestation_tool="$script_dir/retained_upgrade_attestation.py"
[[ -f "$probe" ]] || die 'retained upgrade probe is missing'
[[ -f "$attestation_tool" ]] || die 'retained upgrade attestation tool is missing'
work_dir="$(mktemp -d)"
project="dirextalk-release-$(printf '%s' "$target_version-${from_image##*@}-$$" | tr -cd 'a-zA-Z0-9-' | tr 'A-Z' 'a-z' | cut -c1-54)"
compose_file="$work_dir/compose.yml"
state_file="$work_dir/state.json"
bootstrap_file="$work_dir/bootstrap.json"

cleanup() {
  local status=$?
  RELEASE_HARNESS_IMAGE="${RELEASE_HARNESS_IMAGE:-$from_image}" docker compose -p "$project" -f "$compose_file" down --volumes --remove-orphans >/dev/null 2>&1 || status=1
  rm -rf "$work_dir" || status=1
  exit "$status"
}
trap cleanup EXIT INT TERM

cat >"$compose_file" <<'YAML'
services:
  postgres:
    image: postgres:18-alpine
    environment:
      POSTGRES_USER: dirextalk_message_server
      POSTGRES_PASSWORD: dirextalk_message_server
      POSTGRES_DB: dirextalk_message_server
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U dirextalk_message_server -d dirextalk_message_server"]
      interval: 2s
      timeout: 3s
      retries: 60
    volumes:
      - postgres:/var/lib/postgresql
  message-server-init:
    image: ${RELEASE_HARNESS_IMAGE}
    pull_policy: never
    entrypoint: ["/bin/sh", "-c"]
    command:
      - |
        set -eu
        mkdir -p /etc/dirextalk-message-server /var/dirextalk-message-server
        test -f /etc/dirextalk-message-server/matrix_key.pem || /usr/bin/generate-keys -private-key /etc/dirextalk-message-server/matrix_key.pem
        if [ ! -f /etc/dirextalk-message-server/server.crt ] || [ ! -f /etc/dirextalk-message-server/server.key ]; then
          /usr/bin/generate-keys -tls-cert /etc/dirextalk-message-server/server.crt -tls-key /etc/dirextalk-message-server/server.key -server localhost
        fi
        if [ ! -f /etc/dirextalk-message-server/message-server.yaml ]; then
          /usr/bin/generate-config -dir /var/dirextalk-message-server -db 'postgres://dirextalk_message_server:dirextalk_message_server@postgres/dirextalk_message_server?sslmode=disable' -server localhost > /etc/dirextalk-message-server/message-server.yaml
        fi
        sed -i 's|well_known_client_name: .*|well_known_client_name: "http://localhost"|' /etc/dirextalk-message-server/message-server.yaml
    volumes:
      - config:/etc/dirextalk-message-server
      - data:/var/dirextalk-message-server
    depends_on:
      postgres:
        condition: service_healthy
  message-server:
    image: ${RELEASE_HARNESS_IMAGE}
    pull_policy: never
    environment:
      P2P_PORTAL_CREDENTIALS_FILE: /var/dirextalk-message-server/p2p/bootstrap.json
      P2P_PLUGIN_DOCKER_ENABLED: "false"
      P2P_NATIVE_AGENT_DATA_DIR: /var/dirextalk-message-server/agent
    command: ["--config", "/etc/dirextalk-message-server/message-server.yaml", "--http-bind-address", ":8008", "--https-bind-address", ":8448"]
    ports:
      - "127.0.0.1::8008"
    volumes:
      - config:/etc/dirextalk-message-server
      - data:/var/dirextalk-message-server
    depends_on:
      postgres:
        condition: service_healthy
      message-server-init:
        condition: service_completed_successfully
volumes:
  postgres:
  config:
  data:
YAML

if [[ "$source_mode" == registry ]]; then
  docker pull "$from_image" >/dev/null
fi
actual_source_identity="$(docker image inspect "$from_image" --format '{{.Id}}')"
[[ "$actual_source_identity" == "$source_identity" ]] || die "local source image ID $actual_source_identity does not match attested identity $source_identity"
actual_target_identity="$(docker image inspect "$target_image" --format '{{.Id}}')"
[[ "$actual_target_identity" == "$target_image_id" ]] || die 'verified target image changed before retained-data test'
export RELEASE_HARNESS_IMAGE="$from_image"
docker compose -p "$project" -f "$compose_file" up -d postgres
docker compose -p "$project" -f "$compose_file" run --rm message-server-init
docker compose -p "$project" -f "$compose_file" up -d message-server
port="$(docker compose -p "$project" -f "$compose_file" port message-server 8008 | awk -F: 'END {print $NF}')"
[[ "$port" =~ ^[0-9]+$ ]] || die 'source HTTP port was not assigned'
base="http://127.0.0.1:$port"
source_wait_args=(wait --base "$base" --version "$from_version")
if [[ "$source_mode" == offline_import && "$from_version" == v0.15.2 ]]; then
  source_wait_args+=(--allow-status-only)
fi
python3 "$probe" "${source_wait_args[@]}"
docker compose -p "$project" -f "$compose_file" exec -T message-server cat /var/dirextalk-message-server/p2p/bootstrap.json >"$bootstrap_file"
chmod 600 "$bootstrap_file"
python3 "$probe" seed --base "$base" --bootstrap "$bootstrap_file" --state "$state_file"

export RELEASE_HARNESS_IMAGE="$target_image"
docker compose -p "$project" -f "$compose_file" up -d --no-deps --force-recreate message-server
port="$(docker compose -p "$project" -f "$compose_file" port message-server 8008 | awk -F: 'END {print $NF}')"
[[ "$port" =~ ^[0-9]+$ ]] || die 'target HTTP port was not assigned'
base="http://127.0.0.1:$port"
python3 "$probe" wait --base "$base" --version "$target_version"
python3 "$probe" verify --base "$base" --state "$state_file" --version "$target_version"
python3 "$attestation_tool" create \
  --attestation "$attestation" \
  --from-version "$from_version" \
  --source-identity "$source_identity" \
  --source-mode "$source_mode" \
  --release-version "$target_version" \
  --target-commit "$target_commit" \
  --target-image "$target_image" \
  --target-image-id "$target_image_id" \
  --release-config "$release_config" \
  --runner "$0"
printf 'retained upgrade passed: %s -> %s using %s\n' "$from_version" "$target_version" "${from_image##*@}"
