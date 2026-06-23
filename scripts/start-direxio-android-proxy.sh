#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROXY_PY="${ROOT_DIR}/scripts/direxio_domain_proxy.py"
CERT_DIR="${ROOT_DIR}/.local-certs/direxio"
RUN_DIR="${RUN_DIR:-/tmp/direxio-proxy}"
ADB="${ADB:-}"

HTTP_PORT="${HTTP_PORT:-9444}"
HTTPS_PORT="${HTTPS_PORT:-9443}"
DOMAINS=("a.ai" "b.ai" "c.ai")

mkdir -p "${CERT_DIR}" "${RUN_DIR}" "${HOME}/.local/bin"

log() {
  printf '[direxio-proxy] %s\n' "$*"
}

ensure_adb() {
  if [[ -n "${ADB}" && -x "${ADB}" ]]; then
    return
  fi

  local win_adb="/mnt/c/Users/84960/AppData/Local/Android/Sdk/platform-tools/adb.exe"
  if [[ -x "${win_adb}" ]] && timeout 5 "${win_adb}" devices >/dev/null 2>&1; then
    ADB="${win_adb}"
    return
  fi

  if command -v adb >/dev/null 2>&1; then
    ADB="$(command -v adb)"
    return
  fi

  local sdk_dir="${HOME}/.local/android-sdk"
  local adb_bin="${sdk_dir}/platform-tools/adb"
  if [[ ! -x "${adb_bin}" ]]; then
    log "adb not found; downloading Android platform-tools into ${sdk_dir}"
    mkdir -p "${sdk_dir}"
    (
      cd "${sdk_dir}"
      curl -L --fail -o platform-tools-latest-linux.zip \
        https://dl.google.com/android/repository/platform-tools-latest-linux.zip
      python3 - <<'PY'
import zipfile
with zipfile.ZipFile("platform-tools-latest-linux.zip") as z:
    z.extractall(".")
PY
      chmod -R u+rx platform-tools
    )
  fi

  ln -sf "${adb_bin}" "${HOME}/.local/bin/adb"
  ADB="${adb_bin}"
}

generate_certs() {
  cat > "${CERT_DIR}/server.ext" <<'EOF'
subjectAltName = DNS:a.ai,DNS:b.ai,DNS:c.ai
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
basicConstraints = CA:FALSE
EOF

  if [[ ! -f "${CERT_DIR}/ca.key" || ! -f "${CERT_DIR}/ca.pem" ]]; then
    log "generating local Direxio CA"
    openssl genrsa -out "${CERT_DIR}/ca.key" 4096
    openssl req -x509 -new -nodes -key "${CERT_DIR}/ca.key" -sha256 -days 3650 \
      -out "${CERT_DIR}/ca.pem" \
      -subj "/C=CN/O=Direxio Local Dev/CN=Direxio Local Dev CA"
  fi

  log "generating ${DOMAINS[*]} server certificate"
  openssl genrsa -out "${CERT_DIR}/server.key" 2048 >/dev/null 2>&1
  openssl req -new -key "${CERT_DIR}/server.key" -out "${CERT_DIR}/server.csr" \
    -subj "/C=CN/O=Direxio Local Dev/CN=a.ai" >/dev/null 2>&1
  openssl x509 -req -in "${CERT_DIR}/server.csr" \
    -CA "${CERT_DIR}/ca.pem" -CAkey "${CERT_DIR}/ca.key" -CAcreateserial \
    -out "${CERT_DIR}/server.pem" -days 825 -sha256 -extfile "${CERT_DIR}/server.ext" \
    >/dev/null 2>&1

  # Some Android images validate immediately with small clock skew; avoid
  # hitting the freshly generated certificate's notBefore boundary.
  sleep 3
}

stop_pidfile() {
  local pidfile="$1"
  if [[ -f "${pidfile}" ]]; then
    local pid
    pid="$(cat "${pidfile}")"
    if [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1; then
      kill "${pid}" >/dev/null 2>&1 || true
      sleep 0.2
    fi
    rm -f "${pidfile}"
  fi
}

start_proxy() {
  if [[ ! -f "${PROXY_PY}" ]]; then
    log "missing proxy script: ${PROXY_PY}"
    exit 1
  fi

  stop_pidfile "${RUN_DIR}/http.pid"
  stop_pidfile "${RUN_DIR}/https.pid"

  log "starting HTTP proxy on 127.0.0.1:${HTTP_PORT}"
  setsid -f python3 "${PROXY_PY}" "${HTTP_PORT}" > "${RUN_DIR}/http.log" 2>&1
  pgrep -n -f "python3 ${PROXY_PY} ${HTTP_PORT}" > "${RUN_DIR}/http.pid"

  log "starting HTTPS proxy on 127.0.0.1:${HTTPS_PORT}"
  setsid -f python3 "${PROXY_PY}" "${HTTPS_PORT}" \
    --cert "${CERT_DIR}/server.pem" \
    --key "${CERT_DIR}/server.key" \
    > "${RUN_DIR}/https.log" 2>&1
  pgrep -n -f "python3 ${PROXY_PY} ${HTTPS_PORT}" > "${RUN_DIR}/https.pid"

  local i
  for ((i = 1; i <= 20; i++)); do
    if curl --noproxy '*' -fsS --max-time 2 \
      --cacert "${CERT_DIR}/ca.pem" \
      --resolve "a.ai:${HTTPS_PORT}:127.0.0.1" \
      "https://a.ai:${HTTPS_PORT}/_p2p/health" >/dev/null 2>&1; then
      return
    fi
    sleep 0.5
  done

  log "HTTPS proxy did not become ready; see ${RUN_DIR}/https.log"
  exit 1
}

device_list() {
  "${ADB}" devices | tr -d '\r' | awk 'NR > 1 && $2 == "device" { print $1 }'
}

wait_for_device() {
  local device="$1"
  local attempts="${2:-20}"
  local i

  for ((i = 1; i <= attempts; i++)); do
    if "${ADB}" devices | tr -d '\r' | awk -v target="${device}" '$1 == target && $2 == "device" { found = 1 } END { exit found ? 0 : 1 }'; then
      return 0
    fi
    "${ADB}" connect "${device}" >/dev/null 2>&1 || true
    sleep 1
  done

  log "${device}: device did not reconnect after adb root"
  return 1
}

configure_device_trust() {
  local device="$1"
  local hash cert_file
  hash="$(openssl x509 -inform PEM -subject_hash_old -in "${CERT_DIR}/ca.pem" -noout)"
  cert_file="/tmp/${hash}.0"
  cp "${CERT_DIR}/ca.pem" "${cert_file}"

  "${ADB}" -s "${device}" root >/dev/null 2>&1 || true
  wait_for_device "${device}"
  "${ADB}" -s "${device}" push "${cert_file}" "/data/local/tmp/${hash}.0" >/dev/null

  local configure_system
  configure_system="$(cat <<EOF
mount -o rw,remount /system 2>/dev/null || mount -o rw,remount /dev/block/sda6 /system 2>/dev/null || true
cp -f /system/etc/hosts /system/etc/hosts.direxio.bak 2>/dev/null || true
printf '127.0.0.1       localhost\n::1             ip6-localhost\n127.0.0.1       a.ai b.ai c.ai\n' > /system/etc/hosts
chmod 0644 /system/etc/hosts
mkdir -p /system/etc/security/cacerts
cp -f /data/local/tmp/${hash}.0 /system/etc/security/cacerts/${hash}.0
chmod 0644 /system/etc/security/cacerts/${hash}.0
EOF
)"

  if "${ADB}" -s "${device}" shell "${configure_system}" >/dev/null 2>&1; then
    log "${device}: installed hosts and system CA"
    return
  fi

  wait_for_device "${device}"
  log "${device}: /system is not writable; using temporary bind mounts"
  local configure_bind
  configure_bind="$(cat <<EOF
set -e
printf '127.0.0.1       localhost\n::1             ip6-localhost\n127.0.0.1       a.ai b.ai c.ai\n' > /data/local/tmp/direxio-hosts
chmod 0644 /data/local/tmp/direxio-hosts
rm -rf /data/local/tmp/direxio-cacerts
cp -a /system/etc/security/cacerts /data/local/tmp/direxio-cacerts
cp -f /data/local/tmp/${hash}.0 /data/local/tmp/direxio-cacerts/${hash}.0
chmod 0644 /data/local/tmp/direxio-cacerts/${hash}.0
mount -o bind /data/local/tmp/direxio-hosts /system/etc/hosts
mount -o bind /data/local/tmp/direxio-cacerts /system/etc/security/cacerts
EOF
)"
  "${ADB}" -s "${device}" shell "${configure_bind}" >/dev/null
}

configure_device_reverse() {
  local device="$1"
  "${ADB}" -s "${device}" reverse tcp:80 "tcp:${HTTP_PORT}" >/dev/null
  "${ADB}" -s "${device}" reverse tcp:443 "tcp:${HTTPS_PORT}" >/dev/null
  "${ADB}" -s "${device}" reverse tcp:18008 "tcp:${HTTP_PORT}" >/dev/null
  "${ADB}" -s "${device}" reverse tcp:28008 "tcp:${HTTP_PORT}" >/dev/null
  "${ADB}" -s "${device}" reverse tcp:38008 "tcp:${HTTP_PORT}" >/dev/null
  "${ADB}" -s "${device}" shell settings delete global http_proxy >/dev/null 2>&1 || true
  "${ADB}" -s "${device}" shell settings delete global global_http_proxy_host >/dev/null 2>&1 || true
  "${ADB}" -s "${device}" shell settings delete global global_http_proxy_port >/dev/null 2>&1 || true
  "${ADB}" -s "${device}" shell settings delete global global_http_proxy_exclusion_list >/dev/null 2>&1 || true
}

verify_device() {
  local device="$1"
  "${ADB}" -s "${device}" shell '
set -e
curl -fsS --max-time 8 https://a.ai/_p2p/health >/dev/null
curl -fsS --max-time 8 https://b.ai/_p2p/health >/dev/null
curl -fsS --max-time 8 https://c.ai/_p2p/health >/dev/null
curl -fsS --max-time 8 http://a.ai:18008/_matrix/client/versions >/dev/null
curl -fsS --max-time 8 http://b.ai:28008/_matrix/client/versions >/dev/null
curl -fsS --max-time 8 http://c.ai:38008/_matrix/client/versions >/dev/null
'
}

main() {
  ensure_adb
  generate_certs
  start_proxy

  log "using adb: ${ADB}"
  "${ADB}" devices

  local devices=()
  while IFS= read -r device; do
    devices+=("${device}")
  done < <(device_list)

  if [[ "${#devices[@]}" -eq 0 ]]; then
    log "no adb devices connected; proxy is running, but no device was configured"
    exit 0
  fi

  local device
  for device in "${devices[@]}"; do
    log "${device}: configuring Android trust and reverse ports"
    configure_device_trust "${device}"
    configure_device_reverse "${device}"
    verify_device "${device}"
    log "${device}: verified a.ai/b.ai/c.ai over HTTPS and Matrix port URLs"
  done

  log "ready"
  log "A: https://a.ai or http://a.ai:18008"
  log "B: https://b.ai or http://b.ai:28008"
  log "C: https://c.ai or http://c.ai:38008"
  log "logs: ${RUN_DIR}/http.log and ${RUN_DIR}/https.log"
}

main "$@"
