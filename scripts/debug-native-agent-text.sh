#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-https://www.wenson.art/_p2p}"
CREDENTIALS_FILE="${CREDENTIALS_FILE:-/var/dirextalk-message-server/p2p/bootstrap.json}"
PROMPT="${PROMPT:-查看我昨天建的群，列出最新群聊信息}"
CONVERSATION_ID="${CONVERSATION_ID:-codex-debug-text-native-agent}"
ROOM_ID="${ROOM_ID:-}"
ROOM_TYPE="${ROOM_TYPE:-native_agent}"
MODEL_PROFILE_JSON="${MODEL_PROFILE_JSON:-}"
API_KEY="${API_KEY:-}"
export PROMPT CONVERSATION_ID ROOM_ID ROOM_TYPE MODEL_PROFILE_JSON API_KEY

if [[ -z "${ACCESS_TOKEN:-}" ]]; then
  if [[ ! -f "$CREDENTIALS_FILE" ]]; then
    echo "missing ACCESS_TOKEN and credentials file: $CREDENTIALS_FILE" >&2
    exit 1
  fi
  ACCESS_TOKEN="$(
    python3 - "$CREDENTIALS_FILE" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    data = json.load(f)
print(data.get("access_token", ""))
PY
  )"
fi

if [[ -z "$ACCESS_TOKEN" ]]; then
  echo "missing access token" >&2
  exit 1
fi

payload="$(
  python3 - <<'PY'
import json
import os

params = {
    "prompt": os.environ.get("PROMPT", "").strip(),
    "conversation_id": os.environ.get("CONVERSATION_ID", "").strip(),
    "room_id": os.environ.get("ROOM_ID", "").strip(),
    "room_type": os.environ.get("ROOM_TYPE", "").strip() or "native_agent",
    "source": "native_agent",
}
profile_json = os.environ.get("MODEL_PROFILE_JSON", "").strip()
if profile_json:
    profile = json.loads(profile_json)
    params["model_profile"] = profile
    profile_id = str(profile.get("id", "")).strip()
    if profile_id:
        params["model_profile_id"] = profile_id
api_key = os.environ.get("API_KEY", "").strip()
if api_key:
    params["api_key"] = api_key
print(json.dumps({"action": "agent.chat", "params": params}, ensure_ascii=False))
PY
)"

tmp="$(mktemp)"
status="$(
  curl -sS -o "$tmp" -w '%{http_code}' \
    -X POST "${BASE_URL%/}/command" \
    -H "Authorization: Bearer $ACCESS_TOKEN" \
    -H 'Content-Type: application/json; charset=utf-8' \
    -H 'Accept: application/json' \
    --data "$payload"
)"

echo "HTTP_STATUS $status"
python3 - "$tmp" <<'PY'
import json
import sys

raw = open(sys.argv[1], "r", encoding="utf-8", errors="replace").read()
try:
    decoded = json.loads(raw)
except Exception:
    print(raw[:4000])
    raise SystemExit(0)

for key in ("ok", "native", "provider", "model", "model_ready", "error"):
    if key in decoded:
        print(f"{key}: {decoded[key]}")
text = decoded.get("text") or decoded.get("summary") or decoded.get("answer")
if text:
    print("text:")
    print(str(text)[:3000])
refs = decoded.get("references")
if refs is not None:
    print(f"references_count: {len(refs) if isinstance(refs, list) else 0}")
tools = decoded.get("tool_calls")
if tools is not None:
    print(f"tool_calls_count: {len(tools) if isinstance(tools, list) else 0}")
PY

rm -f "$tmp"
