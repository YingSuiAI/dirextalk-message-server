#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

release_init "$@"
release_preflight
rm -rf "$RELEASE_OUTPUT_DIR"
release_write_context
printf 'release prepare passed for %s at %s\n' "$RELEASE_VERSION" "$RELEASE_COMMIT"
