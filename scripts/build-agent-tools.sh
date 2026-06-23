#!/usr/bin/env sh
set -eu

output_dir="${1:-dist/agent-tools}"
mkdir -p "$output_dir"

build_one() {
  goos="$1"
  goarch="$2"
  ext="$3"
  GOOS="$goos" GOARCH="$goarch" go build -o "$output_dir/direxio-cli-$goos-$goarch$ext" ./cmd/direxio-cli
}

build_one windows amd64 .exe
build_one windows arm64 .exe
build_one linux amd64 ""
build_one linux arm64 ""
build_one darwin amd64 ""
build_one darwin arm64 ""
