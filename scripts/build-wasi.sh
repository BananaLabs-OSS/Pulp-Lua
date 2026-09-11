#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 OUTPUT.wasm" >&2
  exit 2
fi

root=$(cd "$(dirname "$0")/.." && pwd)
case "$1" in
  /*) output=$1 ;;
  *) output=$(pwd)/$1 ;;
esac

stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
cp -R "$root"/. "$stage"/
rm -rf "$stage/vendor" "$stage/.git"
(cd "$root" && go mod vendor -o "$stage/vendor")
(cd "$stage/pulp-cell" && env GOOS=wasip1 GOARCH=wasm go build \
  -mod=vendor \
  -buildvcs=false \
  -trimpath \
  -buildmode=c-shared \
  -ldflags=-buildid= \
  -o "$output" \
  .)
