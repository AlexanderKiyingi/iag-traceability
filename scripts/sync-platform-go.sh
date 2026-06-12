#!/usr/bin/env bash
# Vendor shared/platform-go for standalone iag-traceability builds (Docker/CI).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="${ROOT}/third_party/platform-go"
SRC="${ROOT}/../../../shared/platform-go"
if [[ ! -d "$SRC" ]]; then
  echo "platform-go not found at $SRC — run from monorepo or clone IAG_multi_backend" >&2
  exit 1
fi
rm -rf "$DEST"
mkdir -p "$(dirname "$DEST")"
cp -R "$SRC" "$DEST"
# Do NOT rewrite go.mod's replace here: it stays pointed at the meta-repo path
# (../../../shared/platform-go) so in-monorepo `go build` works. The Dockerfile
# overrides the replace to /deps/platform-go at build time for standalone images.
echo "synced platform-go to $DEST"
