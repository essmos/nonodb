#!/usr/bin/env bash
set -euo pipefail
# Pinned openCypher TCK revision. Keep this in sync with docs/cypher.md.
COMMIT="${OPENCYPHER_TCK_COMMIT:-677cbafabb8c3c5eed458fd3b1ec0daec8d67d23}"
DEST="${1:-tck/opencypher}"
if [ -d "$DEST/.git" ]; then
  git -C "$DEST" fetch --depth 1 origin "$COMMIT"
else
  rm -rf "$DEST"
  mkdir -p "$(dirname "$DEST")"
  git clone --no-checkout --filter=blob:none https://github.com/opencypher/openCypher.git "$DEST"
fi
git -C "$DEST" checkout --detach "$COMMIT"
echo "openCypher TCK ready at $DEST ($COMMIT)"
