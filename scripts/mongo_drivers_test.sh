#!/usr/bin/env bash
# Run the official MongoDB Node.js and Go drivers against the NonoDB wire
# listener. Dependencies are installed in a temporary directory so the
# repository is not modified.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
URL="${NONODB_MONGO_URL:-mongodb://127.0.0.1:37017/?directConnection=true}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

NODE_DIR="$TMP/node"
mkdir -p "$NODE_DIR"
npm install --prefix "$NODE_DIR" --no-save --silent mongodb@6

NODE_PATH="$NODE_DIR/node_modules" NONODB_MONGO_URL="$URL" \
    node "$ROOT/scripts/mongo_driver_node_test.js"

GO_DIR="$TMP/go"
mkdir -p "$GO_DIR"
cp "$ROOT/scripts/mongo_driver_go_test.go" "$GO_DIR/main.go"
(
    cd "$GO_DIR"
    go mod init nonodb-mongodb-driver-test >/dev/null
    go get go.mongodb.org/mongo-driver@v1.17.4 >/dev/null
    go mod tidy >/dev/null
    NONODB_MONGO_URL="$URL" go run .
)
