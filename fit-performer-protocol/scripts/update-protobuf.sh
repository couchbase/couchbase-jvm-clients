#!/bin/sh

# USAGE: Run this script from the `fit-performer-protocol` directory
# to fetch the latest proto files from the `couchbaselabs/fit-protocol` repo.
# Then manually commit any changes.
#
# Why not use git submodule or subtree?
# Existing infrastructure does not expect a submodule.
# `git subtree` has gotchas we don't want to deal with.

set -e

[ "$(basename "$PWD")" = "fit-performer-protocol" ] || { echo "ERROR: Working directory must be fit-performer-protocol" >&2; exit 1; }

mkdir -p target
rm -rf target/unzipped

curl --location --fail https://github.com/couchbaselabs/fit-protocol/archive/refs/heads/main.zip -o target/fit-protocol.zip
unzip target/fit-protocol.zip -d target/unzipped

SRC_DIR=target/unzipped/fit-protocol-main
[ -d "$SRC_DIR" ] || { echo "ERROR: Missing expected directory: $SRC_DIR" >&2; exit 1; }

DEST_DIR=fit-protocol

rm -rf "$DEST_DIR"
mv "$SRC_DIR" "$DEST_DIR"
git add --all "$DEST_DIR"

echo
echo "Protobuf update complete! Please manually commit any modified files."
echo
