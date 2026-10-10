#!/usr/bin/env bash
# Copies workflows-stack's protocol vectors verbatim into tests/vectors and records the commit.
#   scripts/sync-vectors.sh ~/code/workflows-stack [<commit>]
set -euo pipefail
src=${1:?usage: scripts/sync-vectors.sh <workflows-stack checkout> [<commit>]}
rev=$(git -C "$src" rev-parse "${2:-HEAD}")
mkdir -p tests/vectors
rm -f tests/vectors/*.json
for f in $(git -C "$src" ls-tree --name-only "$rev" protocol/vectors/ | grep '\.json$'); do
  git -C "$src" show "$rev:$f" > "tests/vectors/$(basename "$f")"
done
echo "ezgamehost/workflows-stack protocol/vectors at $rev (verbatim; change them there)" > tests/vectors/SOURCE
echo "$rev" > .workflows-stack-rev
