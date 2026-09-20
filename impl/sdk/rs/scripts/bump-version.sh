#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "usage: $0 <new-version>" >&2
  echo "example: $0 0.5.0" >&2
  exit 1
fi

new_version="$1"

if ! [[ "$new_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.-]+)?$ ]]; then
  echo "error: '$new_version' is not a valid semver version" >&2
  exit 1
fi

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cargo_toml="$repo_root/Cargo.toml"

current_version=$(awk '/^\[workspace\.package\]/{f=1; next} f && /^version = "/{sub(/^version = "/,""); sub(/".*/,""); print; exit}' "$cargo_toml")

if [ -z "$current_version" ]; then
  echo "error: could not find workspace.package version in $cargo_toml" >&2
  exit 1
fi

if [ "$current_version" = "$new_version" ]; then
  echo "version is already $new_version, nothing to do"
  exit 0
fi

echo "bumping workspace version: $current_version -> $new_version"

# Match the version line only inside [workspace.package]; anchored to start-of-line
# and to the exact current value, so other crates' literal versions stay put.
sed -i.bak -E "s/^version = \"${current_version}\"\$/version = \"${new_version}\"/" "$cargo_toml"
rm "$cargo_toml.bak"

(cd "$repo_root" && cargo update --workspace)

echo
echo "done. review the diff and commit Cargo.toml + Cargo.lock:"
echo "  git diff Cargo.toml Cargo.lock"
