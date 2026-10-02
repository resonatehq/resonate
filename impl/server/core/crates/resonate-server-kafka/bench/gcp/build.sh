#!/bin/bash
# Build examples/server and examples/load on the builder VM from the current
# commit, and publish them to the bucket.
set -euo pipefail
cd "$(dirname "$0")"; source env.sh
z=$(zone_of 1); root=$(git rev-parse --show-toplevel); rev=$(git rev-parse --short HEAD)
git -C "$root" archive --format=tar.gz -o /tmp/$PREFIX-src.tgz HEAD impl/server/core
$G compute scp /tmp/$PREFIX-src.tgz $PREFIX-builder:/tmp/src.tgz --zone $z
ssh_to $PREFIX-builder $z "set -e
  if ! command -v cargo >/dev/null && [ ! -x \$HOME/.cargo/bin/cargo ]; then
    sudo apt-get update -qq && sudo apt-get install -y -qq build-essential cmake clang libclang-dev pkg-config libssl-dev curl git >/dev/null
    curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal >/dev/null
  fi
  source \$HOME/.cargo/env
  rm -rf ~/src && mkdir -p ~/src && tar -xzf /tmp/src.tgz -C ~/src
  cd ~/src/impl/server/core
  CARGO_TARGET_DIR=~/target cargo build --release -p resonate-server-kafka --example server --example load 2>&1 | tail -3
  gcloud storage cp ~/target/release/examples/server ~/target/release/examples/load $BUCKET/bin/
  echo $rev | gcloud storage cp - $BUCKET/bin/REVISION"
echo "built $rev"
