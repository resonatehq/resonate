#!/bin/bash
# Build examples/server and examples/load on the builder VM from the current
# commit, and publish them to the bucket.
set -euo pipefail
cd "$(dirname "$0")"; source env.sh
z=$(zone_of 1); root=$(git rev-parse --show-toplevel 2>/dev/null); rev=$(git rev-parse --short HEAD 2>/dev/null)
# SRC_TGZ: a source tarball to build instead of HEAD (on the controller, which has no git).
if [ -n "${SRC_TGZ:-}" ]; then cp "$SRC_TGZ" /tmp/$PREFIX-src.tgz; rev=$(cat "${SRC_TGZ%.tgz}.rev" 2>/dev/null || echo unknown)
else git -C "$root" archive --format=tar.gz -o /tmp/$PREFIX-src.tgz HEAD impl/server/core; fi
$G compute scp /tmp/$PREFIX-src.tgz $PREFIX-builder:/tmp/src.tgz --zone $z
ssh_to $PREFIX-builder $z "set -e
  if ! command -v cargo >/dev/null && [ ! -x \$HOME/.cargo/bin/cargo ]; then
    sudo apt-get update -qq && sudo apt-get install -y -qq build-essential cmake clang libclang-dev pkg-config libssl-dev curl git >/dev/null
    curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal >/dev/null
  fi
  source \$HOME/.cargo/env
  rm -rf ~/src && mkdir -p ~/src && tar -xzf /tmp/src.tgz -C ~/src
  cd ~/src/impl/server/core
  CARGO_TARGET_DIR=~/target cargo build --release -p resonate-server-kafka --example server --example load --example produce_latency 2>&1 | tail -3
  gcloud storage cp ~/target/release/examples/server ~/target/release/examples/load ~/target/release/examples/produce_latency $BUCKET/bin/
  echo $rev | gcloud storage cp - $BUCKET/bin/REVISION"
echo "built $rev"
