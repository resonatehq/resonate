#!/bin/bash
# Run on the builder: the Postgres engine's correctness suites over a source
# tree, against a local Postgres — the differential and port suites, and the
# porcupine linearizability check — once per LANES value (group commit lanes;
# 0 is off). Prints one PASS/FAIL line per run; logs land in ~/pgtest/.
#
#   ./pgtest.sh SRC_DIR [LANES..]      DIFF=0 skips the differential and port suites
src=$1; shift; lanes=("${@:-0 1 4}")
log=~/pgtest; mkdir -p $log
if ! command -v pg_ctlcluster >/dev/null; then
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq postgresql >/dev/null 2>&1
fi
sudo -u postgres psql -qc "alter user postgres password 'resonate'" >/dev/null
sudo -u postgres psql -qc "alter system set max_connections = 400" >/dev/null && sudo systemctl restart postgresql
if [ ! -x /usr/local/go/bin/go ]; then
  curl -sSfL https://go.dev/dl/go1.23.4.linux-amd64.tar.gz | sudo tar -xz -C /usr/local
fi
[ -d ~/spec ] || git clone -q --depth 1 https://github.com/resonatehq/resonate-specification ~/spec
export PATH=/usr/local/go/bin:$PATH GOTOOLCHAIN=auto
source ~/.cargo/env
export XTASK_POSTGRES_ADMIN_URL=postgres://postgres:resonate@localhost:5432/postgres XTASK_SPEC_DIR=~/spec/valid/porc
cd $src/impl/server/core
for n in ${lanes[*]}; do
  if [ "$n" = 0 ]; then extra=(); else extra=(TEST_POSTGRES_BATCH_LANES=$n RESONATE_SERVERS__SERVER_POSTGRES__BATCH_LANES=$n); fi
  [ "${DIFF:-1}" = 1 ] && if env "${extra[@]}" CARGO_TARGET_DIR=~/target-test timeout 3000 cargo xtask differential --backend postgres > $log/differential-l$n.log 2>&1
  then echo "PASS differential+port, lanes $n"; else echo "FAIL differential+port, lanes $n (rc $?): $(grep -E 'panicked|FAILED|failed|error' $log/differential-l$n.log | head -3 | tr '\n' ' ' | cut -c1-300)"; fi
  # The source tree's own target directory: porcupine starts target/release/resonate.
  if env "${extra[@]}" timeout 1800 cargo xtask porcupine --backend postgres > $log/porcupine-l$n.log 2>&1
  then echo "PASS porcupine, lanes $n"; grep -vE '^\s*(Compiling|Finished|Running|Blocking|Downloaded|Downloading)' $log/porcupine-l$n.log | tail -12 | sed 's/^/    /' 
  else echo "FAIL porcupine, lanes $n (rc $?): $(tail -5 $log/porcupine-l$n.log | tr '\n' ' ' | cut -c1-300)"; fi
done
