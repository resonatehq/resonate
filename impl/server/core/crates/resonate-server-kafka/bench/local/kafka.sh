#!/bin/bash
# A single Kafka broker (KRaft, no replication) for prof.sh, under $DIR.
# Idempotent: downloads Kafka once, formats its storage once, and starts the
# broker unless one already answers on 9092.
#
#   DIR=/tmp/kafka ./kafka.sh          # start, or confirm it is up
#   DIR=/tmp/kafka ./kafka.sh stop
set -euo pipefail

DIR=${DIR:?DIR must name a directory for Kafka and its data}
VERSION=${VERSION:-3.9.1}
HEAP=${HEAP:-2g}
home=$DIR/kafka_2.13-$VERSION
up() { (echo > /dev/tcp/127.0.0.1/9092) 2> /dev/null; }

if [ "${1:-}" = stop ]; then
  pkill -f 'kafka\.Kafka' || true
  exit 0
fi
up && { echo "kafka is up"; exit 0; }

mkdir -p "$DIR"
if [ ! -x "$home/bin/kafka-server-start.sh" ]; then
  curl -sS "https://archive.apache.org/dist/kafka/$VERSION/kafka_2.13-$VERSION.tgz" | tar xz -C "$DIR"
fi
props=$DIR/server.properties
if [ ! -f "$props" ]; then
  sed "s#^log.dirs=.*#log.dirs=$DIR/data#" "$home/config/kraft/server.properties" > "$props"
  printf 'auto.create.topics.enable=false\ngroup.initial.rebalance.delay.ms=0\n' >> "$props"
fi
if [ ! -f "$DIR/data/meta.properties" ]; then
  "$home/bin/kafka-storage.sh" format -t "$("$home/bin/kafka-storage.sh" random-uuid 2> /dev/null | tail -1)" \
    -c "$props" > /dev/null
fi
KAFKA_HEAP_OPTS="-Xms$HEAP -Xmx$HEAP" nohup "$home/bin/kafka-server-start.sh" "$props" > "$DIR/kafka.log" 2>&1 &
for _ in $(seq 1 120); do up && { echo "kafka is up"; exit 0; }; sleep 1; done
echo "kafka did not come up; see $DIR/kafka.log" >&2
exit 1
