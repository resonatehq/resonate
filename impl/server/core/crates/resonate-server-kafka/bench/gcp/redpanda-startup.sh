#!/bin/bash
# A Redpanda broker in docker, data on the attached pd-ssd. Production mode
# (fsync on), Raft replication; seeds are every broker's RPC address.
set -euxo pipefail
md() { curl -sf -H 'Metadata-Flavor: Google' "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1"; }
SEEDS=$(md seeds)
IP=$(curl -sf -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip)
if ! mountpoint -q /var/lib/redpanda; then
  mkfs.xfs -f /dev/disk/by-id/google-data 2>/dev/null || { apt-get update && apt-get install -y xfsprogs && mkfs.xfs -f /dev/disk/by-id/google-data; }
  mkdir -p /var/lib/redpanda && mount /dev/disk/by-id/google-data /var/lib/redpanda
  mkdir -p /var/lib/redpanda/data && chown -R 101:101 /var/lib/redpanda
fi
sysctl -w fs.aio-max-nr=1048576
command -v docker || (apt-get update && apt-get install -y docker.io)
docker rm -f redpanda || true
docker run -d --name redpanda --restart=always --network host \
  -v /var/lib/redpanda/data:/var/lib/redpanda/data \
  redpandadata/redpanda:latest redpanda start \
  --smp 4 --memory 12G --reserve-memory 1G \
  --kafka-addr internal://0.0.0.0:9092 --advertise-kafka-addr internal://$IP:9092 \
  --rpc-addr 0.0.0.0:33145 --advertise-rpc-addr $IP:33145 \
  --seeds "$SEEDS"
