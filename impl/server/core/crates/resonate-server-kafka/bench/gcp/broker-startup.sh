#!/bin/bash
# A Kafka 3.9 broker+controller (KRaft) in docker, data on the attached pd-ssd.
set -euxo pipefail
md() { curl -sf -H 'Metadata-Flavor: Google' "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1"; }
ID=$(md broker-id); VOTERS=$(md voters); CLUSTER=$(md cluster-id)
IP=$(curl -sf -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip)
if ! mountpoint -q /var/lib/kafka; then
  mkfs.ext4 -F /dev/disk/by-id/google-data || true
  mkdir -p /var/lib/kafka && mount /dev/disk/by-id/google-data /var/lib/kafka
  mkdir -p /var/lib/kafka/data && chown -R 1000:1000 /var/lib/kafka
fi
command -v docker || (apt-get update && apt-get install -y docker.io)
docker rm -f kafka || true
docker run -d --name kafka --restart=always --network host -v /var/lib/kafka/data:/var/lib/kafka/data \
  -e KAFKA_NODE_ID=$ID -e CLUSTER_ID=$CLUSTER -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://$IP:9092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_CONTROLLER_QUORUM_VOTERS=$VOTERS \
  -e KAFKA_LOG_DIRS=/var/lib/kafka/data \
  -e KAFKA_DEFAULT_REPLICATION_FACTOR=3 -e KAFKA_MIN_INSYNC_REPLICAS=2 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e KAFKA_NUM_NETWORK_THREADS=6 -e KAFKA_NUM_IO_THREADS=8 \
  -e KAFKA_HEAP_OPTS="-Xms4g -Xmx4g" -e KAFKA_JMX_PORT=9999 -e KAFKA_JMX_HOSTNAME=127.0.0.1 \
  apache/kafka:3.9.1
