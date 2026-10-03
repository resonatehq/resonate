#!/bin/bash
# A Resonate node on Kafka: binary from GCS, RocksDB on local NVMe SSD, systemd.
set -euxo pipefail
md() { curl -sf -H 'Metadata-Flavor: Google' "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1"; }
IP=$(curl -sf -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip)
if ! mountpoint -q /mnt/ssd; then
  mkfs.ext4 -F /dev/disk/by-id/google-local-nvme-ssd-0
  mkdir -p /mnt/ssd && mount -o discard,noatime /dev/disk/by-id/google-local-nvme-ssd-0 /mnt/ssd
fi
# Tight clocks: timesyncd leaves nodes tens of ms apart, and the Postgres
# backend stamps rows with the serving node's clock.
apt-get install -y -qq chrony >/dev/null
echo "server metadata.google.internal prefer iburst" > /etc/chrony/sources.d/gce.sources
systemctl restart chrony
mkdir -p /opt/resonate
gcloud storage cp "$(md bucket)/bin/server" "$(md bucket)/bin/resonate" /opt/resonate/ && chmod +x /opt/resonate/server /opt/resonate/resonate
cat > /etc/resonate.env <<ENV
RESONATE_SERVERS__ACTIVE=server_kafka
RESONATE_SERVERS__SERVER_KAFKA__BROKERS=$(md brokers)
RESONATE_SERVERS__SERVER_KAFKA__PARTITIONS=$(md partitions)
RESONATE_SERVERS__SERVER_KAFKA__REPLICATION_FACTOR=3
RESONATE_SERVERS__SERVER_KAFKA__TOPIC_PREFIX=$(md topic-prefix)
RESONATE_SERVERS__SERVER_KAFKA__NODE_ID=$(hostname)
RESONATE_SERVERS__SERVER_KAFKA__PEER_BIND=0.0.0.0:8002
RESONATE_SERVERS__SERVER_KAFKA__PEER_URL=http://$IP:8002
RESONATE_SERVERS__SERVER_KAFKA__DATA_DIR=/mnt/ssd/rocks
RESONATE_SERVERS__SERVER_KAFKA__BLOCK_CACHE_MB=2048
RESONATE_GATEWAYS__GATEWAY_HTTP__BIND=0.0.0.0:8001
RESONATE_GATEWAYS__GATEWAY_METRICS__BIND=0.0.0.0:9090
RESONATE_LEVEL=warn
ENV
cat > /etc/systemd/system/resonate.service <<UNIT
[Unit]
Description=Resonate on Kafka
After=network-online.target
[Service]
EnvironmentFile=/etc/resonate.env
ExecStart=/opt/resonate/server
Restart=always
RestartSec=1
LimitNOFILE=1048576
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable resonate
# Started explicitly by the run scripts, once the brokers are up.
