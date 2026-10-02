#!/bin/bash
set -euxo pipefail
md() { curl -sf -H 'Metadata-Flavor: Google' "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1"; }
mkdir -p /opt/resonate
gcloud storage cp "$(md bucket)/bin/load" /opt/resonate/load && chmod +x /opt/resonate/load
sysctl -w net.ipv4.ip_local_port_range="1024 65000"
sysctl -w net.ipv4.tcp_tw_reuse=1
ulimit -n 1048576 || true
