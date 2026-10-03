#!/bin/bash
# Create the benchmark fleet. Idempotent per component:
#   ./up.sh bucket | builder | brokers | nodes N | loadgens N
set -euo pipefail
cd "$(dirname "$0")"; source env.sh
LABELS="--labels=$PREFIX=1"
COMMON="$IMAGE --scopes=cloud-platform $LABELS"
META="--metadata=bucket=$BUCKET"

exists() { $G compute instances describe "$1" --zone "$2" --format='value(name)' >/dev/null 2>&1; }

# create NAME ZONE TYPE ARGS..: the machine type, or the next in its family
# list when the zone is out of it (stockouts are common in single zones).
create() {
  local name=$1 zone=$2 type=$3; shift 3
  for t in $type ${FALLBACK_TYPES:-n2d-standard-4}; do
    [ "$t" = "$type" ] || echo "$name: $type unavailable in $zone, trying $t"
    $G compute instances create $name --zone $zone --machine-type $t "$@" && return 0
  done
  return 1
}

case "$1" in
bucket)
  gcloud storage buckets describe "$BUCKET" --project "$PROJECT" >/dev/null 2>&1 \
    || gcloud storage buckets create "$BUCKET" --project "$PROJECT" --location "$REGION" --uniform-bucket-level-access
  ;;
builder)
  z=$(zone_of 1)
  exists $PREFIX-builder $z || $G compute instances create $PREFIX-builder --zone $z \
    --machine-type $BUILDER_TYPE $COMMON $META --boot-disk-size=100GB --boot-disk-type=pd-ssd
  ;;
brokers)
  # The brokers' cluster id: kept if they already have one, since their data
  # directories are stamped with it and a broker refuses any other.
  cluster=$($G compute instances describe $PREFIX-b1 --zone $(zone_of 1) \
    --format='value(metadata.items.cluster-id)' 2>/dev/null)
  [ -n "$cluster" ] || cluster=$(python3 -c 'import uuid,base64;print(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("="))')
  # Create first without starting Kafka, to learn the IPs the voters need.
  for i in 1 2 3; do
    z=$(zone_of $i)
    exists $PREFIX-b$i $z || create $PREFIX-b$i $z $BROKER_TYPE $COMMON $META --boot-disk-size=20GB \
      --create-disk=name=$PREFIX-b$i-data,size=500GB,type=pd-ssd,device-name=data,auto-delete=yes &
  done; wait
  voters=""; brokers=""
  for i in 1 2 3; do
    ip=$($G compute instances describe $PREFIX-b$i --zone $(zone_of $i) --format='value(networkInterfaces[0].networkIP)')
    voters+="${voters:+,}$i@$ip:9093"; brokers+="${brokers:+,}$ip:9092"
  done
  for i in 1 2 3; do
    $G compute instances add-metadata $PREFIX-b$i --zone $(zone_of $i) \
      "--metadata=^;^broker-id=$i;voters=$voters;cluster-id=$cluster" \
      --metadata-from-file=startup-script=broker-startup.sh &
  done; wait
  for i in 1 2 3; do ssh_to $PREFIX-b$i $(zone_of $i) "sudo google_metadata_script_runner startup" >/dev/null 2>&1 & done; wait
  echo "$brokers" > .brokers
  echo "brokers: $brokers"
  ;;
nodes)
  n=$2; brokers=$(cat .brokers)
  for i in $(seq 1 $n); do
    z=$(zone_of $i)
    exists $PREFIX-n$i $z || create $PREFIX-n$i $z $NODE_TYPE $COMMON --boot-disk-size=20GB --local-ssd=interface=NVME \
      "--metadata=^;^bucket=$BUCKET;brokers=$brokers;partitions=$PARTITIONS;topic-prefix=${TOPIC_PREFIX:-resonate}" \
      --metadata-from-file=startup-script=node-startup.sh &
  done; wait
  ;;
loadgens)
  n=$2
  for i in $(seq 1 $n); do
    z=$(zone_of $i)
    exists $PREFIX-l$i $z || $G compute instances create $PREFIX-l$i --zone $z \
      --machine-type $LOADGEN_TYPE $COMMON $META --boot-disk-size=20GB \
      --metadata-from-file=startup-script=loadgen-startup.sh &
  done; wait
  ;;
controller)
  # Runs experiments inside GCP (controller.sh), so nothing depends on the
  # workstation staying up.
  z=$(zone_of 1)
  exists $PREFIX-ctl $z || $G compute instances create $PREFIX-ctl --zone $z \
    --machine-type e2-standard-2 $COMMON $META --boot-disk-size=20GB
  ;;
*) echo "usage: $0 bucket|builder|brokers|nodes N|loadgens N|controller"; exit 1 ;;
esac
