# Shared settings for the GCP benchmark scripts. Override any of them in the
# environment before sourcing.
: "${PROJECT:=resonate-cloud}"
: "${REGION:=us-central1}"
: "${ZONES:=us-central1-a us-central1-b us-central1-f}"
: "${PREFIX:=rbench}"
: "${BUCKET:=gs://${PROJECT}-${PREFIX}}"
: "${BROKER_TYPE:=n2-standard-4}"
: "${NODE_TYPE:=n2-standard-4}"
: "${LOADGEN_TYPE:=n2-standard-8}"
: "${BUILDER_TYPE:=n2-standard-16}"
: "${PARTITIONS:=128}"
: "${IMAGE:=--image-family=debian-12 --image-project=debian-cloud}"
read -r -a ZONE <<< "$ZONES"
G="gcloud --project $PROJECT --quiet"
# zone for the i-th member (1-based), round-robin over the zones
zone_of() { echo "${ZONE[$(( ($1 - 1) % ${#ZONE[@]} ))]}"; }
# ssh, retried when the connection itself fails (exit 255), as parallel
# first-time connections sometimes do.
ssh_to() {
  local name=$1 zone=$2 rc; shift 2
  for _ in 1 2 3 4 5; do
    $G compute ssh "$name" --zone "$zone" --command "$*" -- -o LogLevel=ERROR -o ConnectTimeout=10 && return 0
    rc=$?; [ $rc -ne 255 ] && return $rc; sleep 2
  done
  return $rc
}
