#!/bin/bash
# Drive experiments from a workstation. Every command goes over ssh.
#
#   ./run.sh start N             start resonate on nodes 1..N, wiping local copies; TOPIC picks
#                                the topic prefix, PARTITIONS the partition count (new topics only),
#                                PIPELINE the rounds in flight per partition,
#                                the topic prefix, LIBRDKAFKA adds client properties (toml lines)
#   ./run.sh stop                stop resonate on every node
#   ./run.sh load NAME K ARGS..  run the load generator on loadgens 1..K at once,
#                                split --rate evenly, against nodes 1..$NODES;
#                                results land in results/NAME/; ROUTE=owner routes
#                                each request to its partition's owner (needs TOPIC)
#   ./run.sh kill-node I | restart-node I | rolling N | kill-broker I | start-broker I
#   ./run.sh isolate-zone Z | heal-zone Z      drop all traffic of zone #Z's VMs (except ssh)
#   ./run.sh cut-node I | heal-node I          cut node I off from the brokers
#   ./run.sh metrics I                         node I's /metrics (kafka series)
set -euo pipefail
cd "$(dirname "$0")"; source env.sh
: "${NODES:=3}"

ip_of() { $G compute instances describe "$1" --zone "$2" --format='value(networkInterfaces[0].networkIP)'; }
node() { ssh_to $PREFIX-n$1 $(zone_of $1) "$2"; }

ready() {
  for _ in $(seq 1 120); do node $1 "curl -sf localhost:8001/ready >/dev/null" 2>/dev/null && return 0; sleep 1; done
  echo "node $1 not ready" >&2; return 1
}

cmd=$1; shift
case "$cmd" in
start)
  n=$1 script=/tmp/$PREFIX-node-start-$$.sh
  # The node's configuration, as a script each node runs: Kafka (topic,
  # partitions, pipeline, librdkafka extras) or Postgres (BACKEND=postgres,
  # PG_URL, PG_POOL) — then a fresh local copy and a start.
  cat > $script <<EOS
#!/bin/bash
set -e
systemctl stop resonate
# The binaries as last built, so a node never runs a stale one.
gcloud storage cp -q $BUCKET/bin/server $BUCKET/bin/resonate /opt/resonate/ && chmod +x /opt/resonate/server /opt/resonate/resonate
sed -i '/TOPIC_PREFIX/d; /__PARTITIONS=/d; /__PIPELINE_DEPTH=/d' /etc/resonate.env
printf 'RESONATE_SERVERS__SERVER_KAFKA__TOPIC_PREFIX=%s\nRESONATE_SERVERS__SERVER_KAFKA__PARTITIONS=%s\nRESONATE_SERVERS__SERVER_KAFKA__PIPELINE_DEPTH=%s\n' \
  '${TOPIC:-resonate}' '$PARTITIONS' '${PIPELINE:-1}' >> /etc/resonate.env
mkdir -p /etc/systemd/system/resonate.service.d
printf '[Service]\nWorkingDirectory=/opt/resonate\n' > /etc/systemd/system/resonate.service.d/cwd.conf
if [ '${BACKEND:-kafka}' = postgres ]; then
  : > /opt/resonate/resonate.toml
  printf 'RESONATE_SERVERS__ACTIVE=server_postgres\nRESONATE_SERVERS__SERVER_POSTGRES__URL=%s\nRESONATE_SERVERS__SERVER_POSTGRES__POOL_SIZE=%s\nRESONATE_GATEWAYS__GATEWAY_HTTP__BIND=0.0.0.0:8001\nRESONATE_GATEWAYS__GATEWAY_METRICS__BIND=0.0.0.0:9090\nRESONATE_LEVEL=warn\n' \
    '${PG_URL:-}' '${PG_POOL:-32}' > /etc/resonate-pg.env
  printf '[Service]\nEnvironmentFile=\nEnvironmentFile=/etc/resonate-pg.env\nExecStart=\nExecStart=/opt/resonate/resonate serve\n' \
    > /etc/systemd/system/resonate.service.d/backend.conf
else
  printf '[servers.server_kafka.librdkafka]\n%b\n' '${LIBRDKAFKA:-}' > /opt/resonate/resonate.toml
  rm -f /etc/systemd/system/resonate.service.d/backend.conf
fi
systemctl daemon-reload
rm -rf /mnt/ssd/rocks
systemctl start resonate
EOS
  for i in $(seq 1 $n); do
    ( $G compute scp ${SSH_FLAGS:-} $script $PREFIX-n$i:/tmp/node-start.sh --zone $(zone_of $i) >/dev/null 2>&1
      node $i "sudo bash /tmp/node-start.sh" ) &
  done; wait
  for i in $(seq 1 $n); do ready $i & done; wait
  echo "started $n node(s): ${BACKEND:-kafka}, topic prefix ${TOPIC:-resonate}"
  ;;
stop)
  for i in $(seq 1 ${1:-12}); do node $i "sudo systemctl stop resonate" 2>/dev/null & done; wait ;;
kill-node) node $1 "sudo systemctl kill -s KILL resonate; sudo systemctl stop resonate"; echo "killed n$1 $(date +%T)" ;;
restart-node) node $1 "sudo systemctl start resonate"; ready $1; echo "n$1 ready $(date +%T)" ;;
rolling)
  for i in $(seq 1 $1); do node $i "sudo systemctl restart resonate"; ready $i; echo "n$i restarted $(date +%T)"; sleep ${PAUSE:-15}; done ;;
kill-broker) ssh_to $PREFIX-b$1 $(zone_of $1) "sudo docker kill -s KILL kafka"; echo "killed b$1 $(date +%T)" ;;
start-broker) ssh_to $PREFIX-b$1 $(zone_of $1) "sudo docker start kafka"; echo "started b$1 $(date +%T)" ;;
isolate-zone|heal-zone)
  z=${ZONE[$(( $1 - 1 ))]}
  rule="INPUT -p tcp --dport 22 -j ACCEPT"
  $G compute instances list --filter="labels.$PREFIX=1 AND zone:$z AND (name~-b OR name~-n)" --format='value(name)' |
  while read -r vm; do
    if [ $cmd = isolate-zone ]; then
      ssh_to $vm $z "sudo iptables -I INPUT 1 -p tcp --dport 22 -j ACCEPT; sudo iptables -I INPUT 2 -s 10.128.0.0/9 -j DROP; sudo iptables -I OUTPUT 1 -p tcp --sport 22 -j ACCEPT; sudo iptables -I OUTPUT 2 -d 10.128.0.0/9 -j DROP" &
    else
      ssh_to $vm $z "sudo iptables -D INPUT -s 10.128.0.0/9 -j DROP; sudo iptables -D OUTPUT -d 10.128.0.0/9 -j DROP; sudo iptables -D INPUT -p tcp --dport 22 -j ACCEPT; sudo iptables -D OUTPUT -p tcp --sport 22 -j ACCEPT" &
    fi
  done; wait; echo "$cmd $z $(date +%T)" ;;
cut-node|heal-node)
  op=$([ $cmd = cut-node ] && echo -I || echo -D)
  rules=""; for b in $(tr ',' ' ' < .brokers); do rules+="sudo iptables $op OUTPUT -d ${b%:*} -j DROP; "; done
  node $1 "$rules"; echo "$cmd n$1 $(date +%T)" ;;
place-leaders)  # each partition led from its owner's zone (TOPIC)
  $G compute scp ${SSH_FLAGS:-} place_leaders.py $PREFIX-b1:/tmp/place_leaders.py --zone $(zone_of 1) >/dev/null
  ssh_to $PREFIX-b1 $(zone_of 1) "python3 /tmp/place_leaders.py ${TOPIC:-resonate}" ;;
metrics) node $1 "curl -s localhost:9090/metrics | grep -E '^resonate_kafka_(takeover|replayed|partitions_served|commit_errors|forwarded)'" ;;
load)
  name=$1 k=$2; shift 2
  mkdir -p results/$name
  urls=""; metrics=""
  # GATEWAYS: the nodes clients send to (default all), as a health-checked
  # load balancer would choose them; metrics come from every node.
  for i in $(seq 1 $NODES); do
    ip=$(ip_of $PREFIX-n$i $(zone_of $i)); metrics+="${metrics:+,}http://$ip:9090/metrics"
    [[ " ${GATEWAYS:-$(seq 1 $NODES)} " =~ [[:space:]]$i[[:space:]] ]] && urls+="${urls:+,}http://$ip:8001"
  done
  args=("$@")
  # ROUTE=owner: clients send each request to its partition's owner
  if [ "${ROUTE:-any}" = owner ]; then
    args+=(--route owner --brokers "$(cat .brokers)" --topic-prefix "${TOPIC:-resonate}" --partitions "$PARTITIONS")
  fi
  # split --rate across the load generators
  for j in "${!args[@]}"; do
    if [ "${args[$j]}" = --rate ]; then args[$((j+1))]=$(python3 -c "print(${args[$((j+1))]}/$k)"); fi
  done
  for l in $(seq 1 $k); do
    z=$(zone_of $l)
    ssh_to $PREFIX-l$l $z "ulimit -n 1048576; ip=\$(hostname -I | awk '{print \$1}'); /opt/resonate/load --url $urls --metrics $metrics --worker-bind 0.0.0.0:7001 --worker-url http://\$ip:7001/ --timeline /tmp/timeline.csv ${args[*]} > /tmp/out.txt 2>&1; cat /tmp/out.txt; echo '--- timeline'; cat /tmp/timeline.csv" > results/$name/l$l.txt 2>&1 &
  done; wait
  echo "== $name: $* (split over $k loadgens, $NODES nodes)" | tee results/$name/summary.txt
  for l in $(seq 1 $k); do grep '^RESULT' results/$name/l$l.txt | sed 's/^RESULT //' ; done | python3 summarize.py | tee -a results/$name/summary.txt
  ;;
*) echo "unknown command $cmd"; exit 1 ;;
esac
