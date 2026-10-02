#!/bin/bash
# Why is a commit 4-7 ms? From the network floor up: RTT between zones, Kafka
# alone with the writer's producer settings (plus the brokers' own Produce
# breakdown), then Resonate with and without Nagle on the producer.
cd "$(dirname "$0")"; source env.sh
b=($(tr ',' ' ' < .brokers)); b=("${b[@]%:9092}")
n1() { ssh_to $PREFIX-n1 $(zone_of 1) "$@"; }
broker() { ssh_to $PREFIX-b$1 $(zone_of $1) "$2"; }
jmx() {  # the Produce request breakdown on every broker (ms, recent-biased)
  for i in 1 2 3; do
    echo "  broker $i Produce:"
    broker $i "for m in TotalTimeMs RequestQueueTimeMs LocalTimeMs RemoteTimeMs ResponseQueueTimeMs ResponseSendTimeMs; do
      v=\$(sudo docker exec -e KAFKA_JMX_PORT= -e KAFKA_OPTS= kafka /opt/kafka/bin/kafka-run-class.sh org.apache.kafka.tools.JmxTool \
        --jmx-url service:jmx:rmi:///jndi/rmi://127.0.0.1:9999/jmxrmi --one-time true --report-format properties \
        --object-name kafka.network:type=RequestMetrics,name=\$m,request=Produce --attributes Mean,50thPercentile,99thPercentile 2>/dev/null | tail -1)
      echo \"    \$m \$v\"; done"
  done
}

echo "==== 1. network: round trips from node 1 (zone a) and between brokers"
n1 "for ip in ${b[*]}; do echo -n \"  n1 -> \$ip: \"; ping -q -c 50 -i 0.2 \$ip | tail -1; done"
broker 1 "for ip in ${b[1]} ${b[2]}; do echo -n \"  b1 -> \$ip: \"; ping -q -c 50 -i 0.2 \$ip | tail -1; done"

echo "==== 2. Kafka alone, from node 1, the writer's producer settings"
n1 "sudo systemctl stop resonate; gcloud storage cp -q $BUCKET/bin/produce_latency /tmp/pl && chmod +x /tmp/pl"
pl() { n1 "/tmp/pl --brokers $(cat .brokers) --seconds 20 $*" 2>&1 | grep -v '^$'; }
echo "-- a. as deployed: replication 3, 12 loops x 2 records";       pl --replication 3; jmx
echo "-- b. a + socket.nagle.disable=true";                          pl --replication 3 --set socket.nagle.disable=true; jmx
echo "-- c. a with acks=1 (no wait for followers)";                  pl --replication 3 --set enable.idempotence=false --set acks=1
echo "-- d. replication 1";                                          pl --replication 1
echo "-- e. one idle loop, replication 3";                           pl --replication 3 --loops 1
echo "-- f. one idle loop, replication 3, nagle off";                pl --replication 3 --loops 1 --set socket.nagle.disable=true
echo "-- g. a with 48-record rounds (as at 30k ops/s)";              pl --replication 3 --batch 48

echo "==== 3. Resonate: 3 nodes, 12 partitions, owner routing, lifecycle"
for nagle in default off; do
  if [ $nagle = off ]; then export LIBRDKAFKA='"topic.metadata.refresh.interval.ms" = "10000"\n"socket.nagle.disable" = "true"'
  else export LIBRDKAFKA='"topic.metadata.refresh.interval.ms" = "10000"'; fi
  echo "-- nagle $nagle"
  PARTITIONS=12 ROUTES=owner ./ab.sh 3 2000 15000 2>&1 | grep -E '^--|^ops|node 1|served|commit|verify|not all' | cut -c1-170
  [ $nagle = off ] && jmx
done
