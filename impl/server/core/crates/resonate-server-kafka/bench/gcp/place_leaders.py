#!/usr/bin/env python3
"""Run on a broker: make each partition's leader the broker in its owner's zone.

The owner of a partition is the group member it is assigned to; members' client
ids carry their node (`resonate/<node>/<peer url>`). Node rbench-nI lives in zone
((I-1) % 3) and broker I in zone ((I-1) % 3), so the preferred replica of a
partition owned by rbench-nI is broker ((I-1) % 3) + 1. Only the order of each
replica list changes, so nothing moves; a preferred-leader election follows.

    place_leaders.py PREFIX
"""
import json, re, subprocess, sys

prefix = sys.argv[1]
K = ["sudo", "docker", "exec", "-e", "KAFKA_JMX_PORT=", "-e", "KAFKA_OPTS=", "kafka"]
BS = ["--bootstrap-server", "localhost:9092"]

def run(*args, stdin=None):
    return subprocess.run(K + list(args), capture_output=True, text=True, input=stdin, check=True).stdout

members = run("/opt/kafka/bin/kafka-consumer-groups.sh", *BS, "--describe", "--group", prefix, "--members", "--verbose")
owner = {}  # partition -> preferred broker
for line in members.splitlines():
    m = re.search(r"resonate/rbench-n(\d+)/\S+", line)
    a = re.search(re.escape(prefix) + r"\.promises\(([\d,]+)\)", line)
    if m and a:
        broker = (int(m.group(1)) - 1) % 3 + 1
        for p in a.group(1).split(","):
            owner[int(p)] = broker
if not owner:
    sys.exit(f"no assignment found for group {prefix}:\n{members}")

plan = {"version": 1, "partitions": []}
for topic in (f"{prefix}.promises", f"{prefix}.schedules"):
    desc = run("/opt/kafka/bin/kafka-topics.sh", *BS, "--describe", "--topic", topic)
    for line in desc.splitlines():
        m = re.search(r"Partition: (\d+)\s+Leader: \S+\s+Replicas: ([\d,]+)", line)
        if not m:
            continue
        p, replicas = int(m.group(1)), [int(r) for r in m.group(2).split(",")]
        want = owner.get(p)
        if want in replicas and replicas[0] != want:
            replicas.remove(want)
            plan["partitions"].append({"topic": topic, "partition": p, "replicas": [want] + replicas})

if plan["partitions"]:
    run("bash", "-c", "cat > /tmp/plan.json", stdin=json.dumps(plan))
    run("/opt/kafka/bin/kafka-reassign-partitions.sh", *BS, "--reassignment-json-file", "/tmp/plan.json", "--execute")
    run("/opt/kafka/bin/kafka-reassign-partitions.sh", *BS, "--reassignment-json-file", "/tmp/plan.json", "--verify")
out = run("/opt/kafka/bin/kafka-leader-election.sh", *BS, "--election-type", "preferred", "--all-topic-partitions")
placed = sum(1 for line in run("/opt/kafka/bin/kafka-topics.sh", *BS, "--describe", "--topic", f"{prefix}.promises").splitlines()
             if (m := re.search(r"Partition: (\d+)\s+Leader: (\d+)", line)) and owner.get(int(m.group(1))) == int(m.group(2)))
print(f"leaders placed: {placed} of {len(owner)} promise partitions led from their owner's zone "
      f"({len(plan['partitions'])} replica lists reordered)")
