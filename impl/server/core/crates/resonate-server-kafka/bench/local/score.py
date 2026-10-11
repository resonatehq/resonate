#!/usr/bin/env python3
"""The scorecard of one prof.sh run: CPU per request by process, node 1's
profile folded into components, wakeups and syscalls per request, and the
server's own time breakdown.

    python3 score.py results/NAME [--json]

Components come from perf's call stacks, frame pointers throughout. A sample
counts towards the innermost frame that names a component (so JSON parsed
while forwarding counts as JSON); `stage` instead names the outermost
request-path frame (so the same sample counts as forwarding). Leaf classes
say what the innermost frame was doing, whatever the component.
"""

import collections
import json
import re
import sys

# Innermost match wins. Order matters only within one frame.
COMPONENTS = [
    ("kafka client", re.compile(r"^(rd_|rd_kafka|rdkafka|rdk_|lz4|LZ4|crc32c|rd_crc32c)|rdkafka::|librdkafka")),
    ("rocksdb", re.compile(r"rocksdb|^_ZN7rocksdb|librocksdb")),
    ("json", re.compile(r"serde_json|<serde::|serde::de|serde::ser|serde_path")),
    ("record codec", re.compile(r"resonate_server_kafka::record|postcard|resonate_server_blob::codec")),
    ("decide", re.compile(r"resonate_server_blob::(applier|kernel|state|machine)|resonate_core::")),
    ("partition shell", re.compile(r"resonate_server_kafka::(partition|cache|timers|prune|keys|log)")),
    ("forward client", re.compile(r"resonate_server_kafka::peer|reqwest::|hyper_util::client|hyper::client")),
    ("metrics", re.compile(r"prometheus|resonate_server_kafka::metrics|metrics::")),
    ("http server", re.compile(r"hyper::|axum::|tower::|tower_http|h2::|resonate_gateway_http|httparse|http::")),
    ("tokio runtime", re.compile(r"tokio::|mio::|parking_lot|futures_util|futures_core")),
]
# The request path, outermost first: which job a sample was part of.
STAGES = [
    ("forwarding", re.compile(r"resonate_server_kafka::peer")),
    ("partition actor", re.compile(r"resonate_server_kafka::partition")),
    ("gateway", re.compile(r"resonate_gateway_http|axum::|hyper::server|hyper::proto")),
]
# Why a kernel sample was in the kernel, from the user frame that called in
# (and the two above it, to tell a wakeup write from a socket write).
KERNEL = [
    ("epoll", re.compile(r"^(epoll_wait|epoll_pwait)")),
    ("wakeup write", re.compile(r"^(__GI___libc_write|write|__write)[^\n]*(\n[^\n]*)*?(Waker::wake|rd_kafka_toppar_enq_msg|"
                                r"rd_kafka_q_io_event|rd_kafka_q_yield|wakeup)")),
    ("socket send", re.compile(r"^(__GI___writev|writev|__libc_sendmsg|sendmsg|__libc_send|send|sendto|"
                               r"__GI___libc_write|write)\b")),
    ("socket recv", re.compile(r"^(readv|__libc_recvmsg|recvmsg|__libc_recv|recv|recvfrom|read|__GI___libc_read)\b")),
    ("librdkafka poll", re.compile(r"^(__poll|poll|ppoll)\b")),
    ("thread park/wake", re.compile(r"^(syscall|.*futex|pthread_cond|__GI___lll|__lll_lock|pthread_mutex|"
                                    r"sched_yield|nanosleep|clock_nanosleep)")),
    ("memory", re.compile(r"^(__mprotect|mprotect|mmap|munmap|madvise|brk|_int_malloc|malloc|sysmalloc|"
                          r"__memmove|__memset|memcpy|cfree|_int_free)")),
]
LEAF = [
    ("alloc", re.compile(r"^(malloc|free|realloc|calloc|cfree|_int_malloc|_int_free|_int_realloc|"
                         r"malloc_consolidate|__libc_malloc|__libc_free|__rust_alloc|__rust_dealloc|"
                         r"__rdl_|alloc::alloc|tcache|unlink_chunk|sysmalloc)")),
    ("memcpy/memmove", re.compile(r"__memcpy|__memmove|memcpy|memmove|__memset|memset|__memcmp|memcmp")),
    ("locks/futex", re.compile(r"pthread_mutex|pthread_cond|__lll_lock|futex|parking_lot")),
]


def threads(name):
    if name.startswith("rdk:broker"):
        return "librdkafka broker threads"
    if name.startswith("rdk:"):
        return "librdkafka other threads"
    if name.startswith("tokio-runtime") or name.startswith("tokio-rt"):
        return "tokio workers"
    if name.startswith("rocksdb") or name.startswith("rocksdb:"):
        return "rocksdb threads"
    return "other: " + re.sub(r"[0-9]+$", "#", name)


def parse_perf(path):
    """Yield (comm, [frames innermost first], [dsos])."""
    comm, frames, dsos = None, [], []
    with open(path, errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line.strip():
                if comm is not None:
                    yield comm, frames, dsos
                comm, frames, dsos = None, [], []
                continue
            if not line.startswith((" ", "\t")):
                # "comm  tid" header (comm may contain spaces)
                m = re.match(r"^(.*?)\s+(\d+)\s*$", line)
                comm = (m.group(1) if m else line).strip()
                continue
            m = re.match(r"^\s*[0-9a-f]+\s+(.*?)\s+\((.*)\)\s*$", line)
            if m:
                sym, dso = m.group(1), m.group(2)
                sym = re.sub(r"\+0x[0-9a-f]+$", "", sym)
                frames.append(sym)
                dsos.append(dso)
    if comm is not None:
        yield comm, frames, dsos


def classify(frames, dsos):
    comp = None
    for sym, dso in zip(frames, dsos):
        if "kernel" in dso:
            continue
        for name, rx in COMPONENTS:
            if rx.search(sym) or (name == "kafka client" and "rdkafka" in dso):
                comp = name
                break
        if comp:
            break
    stage = None
    for sym in reversed(frames):
        for name, rx in STAGES:
            if rx.search(sym):
                stage = name
                break
        if stage:
            break
    leaf = "kernel" if dsos and "kernel" in dsos[0] else None
    if leaf is None and frames:
        for name, rx in LEAF:
            if rx.search(frames[0]):
                leaf = name
                break
    why = None
    if leaf == "kernel":
        user = [re.sub(r"::h[0-9a-f]{16}$", "", f) for f, d in zip(frames, dsos) if "kernel" not in d]
        key = "\n".join(user[:3])
        for name, rx in KERNEL:
            if rx.search(key):
                why = name
                break
        why = why or ("in kernel, no user frame" if not user else "other: " + user[0][:40])
    return comp or "unattributed", stage or "-", leaf or "user code", why


def table(title, counter, total, per_req_us, top=None):
    out = [f"\n{title}"]
    for k, v in counter.most_common(top):
        share = v / total
        out.append(f"  {k:<28} {100 * share:5.1f}%   {share * per_req_us:7.1f} µs/req")
    return out


def main():
    d = sys.argv[1]
    cpu = {}
    nodes = []
    for line in open(f"{d}/cpu.txt"):
        parts = line.split()
        if parts[0] == "cpu":
            if parts[1] == "node":
                nodes.append(int(parts[3]))
            else:
                cpu[parts[1]] = int(parts[3])
        else:
            cpu[parts[0]] = float(parts[1])
    hz, reqs, window = cpu["hz"], cpu["requests"], cpu["window_s"]
    us = lambda ticks: ticks / hz * 1e6 / max(reqs, 1)

    result = {}
    for line in open(f"{d}/load.txt"):
        if line.startswith("RESULT "):
            result = json.loads(line[7:])
    srv = result.get("server", {})

    stat = {}
    try:
        for line in open(f"{d}/perfstat.csv"):
            p = line.strip().split(",")
            if len(p) > 3 and p[0] and p[0][0].isdigit():
                stat[p[2]] = float(p[0])
    except FileNotFoundError:
        pass

    node_us = us(sum(nodes))
    lines = [
        f"== {d.rstrip('/').split('/')[-1]}: {result.get('mode')} at {result.get('rate'):.0f} ops/s, "
        f"{result.get('requests_per_s', 0):.0f} req/s; {reqs:.0f} requests in a {window:.1f} s window",
        f"latency      p50 {result.get('p50_ms', 0):.1f} ms  p99 {result.get('p99_ms', 0):.1f} ms  "
        f"failed {result.get('ops_failed')}  verify {result.get('verify')}",
        f"CPU µs/req   nodes {node_us:.0f}  (per node {', '.join(f'{us(n):.0f}' for n in nodes)})   "
        f"broker {us(cpu['broker']):.0f}   loadgen {us(cpu['load']):.0f}",
        f"node cores   {sum(nodes) / hz / window:.2f} busy of the window",
    ]
    if stat:
        per = lambda k: stat.get(k, 0) / max(reqs, 1)
        lines.append(
            f"per request  {per('context-switches'):.2f} context switches, "
            f"{per('raw_syscalls:sys_enter'):.1f} syscalls, {per('cpu-migrations'):.2f} migrations (both nodes)"
        )
    if srv:
        g = lambda k, f="mean_ms": srv.get(k, {}).get(f, float("nan"))
        lines.append(
            f"server mean  edge {g('edge'):.2f}  server {g('server'):.2f}  queue {g('queue'):.2f}  "
            f"round {g('round'):.2f}  commit {g('commit'):.2f} ms;  "
            f"{srv.get('requests_per_round', float('nan')):.2f} req/round; "
            f"forwarded {srv.get('forwarded', 0):.0f} of {srv.get('served', 0):.0f}"
        )

    comps, stages, leaves, thr, sym, why = (collections.Counter() for _ in range(6))
    n = 0
    for comm, frames, dsos in parse_perf(f"{d}/perf.script"):
        n += 1
        thr[threads(comm)] += 1
        c, s, l, w = classify(frames, dsos)
        if w:
            why[w] += 1
        comps[c] += 1
        stages[s] += 1
        leaves[l] += 1
        if frames:
            sym[re.sub(r"::h[0-9a-f]{16}$", "", frames[0])[:90]] += 1
    if n:
        # Node 1's profile, scaled to node 1's CPU per request.
        per = us(nodes[0])
        lines.append(f"\nnode 1 profile: {n} samples, {per:.0f} µs/req on node 1")
        lines += table("by thread", thr, n, per)
        lines += table("by component (innermost)", comps, n, per)
        lines += table("by request stage (outermost)", stages, n, per)
        lines += table("by leaf", leaves, n, per)
        lines += table("kernel, by why (share of all samples)", why, n, per, top=10)
        lines += table("top leaf symbols", sym, n, per, top=15)
    print("\n".join(lines))


if __name__ == "__main__":
    main()
