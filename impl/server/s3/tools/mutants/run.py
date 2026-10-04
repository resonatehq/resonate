#!/usr/bin/env python3
"""Mutation testing for the S3 server: which layer catches which bug.

    tools/mutants/run.py [--only name,...] [--out DIR]

For each mutant in mutants.py: copy impl/server/s3, apply the one edit,
build, and run every layer — independently, so the table shows all the
layers that catch it, not just the first:

  L1 tests      zig build test (unit tests and the catalogue's tests)
  L2 simulator  simulator soak over SIM_RUNS seeds
  L3 loadgen    raw protocol, 8 clients, native server in debug mode,
                checked by lincheck and conccheck
  L4 scenarios  the Go SDK's four scenarios against the native and the wasm
                server on one bucket (fakes3), checked by both checkers;
                runs that fail or time out count as a liveness catch
  L5 fuzz       resonate-fuzz (impl/server/core): guided and informed, every
                answer and state compared with the in-memory reference model,
                and the server's messages with the ones the model emitted

A layer "catches" a mutant if it fails where it passes on the unmutated
server (the baseline, run first). Tools come from the environment:
ZIG, SCENARIOS, LOADGEN, LINCHECK, CONCCHECK, NODE, FUZZ (default: on PATH).
"""
import argparse, json, os, shutil, signal, subprocess, sys, time, socket

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.dirname(os.path.dirname(HERE))  # impl/server/s3
sys.path.insert(0, HERE)
from mutants import MUTANTS, apply  # noqa: E402

ZIG = os.environ.get("ZIG", "zig")
SCENARIOS = os.environ.get("SCENARIOS", "scenarios")
LOADGEN = os.environ.get("LOADGEN", "loadgen")
LINCHECK = os.environ.get("LINCHECK", "lincheck")
CONCCHECK = os.environ.get("CONCCHECK", "conccheck")
NODE = os.environ.get("NODE", "node")
FUZZ = os.environ.get("FUZZ", "resonate-fuzz")
SIM_RUNS = int(os.environ.get("SIM_RUNS", "100"))
ROUNDS = int(os.environ.get("ROUNDS", "2"))


def sh(cmd, cwd=None, timeout=900, stdin=None):
    """Run; return (exit code, combined output). A timeout is exit 124."""
    try:
        p = subprocess.run(cmd, cwd=cwd, stdin=stdin, stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT, timeout=timeout, text=True)
        return p.returncode, p.stdout
    except subprocess.TimeoutExpired as e:
        out = e.stdout.decode() if isinstance(e.stdout, bytes) else (e.stdout or "")
        return 124, out + "\n[timeout]"


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Procs:
    """Background servers, killed together."""

    def __init__(self, logdir):
        self.procs, self.logdir = [], logdir

    def start(self, name, cmd, ready_url=None):
        log = open(f"{self.logdir}/{name}.log", "w")
        p = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        self.procs.append(p)
        if ready_url:
            for _ in range(100):
                if sh(["curl", "-sf", ready_url], timeout=5)[0] == 0:
                    return
                time.sleep(0.1)
            raise RuntimeError(f"{name} never became ready")
        else:
            time.sleep(0.5)

    def stop(self):
        for p in self.procs:
            try:
                os.killpg(p.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        for p in self.procs:
            p.wait()
        self.procs = []


notes = []  # lincheck refutations, reported but not counted


def check(trace_prefix, logdir, tag):
    """Both checkers over one trace. Returns a list of failures."""
    bad = []
    for tool, ext in ((LINCHECK, ".ndjson"), (CONCCHECK, ".history")):
        with open(trace_prefix + ext) as f:
            code, out = sh([tool], stdin=f, timeout=600)
        name = os.path.basename(tool)
        open(f"{logdir}/{tag}.{name}.txt", "w").write(out)
        if "NOT LINEARIZABLE" in out:
            if tool == CONCCHECK:
                bad.append(f"{tag}: conccheck refuted")
            else:
                # The recorded order failing is a statement about that one
                # order, not the server (valid/README.md): noted, not counted.
                notes.append(f"{tag}: lincheck refuted the recorded order")
        elif code != 0 and "LINEARIZABLE" not in out:
            bad.append(f"{tag}: {name} error")
    return bad


def layer_tests(root, logdir):
    code, out = sh([ZIG, "build", "test"], cwd=root, timeout=1800)
    open(f"{logdir}/tests.txt", "w").write(out)
    return [] if code == 0 else ["zig build test failed"]


def layer_simulator(root, logdir):
    code, out = sh([f"{root}/zig-out/bin/simulator", "soak", "--runs", str(SIM_RUNS)], timeout=1800)
    open(f"{logdir}/simulator.txt", "w").write(out)
    return [] if code == 0 else [out.strip().splitlines()[-1] if out.strip() else "simulator failed"]


def layer_loadgen(root, logdir):
    bad = []
    for seed in (1, 2, 3):
        procs = Procs(logdir)
        port = free_port()
        try:
            procs.start(f"loadgen-server-{seed}", [f"{root}/zig-out/bin/resonate", "serve", "--store", "memory",
                                                   "--debug", "--port", str(port)], f"http://127.0.0.1:{port}/ready")
            prefix = f"{logdir}/loadgen-{seed}"
            code, out = sh([LOADGEN, "-url", f"http://127.0.0.1:{port}/", "-seed", str(seed), "-out", prefix], timeout=600)
            open(prefix + ".txt", "w").write(out)
            if code != 0:
                bad.append(f"loadgen seed {seed}: exit {code}")
                continue
            bad += check(prefix, logdir, f"loadgen-{seed}")
        finally:
            procs.stop()
    return bad


def layer_scenarios(root, logdir):
    bad = []
    procs = Procs(logdir)
    s3, zport, wport = free_port(), free_port(), free_port()
    try:
        procs.start("fakes3", [f"{root}/zig-out/bin/fakes3", "--port", str(s3)])
        common = ["--store", "s3", "--endpoint", f"http://127.0.0.1:{s3}", "--bucket", "b", "--debug", "--deliver"]
        procs.start("native", [f"{root}/zig-out/bin/resonate", "serve", *common, "--port", str(zport)],
                    f"http://127.0.0.1:{zport}/ready")
        procs.start("wasm", [NODE, f"{root}/wasm/host.mjs", "serve", "--wasm", f"{root}/zig-out/bin/resonate.wasm",
                             *common, "--port", str(wport)], f"http://127.0.0.1:{wport}/ready")
        urls = f"http://127.0.0.1:{zport},http://127.0.0.1:{wport}"
        for rnd in range(ROUNDS):
            for sc in ("simple-run", "simple-rpc", "simple-sleep", "fan-out"):
                tag = f"scenarios-{sc}-{rnd}"
                prefix = f"{logdir}/{tag}"
                code, out = sh([SCENARIOS, sc, "-url", urls, "-transport", "push", "-clock", "wall", "-reset=false",
                                "-prefix", f"r{rnd}-", "-runs", "8", "-parallel", "3", "-timeout", "15s",
                                "-out", prefix], timeout=300)
                open(prefix + ".txt", "w").write(out)
                failed = next((l for l in out.splitlines() if "runs ok," in l), "")
                if code != 0 or not failed or " 0 failed" not in failed:
                    bad.append(f"{tag}: {failed.strip() or f'exit {code}'} (liveness)")
                if os.path.exists(prefix + ".ndjson") and os.path.getsize(prefix + ".ndjson") > 0:
                    bad += check(prefix, logdir, tag)
    finally:
        procs.stop()
    return bad


def layer_fuzz(root, logdir):
    bad = []
    for seed in (1, 2):
        procs = Procs(logdir)
        port = free_port()
        try:
            procs.start(f"fuzz-server-{seed}", [f"{root}/zig-out/bin/resonate", "serve", "--store", "memory",
                                                "--debug", "--deliver", "--port", str(port)],
                        f"http://127.0.0.1:{port}/ready")
            # Searches off: promise.search reports a promise past its deadline as
            # pending where the model has it timed out, a known divergence that
            # would otherwise mask everything else. Resets stay on: without them
            # the memory store's documents pile up and debug.snap overflows the
            # stack (the scanner recurses once per document on a store that
            # answers inline) — a second known issue.
            code, out = sh([FUZZ, "--url", f"http://127.0.0.1:{port}", "--programs", "40", "--seed", str(seed),
                            "--searches", "false"],
                           timeout=600)
            open(f"{logdir}/fuzz-{seed}.txt", "w").write(out)
            if code != 0:
                first = next((l.strip() for l in out.splitlines() if l.startswith(("DISAGREE", "STATE"))), f"exit {code}")
                bad.append(f"fuzz seed {seed}: {first}")
            msg = next((l for l in out.splitlines() if "offers received" in l), "")
            import re
            m = re.search(r"; (\d+) the oracle expected never came, (\d+) came unexpected", msg)
            if m and (int(m.group(1)) or int(m.group(2))):
                bad.append(f"fuzz seed {seed}: messages {m.group(1)} missing, {m.group(2)} unexpected")
        finally:
            procs.stop()
    return bad


LAYERS = [("tests", layer_tests), ("simulator", layer_simulator),
          ("loadgen", layer_loadgen), ("scenarios", layer_scenarios), ("fuzz", layer_fuzz)]


def run_one(mutant, out):
    name = mutant["name"] if mutant else "baseline"
    root = f"{out}/{name}/s3"
    logdir = f"{out}/{name}/logs"
    shutil.rmtree(f"{out}/{name}", ignore_errors=True)
    shutil.copytree(SRC, root, ignore=shutil.ignore_patterns("zig-out", ".zig-cache", "tools"))
    os.makedirs(logdir)
    if mutant:
        apply(root, mutant)
    code, build = sh([ZIG, "build", "-Doptimize=ReleaseSafe"], cwd=root, timeout=1800)
    code2, wasm = sh([ZIG, "build", "wasm"], cwd=root, timeout=1800)
    if code or code2:
        open(f"{logdir}/build.txt", "w").write(build + wasm)
        return {"name": name, "build": "failed"}
    notes.clear()
    result = {"name": name, "kind": mutant["kind"] if mutant else "-", "layers": {}}
    for lname, fn in LAYERS:
        t0 = time.time()
        failures = fn(root, logdir)
        result["layers"][lname] = {"caught": bool(failures), "why": failures[:3],
                                   "seconds": round(time.time() - t0)}
        print(f"  {name:30} {lname:10} {'CAUGHT' if failures else 'missed':7} {failures[:1]}", flush=True)
    result["notes"] = list(notes)
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="")
    ap.add_argument("--out", default="/tmp/resonate-mutants")
    ap.add_argument("--layers", default="", help="comma-separated subset of: " + ",".join(l for l, _ in LAYERS))
    args = ap.parse_args()
    if args.layers:
        keep = args.layers.split(",")
        LAYERS[:] = [(n, f) for n, f in LAYERS if n in keep]
    chosen = [m for m in MUTANTS if not args.only or m["name"] in args.only.split(",")]
    os.makedirs(args.out, exist_ok=True)
    results = [run_one(None, args.out)]
    base = results[0]
    if base.get("build") == "failed" or any(l["caught"] for l in base["layers"].values()):
        print("the unmutated server fails a layer; fix that first:", json.dumps(base, indent=1))
        sys.exit(2)
    for m in chosen:
        results.append(run_one(m, args.out))
        json.dump(results, open(f"{args.out}/results.json", "w"), indent=1)

    cols = [l for l, _ in LAYERS]
    print(f"\n{'mutant':30} {'kind':9} " + " ".join(f"{c:10}" for c in cols))
    for r in results[1:]:
        if r.get("build") == "failed":
            print(f"{r['name']:30} build failed")
            continue
        cells = " ".join(f"{'CAUGHT' if r['layers'][c]['caught'] else '-':10}" for c in cols)
        print(f"{r['name']:30} {r['kind']:9} {cells}")
    survivors = [r["name"] for r in results[1:] if r.get("layers") and not any(l["caught"] for l in r["layers"].values())]
    print(f"\nsurvived every layer: {', '.join(survivors) or 'none'}")


if __name__ == "__main__":
    main()
