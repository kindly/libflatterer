#!/usr/bin/env python3
"""Reproducible speed profiling for libflatterer.

Builds the `bench` example in release mode and runs a series of measurements:
  * parse-only vs full flatten (to attribute parse cost)
  * thread scaling for ndjson (JSON-lines) and array (single JSON array) input

Usage:
    python3 examples/profiling/profile.py [--repeats N] [--threads 1,2,4,8]

Notes on measurement reliability:
    Wall-clock on a laptop with a `powersave` CPU governor and a busy browser
    can swing ~2x run-to-run. We therefore report the MIN of N interleaved runs,
    which approximates the contention-free time. Always trust the min, not a
    single run.
"""

import argparse
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
DATA = os.path.join(HERE, "data")
NDJSON = os.path.join(DATA, "digi.json")          # JSON-lines (one object per line)
ARRAY = os.path.join(DATA, "digi_array.json")     # same data as a single JSON array
BENCH = os.path.join(REPO, "target", "release", "examples", "bench")
OUT = "/tmp/flatterer_profile_out"


def make_array_file():
    """Derive the array-of-objects file from the JSON-lines file (idempotent).

    This is the 'list of objects' input variant. Output is byte-identical to the
    ndjson run after flattening; it just exercises the YAJL streaming front-end.
    """
    if os.path.exists(ARRAY):
        return
    print(f"generating {ARRAY} from {NDJSON} ...")
    with open(NDJSON) as f, open(ARRAY, "w") as out:
        out.write("[")
        first = True
        for line in f:
            line = line.strip()
            if not line:
                continue
            if not first:
                out.write(",")
            out.write(line)
            first = False
        out.write("]")


def build():
    print("building release example `bench` ...")
    subprocess.run(
        ["cargo", "build", "--release", "--example", "bench"],
        cwd=REPO,
        check=True,
    )


def run(inp, mode, threads):
    out = subprocess.run(
        [BENCH, inp, OUT, mode, str(threads)],
        capture_output=True,
        text=True,
    )
    if out.returncode != 0:
        sys.exit(f"bench failed:\n{out.stderr}")
    m = re.search(r"elapsed=([0-9.]+)", out.stdout)
    return float(m.group(1))


def best(inp, mode, threads, repeats):
    return min(run(inp, mode, threads) for _ in range(repeats))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repeats", type=int, default=5,
                    help="runs per data point; min is reported (default 5)")
    ap.add_argument("--threads", default="1,2,4,8",
                    help="comma-separated thread counts (default 1,2,4,8)")
    args = ap.parse_args()
    thread_list = [int(t) for t in args.threads.split(",")]

    make_array_file()
    build()

    print(f"\nrepeats={args.repeats} (reporting min, ~ contention-free)\n")

    # 1. Parse attribution: pure serde parse vs full flatten, single-thread ndjson.
    parse = best(NDJSON, "parseonly", 1, args.repeats)
    full = best(NDJSON, "ndjson", 1, args.repeats)
    print("=== parse attribution (ndjson, 1 thread) ===")
    print(f"  parse only : {parse:.3f}s")
    print(f"  full       : {full:.3f}s")
    print(f"  parse share: {100 * parse / full:.0f}%  "
          f"(flatten+write: {100 * (full - parse) / full:.0f}%)\n")

    # 2. Thread scaling for both input shapes.
    print("=== thread scaling (min of runs) ===")
    print(f"  {'threads':>8} | {'ndjson':>8} | {'array':>8}")
    print(f"  {'-'*8}-+-{'-'*8}-+-{'-'*8}")
    for t in thread_list:
        n = best(NDJSON, "ndjson", t, args.repeats)
        a = best(ARRAY, "array", t, args.repeats)
        print(f"  {t:>8} | {n:>7.3f}s | {a:>7.3f}s")
    print("\nndjson parallelises across threads; array/json_stream plateaus because")
    print("the YAJL front-end that re-serialises each object runs single-threaded.")


if __name__ == "__main__":
    main()
