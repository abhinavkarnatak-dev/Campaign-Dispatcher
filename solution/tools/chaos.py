"""Crash test: run the dispatcher, hard-kill it at random moments, restart it, and
check the result at the end.

    python tools/chaos.py                 # 5 kills, then verify
    python tools/chaos.py --kills 8 --down 12

Run it from the solution folder, with the provider and Temporal already running
(see README). Each kill is a hard kill: SIGKILL on Linux and macOS (same as
kill -9), TerminateProcess on Windows. Nothing gets a chance to clean up.

By default it waits until at least one call is in flight before killing, so the
crash always lands while there is work to lose.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

SOLUTION_DIR = Path(__file__).resolve().parents[1]


def status(port: int) -> dict | None:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/status", timeout=2) as r:
            return json.loads(r.read())
    except Exception:
        return None


def start() -> subprocess.Popen:
    return subprocess.Popen([sys.executable, "-m", "dispatcher", "run"], cwd=SOLUTION_DIR)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--kills", type=int, default=5, help="how many hard kills (default 5)")
    ap.add_argument("--min-gap", type=float, default=5, help="min real seconds between kills")
    ap.add_argument("--max-gap", type=float, default=30, help="max real seconds between kills")
    ap.add_argument("--down", type=float, default=10,
                    help="real seconds to stay dead before restarting (default 10, longer "
                         "than the provider's 7 seconds of webhook retries)")
    ap.add_argument("--any-time", action="store_true",
                    help="kill even when no call is in flight")
    ap.add_argument("--seed", type=int, default=None, help="random seed for kill timing")
    args = ap.parse_args()
    rng = random.Random(args.seed)
    port = int(os.environ.get("WEBHOOK_PORT", "8200"))

    proc = start()
    kills = 0
    while kills < args.kills:
        time.sleep(rng.uniform(args.min_gap, args.max_gap))
        if proc.poll() is not None:
            print(f"[chaos] dispatcher finished before kill {kills + 1}; stopping early")
            break
        s = None
        if not args.any_time:  # wait (up to 20 s) for a call to be in flight
            for _ in range(200):
                s = status(port)
                if s and s.get("active_calls", 0) > 0:
                    break
                time.sleep(0.1)
        s = s or status(port)
        proc.kill()  # SIGKILL / TerminateProcess
        proc.wait()
        kills += 1
        active = s.get("active_calls") if s else "?"
        done = s.get("finished") if s else "?"
        print(f"[chaos] kill {kills}/{args.kills}: {active} calls in flight, {done} leads finished. "
              f"Down for {args.down:.0f}s")
        time.sleep(args.down)
        proc = start()

    print("[chaos] letting the dispatcher run to the end")
    code = proc.wait()
    if code != 0:
        print(f"[chaos] dispatcher exited with code {code}")
        return code
    print("[chaos] verifying")
    return subprocess.call([sys.executable, "-m", "dispatcher", "verify"], cwd=SOLUTION_DIR)


if __name__ == "__main__":
    sys.exit(main())
