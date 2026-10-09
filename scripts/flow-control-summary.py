#!/usr/bin/env python3
"""Summarize research runs in Richard's fork; raw output stays in artifacts."""

import argparse
from collections import Counter
import json
from pathlib import Path
import re
import statistics
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--target-failed", action="store_true")
parser.add_argument("--plain-tcp", action="store_true")
parser.add_argument("path", type=Path)
args = parser.parse_args()
target = "Test/AccountCheckWindowSizeWithLargeWindow"
outcomes = Counter()
durations = []
failures = []
slow = []
current = []
first_start = None
first_sockets = []
non_json = []
plain = []
with args.path.open() as source:
    for line in source:
        try:
            event = json.loads(line)
        except ValueError:
            non_json.append(line.rstrip())
            continue
        if args.plain_tcp:
            plain.append(event)
            continue
        action = event.get("Action")
        test = event.get("Test")
        output = event.get("Output", "")
        if action == "fail":
            failures.append((event.get("Package"), test))
        if test == target and action == "run":
            current = []
        if output and (test == target or "panic: test timed out" in output):
            current.extend(output.rstrip().splitlines())
        if "FLOW_CONTROL_START" in output and first_start is None:
            first_start = output.rstrip()
        if "FLOW_CONTROL_SOCKET" in output and len(first_sockets) < 2:
            first_sockets.append(output.rstrip())
        if test == target and action in ("pass", "fail"):
            outcomes[action] += 1
            elapsed = event.get("Elapsed", 0)
            durations.append(elapsed)
            if elapsed >= 5 or action == "fail":
                slow.append((action, elapsed, current))
            current = []

if args.plain_tcp:
    times = [event["elapsed_ns"] / 1e9 for event in plain]
    collapsed = [event for event in plain if any(
        event["rcvbuf_min"][side] < event["rcvbuf_start"][side]
        for side in ("client", "server"))]
    print(f"{args.path.name}: TCP runs={len(plain)} max_seconds={max(times, default=0):.3f} shrunk={len(collapsed)}")
    for event in plain:
        if event in collapsed or event.get("error"):
            print(json.dumps(event))
    print("\n".join(non_json[-20:]))
    sys.exit(0)

if args.target_failed:
    sys.exit(0 if outcomes["fail"] else 1)

if current:
    slow.append(("unfinished", None, current))
print(f"{args.path.name}: target={dict(outcomes)} max_seconds={max(durations, default=0)} median_seconds={statistics.median(durations) if durations else None}")
print(f"Failures: {failures}")
if first_start:
    print(first_start)
for line in first_sockets:
    print(line)
for action, elapsed, lines in slow:
    text = "\n".join(lines)
    buffers = [int(value) for value in re.findall(r"\brb(\d+)", text)]
    print(f"slow_run: outcome={action} seconds={elapsed} min_rcvbuf={min(buffers, default=None)} snapshots={text.count('FLOW_CONTROL_DIAGNOSTIC')}")
    for line in lines:
        if any(marker in line for marker in (
            "FLOW_CONTROL_DIAGNOSTIC", "FLOW_CONTROL_SOCKET", "so_rcvbuf=", "control queued=",
            "ESTAB", "skmem:", "Error on client", "panic:", "FLOW_SERVER_WRITE_ERROR")):
            print(line)
if non_json:
    print("\n".join(non_json[-20:]))
