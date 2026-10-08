#!/usr/bin/env python3
"""Summarize research runs in Richard's fork without flooding the Actions log."""

import argparse
from collections import Counter
import json
from pathlib import Path
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--target-failed", action="store_true")
parser.add_argument("path", type=Path)
args = parser.parse_args()
target = "Test/AccountCheckWindowSizeWithLargeWindow"
outcomes = Counter()
durations = []
failures = []
details = []
capturing = set()
for line in args.path.read_text().splitlines():
    try:
        event = json.loads(line)
    except ValueError:
        details.append(line)
        continue
    action = event.get("Action")
    test = event.get("Test")
    package = event.get("Package")
    if test == target and action in ("pass", "fail"):
        outcomes[action] += 1
        durations.append(event.get("Elapsed", 0))
    if action == "fail":
        failures.append((event.get("Package"), test))
    output = event.get("Output", "")
    if "FLOW_" in output:
        capturing.add(package)
    if output and (test == target or package in capturing):
        details.extend(output.rstrip().splitlines())
    if test == target and action in ("pass", "fail"):
        capturing.discard(package)

if args.target_failed:
    sys.exit(0 if outcomes["fail"] else 1)

print(f"{args.path.name}: target={dict(outcomes)} max_seconds={max(durations, default=0)}")
print(f"Failures: {failures}")
if outcomes["fail"] or any("FLOW_" in line for line in details):
    # Complete raw output is retained as a workflow artifact.
    print("\n".join(details[-1800:]))
