#!/usr/bin/env python3
"""How many operations in a recorded history actually worked.

A history in which nothing succeeded is linearizable for free, so a check over
one means nothing. Prints "<events> <succeeded>".
"""
import json
import sys

events = [json.loads(line) for line in open(sys.argv[1]) if line.strip()]
ok = sum(
    1
    for e in events
    if 200 <= ((e.get("res") or {}).get("head", {}).get("status") or 0) < 400
)
print(len(events), ok)
