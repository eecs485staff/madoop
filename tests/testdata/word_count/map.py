#!/usr/bin/env python3
"""Map example."""

import sys

for line in sys.stdin:
    words = line.split()
    for word in words:
        print(word + "\t1")
