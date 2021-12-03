#!/usr/bin/env python3
"""Invalid map executable returns non-zero."""

import sys

for line in sys.stdin:
    words = line.split()
    for word in words:
        print(word + "\t1")

sys.exit(1)
