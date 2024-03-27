#!/usr/bin/env -S python3 -u
"""Word count partitioner."""
import sys


num_reducers = int(sys.argv[1])


for line in sys.stdin:
    key, value = line.split("\t")
    if key[0] <= "G":
        print(0 % num_reducers)
    else:
        print(1 % num_reducers)
