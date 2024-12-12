#!/usr/bin/env python3
"""Invalid map executable returns non-zero with an error message."""

import sys

sys.stdout.write("Map error message to stdout\n")
sys.stderr.write("Map error message to stderr\n")
sys.exit(1)
