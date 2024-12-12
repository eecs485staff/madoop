#!/usr/bin/env python3
"""Invalid reduce executable returns non-zero with an error message."""

import sys

sys.stdout.write("Reduce error message to stdout\n")
sys.stderr.write("Reduce error message to stderr\n")
sys.exit(1)
