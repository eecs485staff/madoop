#!/usr/bin/env python3
"""Invalid partition executable returns non-zero with an error message."""

import sys

sys.stderr.write("Partition error message to stdout\n")
sys.stderr.write("Partition error message to stderr\n")
sys.exit(1)
