#!/usr/bin/env python3
"""Invalid map executable returns non-zero with an error message."""

import sys

sys.stderr.write("Map error message")
sys.exit(1)
