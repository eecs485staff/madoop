#!/usr/bin/env python3
"""Invalid reduce executable returns non-zero with an error message."""

import sys

sys.stderr.write("Reduce error message")
sys.exit(1)
