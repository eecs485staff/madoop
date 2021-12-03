#!/bin/bash
#
# Word count mapper.
#
# Input: <text>
# Output: <word><tab><1>

# Stop on errors
set -Eeuo pipefail

# Map
cat | tr '[ \t]' '\n' | tr '[:upper:]' '[:lower:]' | awk '{print $1"\t1"}'
