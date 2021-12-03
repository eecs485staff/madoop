"""Invalid reduce executable Reduce example is missing a shebang."""

import sys
import collections

WORDDICT = {}
for line in sys.stdin:
    word = line.split("\t")[0]
    if word in WORDDICT:
        WORDDICT[word] += 1
    else:
        WORDDICT[word] = 1

SORTEDDICT = collections.OrderedDict(sorted(WORDDICT.items()))
for key in SORTEDDICT:
    print(key, SORTEDDICT[key])