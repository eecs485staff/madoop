Michigan Hadoop CLI
=================

Michigan Hadoop CLI (`madoop`) is a light weight, Python-based Hadoop command line interface.


## Quick start

Install madoop

```console
$ pip install madoop
$ madoop
```

Run a MapReduce job on data in `input_small`

```console
$ cd tests/testdata/input_small
$ $ hadoop \
  -input input \
  -output output \
  -mapper ./wc_map.sh \
  -reducer ./wc_reduce.sh
```

Concactenate and print the output

```console
$ cat output/* | sort
autograder	2
eecs485	1
goodbye	1
hello	3
world	1
```

## Install

```console
$ pip install madoop
$ madoop
```

## Example
### Sample input 

#### file01
```console
hadoop map reduce file map 
map streaming file reduce 
map reduce is cool
```
#### file02
```console
hadoop file system
google file system
```

#### map
```python
#!/usr/bin/env python3
"""Map example."""

import sys

for line in sys.stdin:
    words = line.split()
    for word in words:
        print(word + "\t1")

```
#### reduce
```python
#!/usr/bin/env python3
"""Reduce example."""

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

```

Run the MapReduce job. By default, there will be one mapper for each input file.

- `jar hadoop/hadoop-streaming-2.7.2.jar` is required by the real Hadoop.  The simplified Hadoop Streaming work-a-like we provided ignores this argument.
- `-input DIRECTORY` input directory
- `-output DIRECTORY` output directory
- `-mapper FILE` mapper executable
- `-reducer FILE` reducer executable

```console
$ hadoop \
  jar ../hadoop-streaming-2.7.2.jar \
  -input input \
  -output output \
  -mapper ./map.py \
  -reducer ./reduce.py
Starting map stage
+ ./map.py < output/hadooptmp/mapper-input/part-00000 > output/hadooptmp/mapper-output/part-00000
+ ./map.py < output/hadooptmp/mapper-input/part-00001 > output/hadooptmp/mapper-output/part-00001
Starting group stage
+ cat output/hadooptmp/mapper-output/* | sort > output/hadooptmp/grouper-output/sorted.out
Starting reduce stage
+ reduce.py < output/hadooptmp/grouper-output/part-00000 > output/hadooptmp/reducer-output/part-00000
+ reduce.py < output/hadooptmp/grouper-output/part-00001 > output/hadooptmp/reducer-output/part-00001
+ reduce.py < output/hadooptmp/grouper-output/part-00002 > output/hadooptmp/reducer-output/part-00002
+ reduce.py < output/hadooptmp/grouper-output/part-00003 > output/hadooptmp/reducer-output/part-00003
Output directory: output
```

## Similarities and differences from real Hadoop
### Similarities
- Madoop supports similar command line arguments as real Hadoop.
- Madoop supports multiple mappers and reducers.

### Differences
- Madoop ignores the `jar hadoop/hadoop-streaming-2.7.2.jar` argument.
- The real Hadoop works in a distributed environment whereas Madoop works in a single machine environment.

## Contributing
Contributions from the community are welcome! Check out the [guide for contributing](CONTRIBUTING.md).

## Acknowledgments
Michigan Hadoop CLI is written by Andrew DeOrio <awdeorio@umich.edu>.