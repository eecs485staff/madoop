Hadoop Streaming in Python
===========================

This tutorial shows how to write MapReduce programs in Python that are compatible with [Hadoop Streaming](https://hadoop.apache.org/docs/r1.2.1/streaming.html).  We'll use Python's `itertools.groupby()` function to simplify our code.

Install Madoop, a light weight MapReduce framework for education. Madoop implements the Hadoop Streaming interface.
```console
$ pip install madoop
```

We'll use an example MapReduce program and input files provided by Madoop.
```console
$ madoop --example
$ tree example
example
├── input
│   ├── input01.txt
│   └── input02.txt
├── map.py
└── reduce.py
```

Execute the example MapReduce program using Madoop and show the output.
```console
$ cd example
$ madoop \
  -input input \
  -output output \
  -mapper map.py \
  -reducer reduce.py
  
$ cat output/part-*
Goodbye 1
Bye 1
Hadoop 2
World 2
Hello 2
```

## Overview
[Hadoop Streaming](https://hadoop.apache.org/docs/r1.2.1/streaming.html) is a MapReduce API that works with any programming language.  The mapper and the reducer are executables that read input from stdin and write output to stdout.

## Map
The mapper is an executable that reads input from stdin and writes output to stdout.  Here's an example `map.py` which is part of a word count MapReduce program.
```python
#!/usr/bin/env python3
"""Word count mapper."""
import sys

for line in sys.stdin:
    words = line.split()
    for word in words:
        print(f"{word}\t1")
```

The map input format is up to the programmer.  For example:
```console
$ cat input/input01.txt
Hello World
Bye World
$ cat input/input02.txt
Hello Hadoop
Goodbye Hadoop
```

Each map output line contains one key-value pair separated by a TAB character.  We'll fake the map stage at the command line.  While a real MapReduce framework may use multiple map executions, our example runs one.
```console
$ cat input/input* | python3 map.py
Hello	1
World	1
Bye	1
World	1
Hello	1
Hadoop	1
Goodbye	1
Hadoop	1
```

## Group
The MapReduce framework provides the Group functionality.  You can fake the group stage with the `sort` command:
```console
$ cat input/input* | python3 map.py | sort
Bye	1
Goodbye	1
Hadoop	1
Hadoop	1
Hello	1
Hello	1
World	1
World	1
```

## Reduce
The reducer is an executable that reads input from stdin and writes output to stdout.  Here's a simplified `reduce.py` which is part of a word count MapReduce program.  **This is a bad example because it's inefficient and processes multiple groups all at once.**
```python
#!/usr/bin/env python3
"""Word count reducer.

BAD EXAMPLE: it's inefficient and processes multiple groups all at once.

"""
import sys
import collections

def main():
    """Reduce multiple groups."""
    word_count = collections.defaultdict(int)
    for line in sys.stdin:
        word, _, count = line.partition("\t")
        word_count[word] += int(count)
    for word, count in word_count.items():
        print(f"{word}\t{count}")

if __name__ == "__main__":
    main()
```

Each reduce input line contains one key-value pair separated by a TAB character.  For example:
```
Hello	1
Hello	1
```

One reducer execution receives every key in a group.  For example, these two groups are impossible:
```
Hello	1
```

```
Hello	1
```

Lines are supplied to each reducer execution in sorted order.  The entire line is sorted, not just the key.  For example, these unsorted lines will never happen:
```
Hadoop	1
Bye	1
Hadoop	1
Goodbye	1
```

One reducer execution may receive multiple groups.  For example:
```
Bye	1
Goodbye	1
Hadoop	1
Hadoop	1
```

The reduce output format is up to the programmer.  Here's how to run the whole word count MapReduce program at the command line.
```console
$ cat input/input* | python3 map.py | sort | python3 reduce.py
Bye	1
Goodbye	1
Hadoop	2
Hello	2
World	2
```

## `itertools.groupby()`
In this section, we'll simplify our reducer code by using [`itertools.groupby()`](https://docs.python.org/3/library/itertools.html?highlight=groupby#itertools.groupby) to separate the input into groups of lines that share a key.

Our earlier `reduce.py` was a bad example for two reasons:
1. It's inefficient.  It put everything in a big dictionary `word_count`.
2. It's complicated.  It processed multiple groups.  This strategy becomes more of a problem in more complicated MapReduce programs.

```python
# Bad example: Inefficient and complicated
def main():
    """Reduce multiple groups."""
    word_count = collections.defaultdict(int)
    for line in sys.stdin:
        word, _, count = line.partition("\t")
        word_count[word] += int(count)
    for word, count in word_count.items():
        print(f"{word}\t{count}")
```

If one reducer execution received one group, we could simplify the reducer and use only O(1) memory.
```python
# Good example: Efficient and simple
def reduce_one_group(key, group):
    """Reduce one group."""
    word_count = 0
    for line in group:
        count = line.partition("\t")[2]
        word_count += int(count)
    print(f"{key}\t{word_count}")
```

If one reducer execution input contains multiple groups, how can we process one group at a time?  We'll use `itertools.groupby()`.

### Input and output
[`itertools.groupby()`](https://docs.python.org/3/library/itertools.html?highlight=groupby#itertools.groupby) separates input into groups that share a key.  If the input to `groupby()` looks like this:
```
Bye	1
Goodbye	1
Hadoop	1
Hadoop	1
```

The output will be an iterator over three groups, like this:
```
Bye	1
---------
Goodbye	1
---------
Hadoop	1
Hadoop	1
```

### Reducer using `groupby()`
The reducer's main function will divide the input into groups of lines and call a function `reduce_one_group()` on each group.  `groupby()` assumes that the input sorted.  Hadoop Streaming provides sorted input to the reducer.
```python
def main():
    """Divide sorted lines into groups that share a key."""
    for key, group in itertools.groupby(sys.stdin, keyfunc):
        reduce_one_group(key, group)
```

The `keyfunc` function extracts the key.  When lines share a key, they share a group.
```python
def keyfunc(line):
    """Return the key from a TAB-delimited key-value pair."""
    return line.partition("\t")[0]
```

For example:
```python
>>> keyfunc("Hello\t1")
'Hello'
```

We can process one group at a time with `reduce_one_group()`.  The `key` will be one word in our word count example.  The `group` is an iterator over lines of input that start with `key`.
```python
def reduce_one_group(key, group):
    """Reduce one group."""
    word_count = 0
    for line in group:
        count = line.partition("\t")[2]
        word_count += int(count)
    print(f"{key}\t{word_count}")
```

Finally, we can run our entire MapReduce program.
```console
$ cat input/* | ./map.py | sort| ./reduce.py
Bye	1
Goodbye	1
Hadoop	2
Hello	2
World	2
```

### Template reducer
Here's a template you can copy-paste to get started on a reducer.  The only part you need to edit is the `"IMPLEMENT ME"` line.
```python
#!/usr/bin/env python3
"""Word count reducer."""
import sys
import itertools


def reduce_one_group(key, group):
    """Reduce one group."""
    assert False, "IMPLEMENT ME"


def keyfunc(line):
    """Return the key from a TAB-delimited key-value pair."""
    return line.partition("\t")[0]


def main():
    """Divide sorted lines into groups that share a key."""
    for key, group in itertools.groupby(sys.stdin, keyfunc):
        reduce_one_group(key, group)


if __name__ == "__main__":
    main()
```

## Tips and tricks
These are some pro-tips for working with MapReduce programs written in Python for the Hadoop Streaming interface.

### Debugging
We encounter a problem if we add a `breakpoint()` in `map.py`.
```python
for line in sys.stdin:
    breakpoint()  # Added this line
    words = line.split()
    for word in words:
        print(f"{word}\t1")
```

PDB/PDB++ confuses the stdin being piped in from the input file for user input, so we get these errors:
```console
$ cat input/input* | ./map.py
...
(Pdb++) *** SyntaxError: invalid syntax
(Pdb++) *** SyntaxError: invalid syntax
(Pdb++) *** SyntaxError: invalid syntax
...
```

To solve this problem, temporarily refactor your program to read every line of stdin to a variable first. [SO post](https://stackoverflow.com/questions/9178751/use-pdb-set-trace-in-a-script-that-reads-stdin-via-a-pipe/34687825#34687825)
```python
lines = sys.stdin.readlines()  # Temporary addition
sys.stdin = open("/dev/tty")  # Temporary addition

for line in lines:  # Temporary modification
    breakpoint()
    words = line.split()
    for word in words:
        print(f"{word}\t1")
```

Now our debugger works correctly.
```console
$ cat input/input* | ./map.py
...
(Pdb++)
```

Don't forget to remove your temporary changes!

### Multiple iteration
To iterate over the same group twice, convert it to a `list`.  By default, a group is an iterable, which is "one pass".
```python
def reduce_one_group(key, group):
    group = list(group)  # iterable to list
    for line in group:
        pass  # Do something
    for line in group:
        pass  # Do something
```
