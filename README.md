Madoop: Michigan Hadoop
=======================

[![PyPI](https://img.shields.io/pypi/v/madoop.svg)](https://pypi.org/project/madoop/)
[![CI main](https://github.com/eecs485staff/madoop/workflows/CI/badge.svg?branch=develop)](https://github.com/eecs485staff/madoop/actions?query=branch%3Adevelop)
[![codecov](https://codecov.io/gh/eecs485staff/madoop/branch/develop/graph/badge.svg)](https://codecov.io/gh/eecs485staff/madoop)

Michigan Hadoop (`madoop`) is a light weight MapReduce framework for education.  Madoop implements the [Hadoop Streaming](https://hadoop.apache.org/docs/r1.2.1/streaming.html) interface.  Madoop is implemented in Python and runs on a single machine.

For an in-depth explanation of how to write MapReduce programs in Python for Hadoop Streaming, see our [Hadoop Streaming tutorial](README_hadoop_streaming.md).


## Quick start
Install Madoop.
```console
$ pip install madoop
```

Create example MapReduce program with input files.
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

Run example word count MapReduce program.
```console
$ madoop \
  -input example/input \
  -output example/output \
  -mapper example/map.py \
  -reducer example/reduce.py
```

Concatenate and print the output.
```console
$ cat example/output/part-*
Goodbye 1
Bye 1
Hadoop 2
World 2
Hello 2
```

## Comparison with Apache Hadoop and CLI
Madoop implements a subset of the [Hadoop Streaming](https://hadoop.apache.org/docs/r1.2.1/streaming.html) interface.  You can simulate the Hadoop Streaming interface at the command line with `cat` and `sort`.

Here's how to run our example MapReduce program on Apache Hadoop.
```console
$ hadoop \
    jar path/to/hadoop-streaming-X.Y.Z.jar
    -input example/input \
    -output output \
    -mapper example/map.py \
    -reducer example/reduce.py
$ cat output/part-*
```

Here's how to run our example MapReduce program at the command line using `cat` and `sort`.
```console
$ cat input/* | ./map.py | sort | ./reduce.py
```

| Madoop | Hadoop | `cat`/`sort` |
|-|-|-|
| Implement some Hadoop options | All Hadoop options | No Hadoop options |
| Multiple mappers and reducers | Multiple mappers and reducers | One mapper, one reducer |
| Single machine | Many machines | Single Machine |
| `jar hadoop-streaming-X.Y.Z.jar` argument ignored | `jar hadoop-streaming-X.Y.Z.jar` argument required | No arguments |
| Lines within a group are sorted | Lines within a group are sorted | Lines within a group are sorted |


## Contributing
Contributions from the community are welcome! Check out the [guide for contributing](CONTRIBUTING.md).


## Acknowledgments
Michigan Hadoop is written by Andrew DeOrio <awdeorio@umich.edu>.
