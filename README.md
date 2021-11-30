Madoop: Michigan Hadoop
=======================

Michigan Hadoop (`madoop`) is a light weight MapReduce framework for education.  Madoop implements the [Hadoop Streaming](https://hadoop.apache.org/docs/r1.2.1/streaming.html) interface.  Madoop is implemented in Python and runs on a single machine.

## Quick start
Install and run an example word count MapReduce program.
```console
$ pip install madoop
$ madoop \
  -input example/input \
  -output output \
  -mapper example/map.py \
  -reducer example/reduce.py
$ cat output/part-*
autograder	2
world	1
eecs485	1
goodbye	1
hello	3
```


## Example
We'll walk through the example in the Quick Start again, providing more detail.  For an in-depth explanation of the map and reduce code, see the [Hadoop Streaming tutorial](https://eecs485staff.github.io/p5-search-engine/hadoop_streaming.html).

## Install
Install Madoop.  Your version might be different.
```console
$ pip install madoop
$ madoop --version
Madoop 0.1.0
```

### Input
We've provided two small input files.
```console
$ cat example/input/input01.txt
hello world
hello eecs485
$ cat example/input/input02.txt
goodbye autograder
hello autograder
```

### Run
Run a MapReduce word count job.  By default, there will be one mapper for each input file.  Large input files maybe segmented and processed by multiple mappers.
- `-input DIRECTORY` input directory
- `-output DIRECTORY` output directory
- `-mapper FILE` mapper executable
- `-reducer FILE` reducer executable
```console
$ madoop \
    -input example/input \
    -output output \
    -mapper example/map.py \
    -reducer example/reduce.py
```

### Output
Concatenate and print output.  The concatenation of multiple output files may not be sorted.
```console
$ ls output
part-00000  part-00001  part-00002  part-00003
$ cat output/part-*
autograder	2
world	1
eecs485	1
goodbye	1
hello	3
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
