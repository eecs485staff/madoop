Madoop: Michigan Hadoop
=======================

Michigan Hadoop (`madoop`) is a light weight MapReduce framework intended for education.  Madoop implements the [Hadoop Streaming](https://hadoop.apache.org/docs/r1.2.1/streaming.html) interface.  Madoop is implemented in Python and runs on a single machine.

## Quick start
Install, run, check output.
```console
$ pip install madoop
$ madoop \
  -input example/input \
  -output example/output \
  -mapper example/map.py \
  -reducer example/reduce.py
$ cat example/output/part-*
autograder	2
world	1
eecs485	1
goodbye	1
hello	3
```


## Example
We'll walk through the example in the Quick Start again, providing more detail.

FIXME: link to https://eecs485staff.github.io/p5-search-engine/hadoop_streaming.html

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
$ cat input/input01.txt
Hello World
Bye World
$ cat input/input02.txt
Hello Hadoop
Goodbye Hadoop

### Run
Run the MapReduce job.  By default, there will be one mapper for each input file.  Large input files maybe segmented and processed by multiple mappers.
- `-input DIRECTORY` input directory
- `-output DIRECTORY` output directory
- `-mapper FILE` mapper executable
- `-reducer FILE` reducer executable

Run a MapReduce word count job.
```console
$ madoop \
    -input example/input \
    -output example/output \
    -mapper example/map.py \
    -reducer example/reduce.py
```

### Output
Concatenate and print output.
```console
$ ls example/output
part-00000  part-00001  part-00002  part-00003
$ cat example/output/part-*
autograder	2
world	1
eecs485	1
goodbye	1
hello	3
```


## Comparison with Apache Hadoop
Madoop implements a subset of the [Hadoop Streaming](https://hadoop.apache.org/docs/r1.2.1/streaming.html) interface.

### Similarities
- Command line options.  Madoop implements a subset of Hadoop's CLI.
- Support for multiple mappers and reducers.
- Input to reduce executions is sorted by line.

### Differences
- Madoop ignores the `jar hadoop/hadoop-streaming-2.7.2.jar` argument, if present
- Madoop runs on a single machine.  Apache Hadoop supports many machines.


## Contributing
Contributions from the community are welcome! Check out the [guide for contributing](CONTRIBUTING.md).

## Acknowledgments
Michigan Hadoop is written by Andrew DeOrio <awdeorio@umich.edu>.
