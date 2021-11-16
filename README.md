Madoop: Michigan Hadoop
=======================

Michigan Hadoop (`madoop`) is a light weight MapReduce framework intended for education.  Madoop implements the [Hadoop Streaming](https://hadoop.apache.org/docs/r1.2.1/streaming.html) interface.  Madoop is implemented in Python and runs on a single machine.

## Quick start
Install and run an example word count MapReduce program.
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
Concatenate and print output.  While each output file in our example is sorted, the concatenation of the three files may not be sorted.
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
Madoop implements a subset of the [Hadoop Streaming](https://hadoop.apache.org/docs/r1.2.1/streaming.html) interface.  You can simulate the Hadoop Streaming interface at the command line with `cat` and `sort`.

FIXME: should probably move this somewhere else
```console
$ cat input/* | ./map.py | sort | ./reduce.py
autograder	2
eecs485	1
goodbye	1
hello	3
world	1
```

| Madoop | Hadoop | `cat`/`sort` |
|-|-|-|
| Hadoop CLI subset | Hadoop CLI | No CLI |
| Multiple mappers and reducers | Multiple mappers and reducers | One mapper, one reducer |
| Single machine | Many machines | Single Machine |
| `jar hadoop/hadoop-streaming-2.7.2.jar` argument ignored | `jar hadoop/hadoop-streaming-2.7.2.jar` argument required | No arguments |
| Groups sorted | Groups sorted | Groups sorted |


## Contributing
Contributions from the community are welcome! Check out the [guide for contributing](CONTRIBUTING.md).


## Acknowledgments
Michigan Hadoop is written by Andrew DeOrio <awdeorio@umich.edu>.
