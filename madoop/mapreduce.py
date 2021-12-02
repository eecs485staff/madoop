"""A light weight MapReduce framework for education.

Andrew DeOrio <awdeorio@umich.edu>

"""
import collections
import contextlib
import math
import pathlib
import shutil
import subprocess
import tempfile
from .exceptions import MadoopError


# Large input files are automatically split
MAX_INPUT_SPLIT_SIZE = 2**20  # 1 MB

# The number of reducers is dynamically determined by the number of unique keys
# but will not be more than MAX_NUM_REDUCE
MAX_NUM_REDUCE = 4


def mapreduce(input_dir, output_dir, map_exe, reduce_exe):
    """Madoop API."""
    # Do not clobber existing output directory
    output_dir = pathlib.Path(output_dir)
    if output_dir.exists():
        raise MadoopError(f"Output directory already exists: {output_dir}")
    output_dir.mkdir()

    # Executable scripts must have valid shebangs
    is_executable(map_exe)
    is_executable(reduce_exe)

    # Create a tmp directory which will be automatically cleaned up
    with tempfile.TemporaryDirectory(prefix="madoop-") as tmpdir:
        tmpdir = pathlib.Path(tmpdir)

        # Create stage input and output directory
        map_input_dir = tmpdir/'input'
        map_output_dir = tmpdir/'mapper-output'
        group_output_dir = tmpdir/'grouper-output'
        reduce_output_dir = tmpdir/'reducer-output'
        map_input_dir.mkdir()
        map_output_dir.mkdir()
        group_output_dir.mkdir()
        reduce_output_dir.mkdir()

        # Copy and rename input files: part-00000, part-00001, etc.
        input_dir = pathlib.Path(input_dir)
        prepare_input_files(input_dir, map_input_dir)

        # Executables must be absolute paths
        map_exe = pathlib.Path(map_exe).resolve()
        reduce_exe = pathlib.Path(reduce_exe).resolve()

        # Run the mapping stage
        print("Starting map stage")
        map_stage(
            exe=map_exe,
            input_dir=map_input_dir,
            output_dir=map_output_dir,
        )

        # Run the grouping stage
        print("Starting group stage")
        group_stage(
            input_dir=map_output_dir,
            output_dir=group_output_dir,
        )

        # Run the reducing stage
        print("Starting reduce stage")
        reduce_stage(
            exe=reduce_exe,
            input_dir=group_output_dir,
            output_dir=reduce_output_dir,
        )

        # Move files from temporary output dir to user-specified output dir
        for filename in reduce_output_dir.glob("*"):
            shutil.copy(filename, output_dir)

    # Remind user where to find output
    print(f"Output directory: {output_dir}")


def prepare_input_files(input_dir, output_dir):
    """Copy and split input files.  Rename to part-00000, part-00001, etc.

    If a file in input_dir is smaller than MAX_INPUT_SPLIT_SIZE, then copy it
    to output_dir.  For larger files, split into blocks of MAX_INPUT_SPLIT_SIZE
    bytes and write block to output_dir. Input files will never be combined.

    The number of files created will be the number of mappers since we will
    assume that the number of tasks per mapper is 1.  Apache Hadoop has a
    configurable number of tasks per mapper, however for both simplicity and
    because our use case has smaller inputs we use 1.

    """
    assert input_dir.is_dir(), f"Can't find input_dir '{input_dir}'"

    # Input filenames
    inpaths = list(input_dir.glob('*'))
    assert all(i.is_file() for i in inpaths)

    # Split and copy input files
    part_num = 0
    for inpath in inpaths:
        # Compute output filenames
        num_splits = math.ceil(inpath.stat().st_size / MAX_INPUT_SPLIT_SIZE)
        outpaths = [
            output_dir/part_filename(part_num + i) for i in range(num_splits)
        ]
        part_num += num_splits

        # Copy to new output files
        with contextlib.ExitStack() as stack:
            outfiles = [stack.enter_context(i.open('w')) for i in outpaths]
            infile = stack.enter_context(inpath.open(encoding="utf-8"))
            for i, line in enumerate(infile):
                outfiles[i % num_splits].write(line)


def is_executable(exe):
    """Verify exe is executable and raise exception if it is not.

    Execute exe with an empty string input and verify that it returns zero.  We
    can't just check the executable bit because scripts with incorrect shebangs
    result in difficult-to-under error messages.

    """
    try:
        subprocess.run(
            str(exe),
            shell=True,
            input="".encode(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
    except subprocess.CalledProcessError as err:
        raise MadoopError(f"Failed is executable test: {exe} {err}") from err


def part_filename(num):
    """Return a string conforming to the output filename convention.

    EXAMPLE
    >>> part_filename(3)
    'part-00003'

    """
    return f"part-{num:05d}"


def map_stage(exe, input_dir, output_dir):
    """Execute mappers."""
    for i, input_path in enumerate(sorted(input_dir.iterdir())):
        output_path = output_dir/part_filename(i)
        print(f"+ {exe.name} < {input_path} > {output_path}")
        with input_path.open() as infile, output_path.open('w') as outfile:
            try:
                subprocess.run(
                    str(exe),
                    shell=True,
                    check=True,
                    stdin=infile,
                    stdout=outfile,
                )
            except subprocess.CalledProcessError as err:
                raise MadoopError(
                    f"Command returned non-zero: "
                    f"{exe} < {input_path} > {output_path}"
                ) from err


def group_stage_cat_sort(input_dir, sorted_output_filename):
    """Concatenate and sort input files, saving to 'sorted_ouput_filename'.

    Set the locale with the LC_ALL environment variable to force an ASCII
    sort order.
    """
    input_filenames = input_dir.glob("*")
    with sorted_output_filename.open('w') as outfile:
        with subprocess.Popen(
            ["cat", *input_filenames],
            stdout=subprocess.PIPE,
            env={'LC_ALL': 'C.UTF-8'},
        ) as cat_proc, \
            subprocess.Popen(
                ["sort"],
                stdin=cat_proc.stdout,
                stdout=outfile,
                env={'LC_ALL': 'C.UTF-8'},
        ) as sort_proc:
            cat_proc.wait()
            sort_proc.wait()
    assert cat_proc.returncode == 0
    assert sort_proc.returncode == 0


def group_stage(input_dir, output_dir):
    """Run group stage.

    Concatenate and sort input files to 'sorted.out'. Determine the number of
    reducers and split 'sorted.out' into that many files.

    """
    sorted_output_filename = output_dir/'sorted.out'
    print(f"+ cat {input_dir}/* | sort > {sorted_output_filename}")

    # Concatenate and sort
    group_stage_cat_sort(input_dir, sorted_output_filename)

    # Write lines to grouper output files.  Round robin allocation by key.
    with contextlib.ExitStack() as stack:
        grouper_files = collections.deque(maxlen=MAX_NUM_REDUCE)
        sorted_output_file = stack.enter_context(sorted_output_filename.open())
        prev_key = None
        for lineno, line in enumerate(sorted_output_file):
            # Parse the line.  Must be two strings separated by a tab.
            assert '\t' in line, \
                f"Missing TAB {sorted_output_filename}:{lineno}"
            key, _ = line.split('\t', maxsplit=1)

            # If it's a new key, ...
            if key != prev_key:
                # Update prev_key
                prev_key = key

                # If using less than the maximum number of reducers, create and
                # open a new grouper output file.
                num_grouper_files = len(grouper_files)
                if num_grouper_files < MAX_NUM_REDUCE:
                    filename = output_dir/part_filename(num_grouper_files)
                    file = filename.open('w')
                    grouper_files.append(stack.enter_context(file))

                # Rotate circular queue of grouper files
                grouper_files.rotate(1)

            # Write to grouper output file
            grouper_files[0].write(line)


def reduce_stage(exe, input_dir, output_dir):
    """Execute reducers."""
    input_files = [i for i in input_dir.iterdir() if i.name != "sorted.out"]
    for i, input_path in enumerate(sorted(input_files)):
        output_path = output_dir/part_filename(i)
        print(f"+ {exe.name} < {input_path} > {output_path}")
        with input_path.open() as infile, output_path.open('w') as outfile:
            try:
                subprocess.run(
                    str(exe),
                    shell=True,
                    check=True,
                    stdin=infile,
                    stdout=outfile,
                )
            except subprocess.CalledProcessError as err:
                raise MadoopError(
                    f"Command returned non-zero: "
                    f"{exe} < {input_path} > {output_path}"
                ) from err
