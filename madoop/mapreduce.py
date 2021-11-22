"""A light weight MapReduce framework for education.

Andrew DeOrio <awdeorio@umich.edu>

"""
import collections
import shutil
import pathlib
import subprocess
import math
import contextlib
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
    check_shebang(map_exe)
    check_shebang(reduce_exe)

    # Create a tmp directory which will be automatically cleaned up
    with tempfile.TemporaryDirectory(prefix="madoop-") as tmpdir:
        tmpdir = pathlib.Path(tmpdir)

        # Create stage input and output directory
        map_input_dir = tmpdir/'mapper-input'
        map_output_dir = tmpdir/'mapper-output'
        group_output_dir = tmpdir/'grouper-output'
        reduce_output_dir = tmpdir/'reducer-output'
        map_input_dir.mkdir()
        map_output_dir.mkdir()
        group_output_dir.mkdir()
        reduce_output_dir.mkdir()

        # Copy and rename input files: part-00000, part-00001, etc.
        input_dir = pathlib.Path(input_dir)
        num_map = prepare_input_files(input_dir, map_input_dir)

        # Executables must be absolute paths
        map_exe = pathlib.Path(map_exe).resolve()
        reduce_exe = pathlib.Path(reduce_exe).resolve()

        # Run the mapping stage
        print("Starting map stage")
        map_stage(
            exe=map_exe,
            input_dir=map_input_dir,
            output_dir=map_output_dir,
            num_map=num_map,
        )

        # Run the grouping stage
        print("Starting group stage")
        num_reduce = group_stage(
            input_dir=map_output_dir,
            output_dir=group_output_dir,
        )

        # Run the reducing stage
        print("Starting reduce stage")
        reduce_stage(
            exe=reduce_exe,
            input_dir=group_output_dir,
            output_dir=reduce_output_dir,
            num_reduce=num_reduce,
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

    Return the number of files created. This will be the number of mappers
    since we will assume that the number of tasks per mapper is 1.  Apache
    Hadoop has a configurable number of tasks per mapper, however for both
    simplicity and because our use case has smaller inputs we use 1.

    """
    assert input_dir.is_dir(), f"Can't find input_dir '{input_dir}'"

    # Count input files
    filenames = []
    for filename in input_dir.glob('*'):
        if not filename.is_dir():
            filenames.append(filename)

    # Copy and rename input files
    part_num = 0
    for filename in filenames:
        # Calculate the number of splits
        in_file = pathlib.Path(filename)
        num_split = math.ceil(in_file.stat().st_size / MAX_INPUT_SPLIT_SIZE)

        # create num_split output files
        out_filenames = [
            output_dir/part_filename(part_num + i) for i in range(num_split)]
        part_num += num_split

        # copy to new files
        with in_file.open(encoding="utf-8") as file:
            with contextlib.ExitStack() as stack:
                out_files = [
                    stack.enter_context(file2.open('w'))
                    for file2 in out_filenames]
                for i, line in enumerate(file):
                    out_files[i % num_split].write(line)

    return part_num


def check_num_keys(filename):
    """Check num keys."""
    key_instances = 0
    with open(filename, encoding="utf-8") as file:
        for _ in file:
            key_instances += 1

    # implies we are dumping everything into one key
    if key_instances == 1:
        raise MadoopError('Single key detected')


def check_shebang(exe):
    """Verify correct exe starts with '#!/usr/bin/env python3'.

    We need to verify the shebang manually because subprocess.run() throws
    confusing errors when it tries to execute a script with an error in the
    shebang.

    """
    exe = pathlib.Path(exe)
    with exe.open(encoding="utf-8") as infile:
        line = infile.readline().rstrip()
    if line != "#!/usr/bin/env python3":
        raise MadoopError(
            f"{exe}: invalid shebang on first line '{line}'.  "
            "Expected '#!/usr/bin/env python3'"
        )


def part_filename(num):
    """Return a string conforming to the output filename convention.

    EXAMPLE
    >>> part_filename(3)
    'part-00003'

    """
    return f"part-{num:05d}"


def map_stage(exe, input_dir, output_dir, num_map):
    """Execute mappers."""
    for i in range(num_map):
        input_path = input_dir/part_filename(i)
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
    with open(sorted_output_filename, 'w', encoding='utf-8') as outfile:
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

    Return the number of reducers to be used in the reduce stage.

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

    # Number of grouper output files = number of reducers
    return len(grouper_files)


def reduce_stage(exe, input_dir, output_dir, num_reduce):
    """Execute reducers."""
    for i in range(num_reduce):
        input_path = input_dir/part_filename(i)
        output_path = output_dir/part_filename(i)
        print(f"+ {exe.name} < {input_path} > {output_path}")
        with open(input_path, encoding="utf-8") as infile, \
             open(output_path, 'w', encoding="utf-8") as outfile:
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
