"""A light weight MapReduce framework for education.

Andrew DeOrio <awdeorio@umich.edu>

"""
import contextlib
import hashlib
import logging
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

# Madoop logger
LOGGER = logging.getLogger(__name__)


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
        LOGGER.debug("tmpdir=%s", tmpdir)

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
        LOGGER.info("Starting map stage")
        map_stage(
            exe=map_exe,
            input_dir=map_input_dir,
            output_dir=map_output_dir,
        )

        # Run the grouping stage
        LOGGER.info("Starting group stage")
        group_stage(
            input_dir=map_output_dir,
            output_dir=group_output_dir,
        )

        # Run the reducing stage
        LOGGER.info("Starting reduce stage")
        reduce_stage(
            exe=reduce_exe,
            input_dir=group_output_dir,
            output_dir=reduce_output_dir,
        )

        # Move files from temporary output dir to user-specified output dir
        for filename in reduce_output_dir.glob("*"):
            shutil.copy(filename, output_dir)

    # Remind user where to find output
    LOGGER.info("Output directory: %s", output_dir)


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


def map_stage(exe, input_dir, output_dir):
    """Execute mappers."""
    for i, input_path in enumerate(sorted(input_dir.iterdir())):
        output_path = output_dir/part_filename(i)
        LOGGER.debug(
            "%s < %s > %s",
            exe.name, input_path.name, output_path.name
        )
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


def sort_file(path):
    """Sort contents of path, overwriting it."""
    with path.open() as infile:
        sorted_lines = sorted(infile)
    with path.open("w") as outfile:
        outfile.writelines(sorted_lines)


def keyhash(key):
    """Hash key and return an integer."""
    hexdigest = hashlib.md5(key.encode("utf-8")).hexdigest()
    return int(hexdigest, base=16)


def partition_keys(inpath, outpaths):
    """Allocate lines of inpath among outpaths using hash of key."""
    assert len(outpaths) == MAX_NUM_REDUCE
    with contextlib.ExitStack() as stack:
        outfiles = [stack.enter_context(p.open("a")) for p in outpaths]
        for line in stack.enter_context(inpath.open()):
            key = line.partition('\t')[0]
            reducer_idx = keyhash(key) % MAX_NUM_REDUCE
            outfiles[reducer_idx].write(line)


def group_stage(input_dir, output_dir):
    """Run group stage.

    Process each mapper output file, allocating lines to grouper output files
    using the hash and modulo of the key.

    """
    # Compute output filenames
    outpaths = []
    for i in range(MAX_NUM_REDUCE):
        outpaths.append(output_dir/part_filename(i))

    # Parition input, appending to output files
    for inpath in sorted(input_dir.iterdir()):
        LOGGER.debug(
            "partition %s -> %s",
            inpath.name, [i.name for i in outpaths],
        )
        partition_keys(inpath, outpaths)

    # Sort output files
    for path in sorted(output_dir.iterdir()):
        LOGGER.debug("sort %s", path.name)
        sort_file(path)

    # Remove empty output files.  We won't always use the maximum number of
    # reducers because some MapReduce programs have fewer intermediate keys.
    for path in output_dir.iterdir():
        if path.stat().st_size == 0:
            path.unlink()


def reduce_stage(exe, input_dir, output_dir):
    """Execute reducers."""
    for i, input_path in enumerate(sorted(input_dir.iterdir())):
        output_path = output_dir/part_filename(i)
        LOGGER.debug(
            "%s < %s > %s",
            exe.name, input_path.name, output_path.name
        )
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
