"""A light weight MapReduce framework for education.

Andrew DeOrio <awdeorio@umich.edu>

"""
import contextlib
import collections
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
# but will not be more than num_reducers

# Madoop logger
LOGGER = logging.getLogger("madoop")


def mapreduce(input_path, output_dir, map_exe, reduce_exe, num_reducers):
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
        LOGGER.debug("tmpdir=%s", tmpdir)

        # Create stage input and output directory
        map_input_dir = tmpdir/'input'
        map_output_dir = tmpdir/'mapper-output'
        reduce_input_dir = tmpdir/'reducer-input'
        reduce_output_dir = tmpdir/'output'
        map_input_dir.mkdir()
        map_output_dir.mkdir()
        reduce_input_dir.mkdir()
        reduce_output_dir.mkdir()

        # Copy and rename input files: part-00000, part-00001, etc.
        input_path = pathlib.Path(input_path)
        prepare_input_files(input_path, map_input_dir)

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
            output_dir=reduce_input_dir,
            num_reducers=num_reducers
        )

        # Run the reducing stage
        LOGGER.info("Starting reduce stage")
        reduce_stage(
            exe=reduce_exe,
            input_dir=reduce_input_dir,
            output_dir=reduce_output_dir,
        )

        # Move files from temporary output dir to user-specified output dir
        total_size = 0
        for filename in sorted(reduce_output_dir.glob("*")):
            st_size = filename.stat().st_size
            total_size += st_size
            shutil.copy(filename, output_dir)
            output_path = output_dir.parent/last_two(filename)
            LOGGER.debug("%s size=%sB", output_path, st_size)

    # Remind user where to find output
    LOGGER.debug("total output size=%sB", total_size)
    LOGGER.info("Output directory: %s", output_dir)


def prepare_input_files(input_path, output_dir):
    """Copy and split input files.  Rename to part-00000, part-00001, etc.

    The input_path can be a file or a directory of files.  If a file is smaller
    than MAX_INPUT_SPLIT_SIZE, then copy it to output_dir.  For larger files,
    split into blocks of MAX_INPUT_SPLIT_SIZE bytes and write block to
    output_dir. Input files will never be combined.

    The number of files created will be the number of mappers since we will
    assume that the number of tasks per mapper is 1.  Apache Hadoop has a
    configurable number of tasks per mapper, however for both simplicity and
    because our use case has smaller inputs we use 1.

    """
    part_num = 0
    total_size = 0
    for inpath in normalize_input_paths(input_path):
        assert inpath.is_file()

        # Compute output filenames
        st_size = inpath.stat().st_size
        total_size += st_size
        n_splits = math.ceil(st_size / MAX_INPUT_SPLIT_SIZE)
        n_splits = 1 if not n_splits else n_splits  # Handle empty input file
        LOGGER.debug(
            "input %s size=%sB partitions=%s", inpath, st_size, n_splits
        )
        outpaths = [
            output_dir/part_filename(part_num + i) for i in range(n_splits)
        ]
        part_num += n_splits

        # Copy to new output files
        with contextlib.ExitStack() as stack:
            outfiles = [stack.enter_context(i.open('w')) for i in outpaths]
            infile = stack.enter_context(inpath.open(encoding="utf-8"))
            outparent = outpaths[0].parent
            assert all(i.parent == outparent for i in outpaths)
            outnames = [i.name for i in outpaths]
            logging.debug(
                "partition %s >> %s/{%s}",
                last_two(inpath), outparent.name, ",".join(outnames),
            )
            for i, line in enumerate(infile):
                outfiles[i % n_splits].write(line)
    LOGGER.debug("total input size=%sB", total_size)


def normalize_input_paths(input_path):
    """Return a list of filtered input files.

    If input_path is a file, then use it.  If input_path is a directory, then
    grab all the *files* inside.  Ignore subdirectories.

    """
    input_paths = []
    if input_path.is_dir():
        for path in sorted(input_path.glob('*')):
            if path.is_file():
                input_paths.append(path)
            else:
                LOGGER.warning("Ignoring non-file: %s", path)
    elif input_path.is_file():
        input_paths.append(input_path)
    assert input_paths, f"No input: {input_path}"
    return input_paths


def is_executable(exe):
    """Verify exe is executable and raise exception if it is not.

    Execute exe with an empty string input and verify that it returns zero.  We
    can't just check the executable bit because scripts with incorrect shebangs
    result in difficult-to-understand error messages.

    """
    exe = pathlib.Path(exe).resolve()
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
        raise MadoopError(f"Failed executable test: {err}") from err


def part_filename(num):
    """Return a string conforming to the output filename convention.

    EXAMPLE
    >>> part_filename(3)
    'part-00003'

    """
    return f"part-{num:05d}"


def map_stage(exe, input_dir, output_dir):
    """Execute mappers."""
    i = 0
    for i, input_path in enumerate(sorted(input_dir.iterdir()), 1):
        output_path = output_dir/part_filename(i)
        LOGGER.debug(
            "%s < %s > %s",
            exe.name, last_two(input_path), last_two(output_path),
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
    LOGGER.info("Finished map executions: %s", i)


def sort_file(path):
    """Sort contents of path, overwriting it."""
    LOGGER.debug("sort %s", last_two(path))
    with path.open() as infile:
        sorted_lines = sorted(infile)
    with path.open("w") as outfile:
        outfile.writelines(sorted_lines)


def keyhash(key):
    """Hash key and return an integer."""
    hexdigest = hashlib.md5(key.encode("utf-8")).hexdigest()
    return int(hexdigest, base=16)


def partition_keys(
        inpath,
        outpaths,
        input_keys_stats,
        output_keys_stats,
        num_reducers):
    """Allocate lines of inpath among outpaths using hash of key.

    Update the data structures provided by the caller input_keys_stats and
    output_keys_stats.  Both map a filename to a set of of keys.

    """
    assert len(outpaths) == num_reducers
    outparent = outpaths[0].parent
    assert all(i.parent == outparent for i in outpaths)
    with contextlib.ExitStack() as stack:
        outfiles = [stack.enter_context(p.open("a")) for p in outpaths]
        for line in stack.enter_context(inpath.open()):
            key = line.partition('\t')[0]
            input_keys_stats[inpath].add(key)
            reducer_idx = keyhash(key) % num_reducers
            outfiles[reducer_idx].write(line)
            outpath = outpaths[reducer_idx]
            output_keys_stats[outpath].add(key)


def group_stage(input_dir, output_dir, num_reducers):
    """Run group stage.

    Process each mapper output file, allocating lines to grouper output files
    using the hash and modulo of the key.

    """
    # Compute output filenames
    LOGGER.debug("%s reducers", num_reducers)
    outpaths = []
    for i in range(num_reducers):
        outpaths.append(output_dir/part_filename(i))

    # Track keyspace stats, map filename -> set of keys
    input_keys_stats = collections.defaultdict(set)
    output_keys_stats = collections.defaultdict(set)

    # Partition input, appending to output files
    for inpath in sorted(input_dir.iterdir()):
        partition_keys(inpath, outpaths, input_keys_stats,
                       output_keys_stats, num_reducers)

    # Log input keyspace stats
    all_input_keys = set()
    for inpath, keys in sorted(input_keys_stats.items()):
        all_input_keys.update(keys)
        LOGGER.debug("%s unique_keys=%s", last_two(inpath), len(keys))
    LOGGER.debug("%s all_unique_keys=%s", input_dir.name, len(all_input_keys))

    # Log partition input and output filenames
    outnames = [i.name for i in outpaths]
    outparent = outpaths[0].parent
    for inpath in sorted(input_keys_stats.keys()):
        LOGGER.debug(
            "partition %s >> %s/{%s}",
            last_two(inpath), outparent.name, ",".join(outnames),
        )

    # Remove empty output files.  We won't always use the maximum number of
    # reducers because some MapReduce programs have fewer intermediate keys.
    for path in sorted(output_dir.iterdir()):
        if path.stat().st_size == 0:
            LOGGER.debug("empty partition: rm %s", last_two(path))
            path.unlink()

    # Sort output files
    for path in sorted(output_dir.iterdir()):
        sort_file(path)

    # Log output keyspace stats
    all_output_keys = set()
    for outpath, keys in sorted(output_keys_stats.items()):
        all_output_keys.update(keys)
        LOGGER.debug("%s unique_keys=%s", last_two(outpath), len(keys))
    LOGGER.debug("%s all_unique_keys=%s", output_dir.name,
                 len(all_output_keys))


def reduce_stage(exe, input_dir, output_dir):
    """Execute reducers."""
    i = 0
    for i, input_path in enumerate(sorted(input_dir.iterdir())):
        output_path = output_dir/part_filename(i)
        LOGGER.debug(
            "%s < %s > %s",
            exe.name, last_two(input_path), last_two(output_path),
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
    LOGGER.info("Finished reduce executions: %s", i+1)


def last_two(path):
    """Return the last two parts of path."""
    return pathlib.Path(*path.parts[-2:])
