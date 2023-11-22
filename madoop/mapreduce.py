"""A light weight MapReduce framework for education.

Andrew DeOrio <awdeorio@umich.edu>

"""
import contextlib
import collections
import hashlib
import logging
import pathlib
import shutil
import subprocess
import tempfile
import multiprocessing
import concurrent.futures
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
        map_output_dir = tmpdir/'mapper-output'
        reduce_input_dir = tmpdir/'reducer-input'
        reduce_output_dir = tmpdir/'output'
        map_output_dir.mkdir()
        reduce_input_dir.mkdir()
        reduce_output_dir.mkdir()

        # Copy and rename input files: part-00000, part-00001, etc.
        input_path = pathlib.Path(input_path)

        # Executables must be absolute paths
        map_exe = pathlib.Path(map_exe).resolve()
        reduce_exe = pathlib.Path(reduce_exe).resolve()

        # Run the mapping stage
        LOGGER.info("Starting map stage")
        map_stage(
            exe=map_exe,
            input_dir=input_path,
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
            shutil.move(filename, output_dir)
            output_path = output_dir.parent/last_two(filename)
            LOGGER.debug("%s size=%sB", output_path, st_size)

    # Remind user where to find output
    LOGGER.debug("total output size=%sB", total_size)
    LOGGER.info("Output directory: %s", output_dir)


def split_file(input_filename, max_chunksize):
    """Iterate over the data in a file one chunk at a time."""
    with open(input_filename, "rb") as input_file:
        buffer = b""

        while True:
            chunk = input_file.read(max_chunksize)
            # Break if no more data remains.
            if not chunk:
                break

            # Add the chunk to the buffer.
            buffer += chunk

            # Find the last newline character in the buffer. We don't want to
            # yield a chunk that ends in the middle of a line; we have to
            # respect line boundaries or we'll corrupt the input.
            last_newline = buffer.rfind(b"\n")
            if last_newline != -1:
                # Yield the content up to the last newline, saving the rest
                # for the next chunk.
                yield buffer[:last_newline + 1]

                # Remove unprocessed data from the buffer. The next chunk will
                # start with whatever data came after the last newline.
                buffer = buffer[last_newline + 1:]

        # Yield any remaining data.
        if buffer:
            yield buffer


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


def map_single_chunk(exe, input_path, output_path, chunk):
    """Execute mapper on a single chunk."""
    with output_path.open("w") as outfile:
        try:
            subprocess.run(
                str(exe),
                shell=True,
                check=True,
                input=chunk,
                stdout=outfile,
            )
        except subprocess.CalledProcessError as err:
            raise MadoopError(
                f"Command returned non-zero: "
                f"{exe} < {input_path} > {output_path}"
            ) from err


def map_stage(exe, input_dir, output_dir):
    """Execute mappers."""
    part_num = 0
    futures = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=multiprocessing.cpu_count()
    ) as pool:
        for input_path in normalize_input_paths(input_dir):
            for chunk in split_file(input_path, MAX_INPUT_SPLIT_SIZE):
                output_path = output_dir/part_filename(part_num)
                LOGGER.debug(
                    "%s < %s > %s",
                    exe.name, last_two(input_path), last_two(output_path),
                )
                futures.append(pool.submit(
                    map_single_chunk,
                    exe,
                    input_path,
                    output_path,
                    chunk,
                ))
                part_num += 1
    concurrent.futures.wait(futures)
    LOGGER.info("Finished map executions: %s", part_num)


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
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        pool.map(sort_file, sorted(output_dir.iterdir()))

    # Log output keyspace stats
    all_output_keys = set()
    for outpath, keys in sorted(output_keys_stats.items()):
        all_output_keys.update(keys)
        LOGGER.debug("%s unique_keys=%s", last_two(outpath), len(keys))
    LOGGER.debug("%s all_unique_keys=%s", output_dir.name,
                 len(all_output_keys))


def reduce_single_file(exe, input_path, output_path):
    """Execute reducer on a single file."""
    with input_path.open() as infile, output_path.open("w") as outfile:
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


def reduce_stage(exe, input_dir, output_dir):
    """Execute reducers."""
    i = 0
    futures = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=multiprocessing.cpu_count()
    ) as pool:
        for i, input_path in enumerate(sorted(input_dir.iterdir())):
            output_path = output_dir/part_filename(i)
            LOGGER.debug(
                "%s < %s > %s",
                exe.name, last_two(input_path), last_two(output_path),
            )
            futures.append(pool.submit(
                reduce_single_file,
                exe,
                input_path,
                output_path,
            ))
    concurrent.futures.wait(futures)
    LOGGER.info("Finished reduce executions: %s", i+1)


def last_two(path):
    """Return the last two parts of path."""
    return pathlib.Path(*path.parts[-2:])
