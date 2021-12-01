"""Unit test utilities."""
import filecmp
import pathlib


# Directory containing unit test input files, etc.
TESTDATA_DIR = pathlib.Path(__file__).parent/"testdata"


def assert_dirs_eq(dir1, dir2):
    """Compare two directories of files."""
    assert dir1 != dir2, (
        "Refusing to compare a directory to itself:\n"
        f"dir1 = {dir1}\n"
        f"dir2 = {dir2}\n"
    )

    # Get a list of files in each directory
    dir1 = pathlib.Path(dir1)
    dir2 = pathlib.Path(dir2)
    paths1 = list(dir1.iterdir())
    paths2 = list(dir2.iterdir())

    # Sanity checks
    assert paths1, f"Empty directory: {dir1}"
    assert paths2, f"Empty directory: {dir2}"
    assert all(p.is_file() for p in paths1)
    assert all(p.is_file() for p in paths2)
    assert len(paths1) == len(paths2), (
        "Number of output files does not match:\n"
        f"dir1 = {dir1}\n"
        f"dir2 = {dir2}\n"
        f"number of files in dir1 = {len(paths1)}\n"
        f"number of files in dir2 = {len(paths2)}\n"
    )

    # Compare files pairwise
    for path1, path2 in zip(sorted(paths1), sorted(paths2)):
        assert filecmp.cmp(path1, path2, shallow=False), (
            "Files do not match:\n"
            f"path1 = {path1}\n"
            f"path2 = {path2}\n"
        )
