"""System tests for the API interface."""
import pathlib
import filecmp
import madoop


# Directory containing unit test input files, etc.
TESTDATA_DIR = pathlib.Path(__file__).parent/"testdata"


def test_simple(tmpdir):
    """Run a simple MapReduce job and verify the output."""
    with tmpdir.as_cwd():
        madoop.hadoop(
            input_dir=str(TESTDATA_DIR/"word_count/input"),
            output_dir="output",
            map_exe=str(TESTDATA_DIR/"word_count/map.py"),
            reduce_exe=str(TESTDATA_DIR/"word_count/reduce.py"),
        )
    for path in (TESTDATA_DIR/"word_count/correct").glob("part-*"):
        assert filecmp.cmp(
            path,
            TESTDATA_DIR/"word_count/correct"/path,
            shallow=False,
        )
