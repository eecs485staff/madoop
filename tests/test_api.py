"""System tests for the API interface."""
import pathlib
import filecmp
import madoop


# Directory containing unit test input files, etc.
TESTDATA_DIR = pathlib.Path(__file__).parent/"testdata"


def test_simple(tmpdir):
    """Run a simple MapReduce job and verify the output."""
    with tmpdir.as_cwd():
        madoop.mapreduce(
            input_dir=str(TESTDATA_DIR/"word_count/input"),
            output_dir="output",
            map_exe=str(TESTDATA_DIR/"word_count/map.py"),
            reduce_exe=str(TESTDATA_DIR/"word_count/reduce.py"),
        )

    for correct in (TESTDATA_DIR/"word_count/correct/output").glob("part-*"):
        actual = tmpdir/"output"/correct.name
        assert filecmp.cmp(correct, actual, shallow=False)
