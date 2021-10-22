"""System tests for the API interface."""
import filecmp
from utils import TESTDATA_DIR
import hadoop


def test_simple(tmpdir):
    """Run a simple MapReduce job and verify the output."""
    with tmpdir.as_cwd():
        madoop.hadoop(
            input_dir=str(TESTDATA_DIR/"word_count/input"),
            output_dir="output",
            map_exe=str(TESTDATA_DIR/"word_count/map.py"),
            reduce_exe=str(TESTDATA_DIR/"word_count/reduce.py"),
        )
    correct = TESTDATA_DIR/"word_count/correct"
    for basename in correct.listdir():
        assert filecmp.cmp(
            tmpdir/"output"/basename,
            TESTDATA_DIR/"word_count/correct"/basename,
            shallow=False,
        )
