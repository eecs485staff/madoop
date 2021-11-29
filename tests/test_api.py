"""System tests for the API interface."""
import madoop
from . import utils
from .utils import TESTDATA_DIR


def test_simple(tmpdir):
    """Run a simple MapReduce job and verify the output."""
    with tmpdir.as_cwd():
        madoop.mapreduce(
            input_dir=TESTDATA_DIR/"word_count/input",
            output_dir="output",
            map_exe=TESTDATA_DIR/"word_count/map.py",
            reduce_exe=TESTDATA_DIR/"word_count/reduce.py",
        )
    utils.assert_dirs_eq(
        TESTDATA_DIR/"word_count/correct/output",
        tmpdir/"output",
    )
