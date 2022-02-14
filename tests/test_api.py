"""System tests for the API interface."""
import pytest
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


def test_bash_executable(tmpdir):
    """Run a MapReduce job written in Bash."""
    with tmpdir.as_cwd():
        madoop.mapreduce(
            input_dir=TESTDATA_DIR/"word_count/input",
            output_dir="output",
            map_exe=TESTDATA_DIR/"word_count/map.sh",
            reduce_exe=TESTDATA_DIR/"word_count/reduce.sh",
        )
    utils.assert_dirs_eq(
        TESTDATA_DIR/"word_count/correct/output",
        tmpdir/"output",
    )


def test_bad_map_exe(tmpdir):
    """Map exe returns non-zero should produce an error message."""
    with tmpdir.as_cwd(), pytest.raises(madoop.MadoopError):
        madoop.mapreduce(
            input_dir=TESTDATA_DIR/"word_count/input",
            output_dir="output",
            map_exe=TESTDATA_DIR/"word_count/map_invalid.py",
            reduce_exe=TESTDATA_DIR/"word_count/reduce.py",
        )


def test_missing_shebang(tmpdir):
    """Reduce exe with a bad shebag should produce an error message."""
    with tmpdir.as_cwd(), pytest.raises(madoop.MadoopError):
        madoop.mapreduce(
            input_dir=TESTDATA_DIR/"word_count/input",
            output_dir="output",
            map_exe=TESTDATA_DIR/"word_count/map.py",
            reduce_exe=TESTDATA_DIR/"word_count/reduce_invalid.py",
        )


def test_empty_inputs(tmpdir):
    """Empty input files should not raise an error."""
    with tmpdir.as_cwd():
        madoop.mapreduce(
            input_dir=TESTDATA_DIR/"word_count/input_empty",
            output_dir="output",
            map_exe=TESTDATA_DIR/"word_count/map.py",
            reduce_exe=TESTDATA_DIR/"word_count/reduce.py",
        )
    utils.assert_dirs_eq(
        TESTDATA_DIR/"word_count/correct/output",
        tmpdir/"output",
    )
