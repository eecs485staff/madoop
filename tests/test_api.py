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


def test_non_zero_return_code(tmpdir):
    """Run a MapReduce job that fails and verify the error.

    Run a MapReduce job on a mapper executable
    that returns a non-zero return code and verify
    the appropriate error message.

    """
    with tmpdir.as_cwd():
        try:
            madoop.mapreduce(
                input_dir=TESTDATA_DIR/"word_count/input",
                output_dir="output",
                map_exe=TESTDATA_DIR/"word_count/map_invalid.py",
                reduce_exe=TESTDATA_DIR/"word_count/reduce.py",
            )
        except madoop.MadoopError as err:
            assert "Failed is executable test" in str(err)


def test_missing_shebang(tmpdir):
    """Run a MapReduce job that fails and verify the error.

    Run a MapReduce job on a reducer executable
    that is missing a shebang and verify
    the appropriate error message.

    """
    with tmpdir.as_cwd():
        try:
            madoop.mapreduce(
                input_dir=TESTDATA_DIR/"word_count/input",
                output_dir="output",
                map_exe=TESTDATA_DIR/"word_count/map.py",
                reduce_exe=TESTDATA_DIR/"word_count/reduce_invalid.py",
            )
        except madoop.MadoopError as err:
            assert "Failed is executable test" in str(err)


def test_bash_executable(tmpdir):
    """Run a MapReduce job on bash executables and verify the output."""
    with tmpdir.as_cwd():
        madoop.mapreduce(
            input_dir=TESTDATA_DIR/"word_count/input",
            output_dir="output",
            map_exe=TESTDATA_DIR/"word_count/wc_map.sh",
            reduce_exe=TESTDATA_DIR/"word_count/wc_reduce.sh",
        )
    utils.assert_dirs_eq(
        TESTDATA_DIR/"word_count/correct/output_bash",
        tmpdir/"output",
    )
