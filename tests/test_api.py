"""System tests for the API interface."""
from pathlib import Path
import pytest
import madoop
from madoop.mapreduce import map_stage, reduce_stage
from . import utils
from .utils import TESTDATA_DIR


def test_simple(tmpdir):
    """Run a simple MapReduce job and verify the output."""
    with tmpdir.as_cwd():
        madoop.mapreduce(
            input_path=TESTDATA_DIR/"word_count/input",
            output_dir="output",
            map_exe=TESTDATA_DIR/"word_count/map.py",
            reduce_exe=TESTDATA_DIR/"word_count/reduce.py",
            num_reducers=4,
            partitioner=None,
        )
    utils.assert_dirs_eq(
        TESTDATA_DIR/"word_count/correct/output",
        tmpdir/"output",
    )


def test_2_reducers(tmpdir):
    """Run a simple MapReduce job with 2 reducers."""
    with tmpdir.as_cwd():
        madoop.mapreduce(
            input_path=TESTDATA_DIR/"word_count/input",
            output_dir="output",
            map_exe=TESTDATA_DIR/"word_count/map.py",
            reduce_exe=TESTDATA_DIR/"word_count/reduce.py",
            num_reducers=2,
            partitioner=None,
        )
    utils.assert_dirs_eq(
        TESTDATA_DIR/"word_count/correct/output-2-reducers",
        tmpdir/"output",
    )


def test_bash_executable(tmpdir):
    """Run a MapReduce job written in Bash."""
    with tmpdir.as_cwd():
        madoop.mapreduce(
            input_path=TESTDATA_DIR/"word_count/input",
            output_dir="output",
            map_exe=TESTDATA_DIR/"word_count/map.sh",
            reduce_exe=TESTDATA_DIR/"word_count/reduce.sh",
            num_reducers=4,
            partitioner=None,
        )
    utils.assert_dirs_eq(
        TESTDATA_DIR/"word_count/correct/output",
        tmpdir/"output",
    )


def test_output_already_exists(tmpdir):
    """Output already existing should raise an error."""
    with tmpdir.as_cwd(), pytest.raises(madoop.MadoopError):
        madoop.mapreduce(
            input_path=TESTDATA_DIR/"word_count/input",
            output_dir=tmpdir,
            map_exe=TESTDATA_DIR/"word_count/map.py",
            reduce_exe=TESTDATA_DIR/"word_count/reduce.py",
            num_reducers=2,
        )


def test_bad_map_exe(tmpdir):
    """Map exe returns non-zero should produce an error message."""
    with tmpdir.as_cwd(), pytest.raises(madoop.MadoopError):
        madoop.mapreduce(
            input_path=TESTDATA_DIR/"word_count/input",
            output_dir="output",
            map_exe=TESTDATA_DIR/"word_count/map_invalid.py",
            reduce_exe=TESTDATA_DIR/"word_count/reduce.py",
            num_reducers=4,
            partitioner=None,
        )


def test_bad_partition_exe(tmpdir):
    """Partition exe returns non-zero should produce an error message."""
    with tmpdir.as_cwd(), pytest.raises(madoop.MadoopError):
        madoop.mapreduce(
            input_path=TESTDATA_DIR/"word_count/input",
            output_dir="output",
            map_exe=TESTDATA_DIR/"word_count/map.py",
            reduce_exe=TESTDATA_DIR/"word_count/reduce.py",
            num_reducers=4,
            partitioner=TESTDATA_DIR/"word_count/partition_invalid.py",
        )


def test_noninteger_partition_exe(tmpdir):
    """Partition exe prints non-integer should produce an error message."""
    with tmpdir.as_cwd(), pytest.raises(madoop.MadoopError):
        madoop.mapreduce(
            input_path=TESTDATA_DIR/"word_count/input",
            output_dir="output",
            map_exe=TESTDATA_DIR/"word_count/map.py",
            reduce_exe=TESTDATA_DIR/"word_count/reduce.py",
            num_reducers=4,
            partitioner=TESTDATA_DIR/"word_count/partition_noninteger.py",
        )

    with tmpdir.as_cwd(), pytest.raises(madoop.MadoopError):
        map_stage(
            exe=TESTDATA_DIR/"word_count/map_invalid.py",
            input_dir=TESTDATA_DIR/"word_count/input",
            output_dir=Path(tmpdir),
        )


def test_bad_reduce_exe(tmpdir):
    """Reduce exe returns non-zero should produce an error message."""
    with tmpdir.as_cwd(), pytest.raises(madoop.MadoopError):
        reduce_stage(
            exe=TESTDATA_DIR/"word_count/reduce_exit_1.py",
            input_dir=TESTDATA_DIR/"word_count/input",
            output_dir=Path(tmpdir),
        )


def test_missing_shebang(tmpdir):
    """Reduce exe with a bad shebag should produce an error message."""
    with tmpdir.as_cwd(), pytest.raises(madoop.MadoopError):
        madoop.mapreduce(
            input_path=TESTDATA_DIR/"word_count/input",
            output_dir="output",
            map_exe=TESTDATA_DIR/"word_count/map.py",
            reduce_exe=TESTDATA_DIR/"word_count/reduce_invalid.py",
            num_reducers=4,
            partitioner=None,
        )


def test_empty_inputs(tmpdir):
    """Empty input files should not raise an error."""
    with tmpdir.as_cwd():
        madoop.mapreduce(
            input_path=TESTDATA_DIR/"word_count/input_empty",
            output_dir="output",
            map_exe=TESTDATA_DIR/"word_count/map.py",
            reduce_exe=TESTDATA_DIR/"word_count/reduce.py",
            num_reducers=4,
            partitioner=None,
        )
    utils.assert_dirs_eq(
        TESTDATA_DIR/"word_count/correct/output",
        tmpdir/"output",
    )


def test_single_input_file(tmpdir):
    """Run a simple MapReduce job with an input file instead of dir."""
    with tmpdir.as_cwd():
        madoop.mapreduce(
            input_path=TESTDATA_DIR/"word_count/input-single-file.txt",
            output_dir="output",
            map_exe=TESTDATA_DIR/"word_count/map.py",
            reduce_exe=TESTDATA_DIR/"word_count/reduce.py",
            num_reducers=4,
            partitioner=None,
        )
    utils.assert_dirs_eq(
        TESTDATA_DIR/"word_count/correct/output",
        tmpdir/"output",
    )


def test_ignores_subdirs(tmpdir):
    """Run a simple MapReduce job with an input directory containing a
    subdirectory. The subdirectory should be gracefully ignored.
    """
    with tmpdir.as_cwd():
        madoop.mapreduce(
            input_path=TESTDATA_DIR/"word_count/input_with_subdir",
            output_dir="output",
            map_exe=TESTDATA_DIR/"word_count/map.py",
            reduce_exe=TESTDATA_DIR/"word_count/reduce.py",
            num_reducers=4,
            partitioner=None,
        )
    utils.assert_dirs_eq(
        TESTDATA_DIR/"word_count/correct/output",
        tmpdir/"output",
    )


def test_input_path_spaces(tmpdir):
    """Run a simple MapReduce job with an input directory containing a
    subdirectory. The subdirectory should be gracefully ignored.
    """
    with tmpdir.as_cwd():
        madoop.mapreduce(
            input_path=TESTDATA_DIR/"word_count SPACE/input SPACE",
            output_dir="output",
            map_exe=TESTDATA_DIR/"word_count SPACE/map SPACE.py",
            reduce_exe=TESTDATA_DIR/"word_count SPACE/reduce SPACE.py",
            num_reducers=4
        )
    utils.assert_dirs_eq(
        TESTDATA_DIR/"word_count/correct/output",
        tmpdir/"output",
    )
