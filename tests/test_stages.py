"""System tests for the map stage of Michigan Hadoop."""
from madoop.mapreduce import map_stage, group_stage, reduce_stage
from . import utils
from .utils import TESTDATA_DIR


def test_map_stage(tmpdir):
    """Test the map stage using word count example."""
    map_stage(
        exe=TESTDATA_DIR/"word_count/map.py",
        input_dir=TESTDATA_DIR/"word_count/correct/mapper-input",
        output_dir=tmpdir,
        num_map=2,
    )
    utils.assert_dirs_eq(
        TESTDATA_DIR/"word_count/correct/mapper-output",
        tmpdir,
    )


def test_group_stage(tmpdir):
    """Test group stage using word count example."""
    group_stage(
        input_dir=TESTDATA_DIR/"word_count/correct/mapper-output",
        output_dir=tmpdir,
    )
    utils.assert_dirs_eq(
        TESTDATA_DIR/"word_count/correct/grouper-output",
        tmpdir,
    )


def test_reduce_stage(tmpdir):
    """Test reduce stage using word count example."""
    reduce_stage(
        exe=TESTDATA_DIR/"word_count/reduce.py",
        input_dir=TESTDATA_DIR/"word_count/correct/grouper-output",
        output_dir=tmpdir,
        num_reduce=4,
    )
    utils.assert_dirs_eq(
        TESTDATA_DIR/"word_count/correct/reducer-output",
        tmpdir,
    )
