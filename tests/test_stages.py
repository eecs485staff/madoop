"""System tests for the map stage of Michigan Hadoop."""
from pathlib import Path
from madoop.mapreduce import map_stage, group_stage, reduce_stage
from . import utils
from .utils import TESTDATA_DIR


def test_map_stage(tmpdir):
    """Test the map stage using word count example."""
    map_stage(
        exe=TESTDATA_DIR/"word_count/map.py",
        input_dir=TESTDATA_DIR/"word_count/input",
        output_dir=Path(tmpdir),
    )
    utils.assert_dirs_eq(
        TESTDATA_DIR/"word_count/correct/mapper-output",
        tmpdir,
    )


def test_group_stage(tmpdir):
    """Test group stage using word count example."""
    group_stage(
        input_dir=TESTDATA_DIR/"word_count/correct/mapper-output",
        output_dir=Path(tmpdir),
        num_reducers=4,
        partitioner=None,
    )
    utils.assert_dirs_eq(
        TESTDATA_DIR/"word_count/correct/grouper-output",
        tmpdir,
    )


def test_group_stage_2_reducers(tmpdir):
    """Test group stage using word count example with 2 reducers."""
    group_stage(
        input_dir=TESTDATA_DIR/"word_count/correct/mapper-output",
        output_dir=Path(tmpdir),
        num_reducers=2,
        partitioner=None,
    )
    utils.assert_dirs_eq(
        TESTDATA_DIR/"word_count/correct/grouper-output-2-reducers",
        tmpdir,
    )


def test_group_stage_custom_partitioner(tmpdir):
    """Test group stage using word count example with custom partitioner."""
    group_stage(
        input_dir=TESTDATA_DIR/"word_count/correct/mapper-output",
        output_dir=Path(tmpdir),
        num_reducers=2,
        partitioner=TESTDATA_DIR/"word_count/partition.py",
    )
    utils.assert_dirs_eq(
        TESTDATA_DIR/"word_count/correct/grouper-output-custom-partitioner",
        tmpdir,
    )


def test_reduce_stage(tmpdir):
    """Test reduce stage using word count example."""
    reduce_stage(
        exe=TESTDATA_DIR/"word_count/reduce.py",
        input_dir=TESTDATA_DIR/"word_count/correct/grouper-output",
        output_dir=Path(tmpdir),
    )
    utils.assert_dirs_eq(
        TESTDATA_DIR/"word_count/correct/reducer-output",
        tmpdir,
    )


def test_reduce_stage_2_reducers(tmpdir):
    """Test reduce stage using word count example with 2 reducers."""
    reduce_stage(
        exe=TESTDATA_DIR/"word_count/reduce.py",
        input_dir=TESTDATA_DIR/"word_count/correct/grouper-output-2-reducers",
        output_dir=Path(tmpdir),
    )
    utils.assert_dirs_eq(
        TESTDATA_DIR/"word_count/correct/reducer-output-2-reducers",
        tmpdir,
    )
