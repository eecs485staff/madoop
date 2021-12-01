"""System tests for the map stage of Michigan Hadoop."""
import shutil
from pathlib import Path
from madoop.mapreduce import map_stage, group_stage, reduce_stage
from . import utils
from .utils import TESTDATA_DIR


def test_map_stage(tmpdir):
    """Test the map stage using word count example."""
    # Copy input files to tmpdir
    input_dir = Path(tmpdir)/"input"
    output_dir = Path(tmpdir)/"grouper-output"
    output_dir.mkdir()
    shutil.copytree(
        TESTDATA_DIR/"word_count/correct/input",
        input_dir,
    )

    # Execute map stage
    map_stage(
        exe=TESTDATA_DIR/"word_count/map.py",
        input_dir=input_dir,
        output_dir=output_dir,
        num_map=2,
    )

    # Verify contents of output directory
    utils.assert_dirs_eq(
        TESTDATA_DIR/"word_count/correct/mapper-output",
        output_dir,
    )


def test_group_stage(tmpdir):
    """Test group stage using word count example."""
    # Copy input files to tmpdir
    input_dir = Path(tmpdir)/"mapper-output"
    output_dir = Path(tmpdir)/"grouper-output"
    output_dir.mkdir()
    shutil.copytree(
        TESTDATA_DIR/"word_count/correct/mapper-output",
        input_dir,
    )

    # Execute group stage
    group_stage(
        input_dir=input_dir,
        output_dir=output_dir,
    )

    # Verify contents of output directory
    utils.assert_dirs_eq(
        TESTDATA_DIR/"word_count/correct/grouper-output",
        output_dir,
    )


def test_reduce_stage(tmpdir):
    """Test reduce stage using word count example."""
    # Copy input files to tmpdir
    input_dir = Path(tmpdir)/"grouper-output"
    output_dir = Path(tmpdir)/"reducer-output"
    output_dir.mkdir()
    shutil.copytree(
        TESTDATA_DIR/"word_count/correct/grouper-output",
        input_dir,
    )

    # Execute reduce stage
    reduce_stage(
        exe=TESTDATA_DIR/"word_count/reduce.py",
        input_dir=input_dir,
        output_dir=output_dir,
        num_reduce=4,
    )

    # Verify contents of output directory
    utils.assert_dirs_eq(
        TESTDATA_DIR/"word_count/correct/reducer-output",
        output_dir,
    )
