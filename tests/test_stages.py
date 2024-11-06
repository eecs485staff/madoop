"""System tests for the map stage of Michigan Hadoop."""
import shutil
from pathlib import Path
from madoop.mapreduce import (
    map_stage,
    group_stage,
    reduce_stage,
    split_file,
    MAX_INPUT_SPLIT_SIZE,
)
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


def test_input_splitting(tmp_path):
    """Test that the Map Stage correctly splits input."""
    input_data = "o" * (MAX_INPUT_SPLIT_SIZE - 10) + "\n" + \
        "a" * int(MAX_INPUT_SPLIT_SIZE / 2)
    input_dir = tmp_path/"input"
    output_dir = tmp_path/"output"
    input_dir.mkdir()
    output_dir.mkdir()

    with open(input_dir/"input.txt", "w", encoding="utf-8") as input_file:
        input_file.write(input_data)

    map_stage(
        exe=Path(shutil.which("cat")),
        input_dir=input_dir,
        output_dir=output_dir,
    )

    output_files = sorted(output_dir.glob("*"))
    assert len(output_files) == 2
    assert output_files == [output_dir/"part-00000", output_dir/"part-00001"]

    with open(output_dir/"part-00000", "r", encoding="utf-8") as outfile1:
        data = outfile1.read()
        assert data == "o" * (MAX_INPUT_SPLIT_SIZE - 10) + "\n"
    with open(output_dir/"part-00001", "r", encoding="utf-8") as outfile2:
        data = outfile2.read()
        assert data == "a" * int(MAX_INPUT_SPLIT_SIZE / 2)


def test_split_file_mid_chunk(tmp_path):
    """Test that file splitting still works when data remains in the buffer."""
    input_data = "noah says\nhello world"
    input_file = tmp_path/"input.txt"
    with open(input_file, "w", encoding="utf-8") as infile:
        infile.write(input_data)

    splits = list(split_file(input_file, 50))
    assert splits == [b"noah says\n", b"hello world"]
