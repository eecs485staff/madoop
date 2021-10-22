"""
System tests for the map stage of Michigan Hadoop.
"""

import filecmp
import os
import pathlib
import shutil
from madoop.__main__ import map_stage


def test_map_stage():
    """Test the map_stage function using the word_count example"""

    input_dir = pathlib.Path("tests/testdata/word_count/"
                             "correct/hadooptmp/mapper-input")

    output_dir = pathlib.Path("tests/testdata/word_count/"
                              "output/mapper-output")

    exe = pathlib.Path("tests/testdata/word_count/map.py").resolve()
    num_mappers = 2

    if output_dir.exists():
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)

    map_stage(exe, input_dir, output_dir, num_mappers, False)
    correct = pathlib.Path("tests/testdata/word_count/"
                           "correct/hadooptmp/mapper-output")

    for file in os.listdir(output_dir):
        assert filecmp.cmp(output_dir/file, correct/file)
