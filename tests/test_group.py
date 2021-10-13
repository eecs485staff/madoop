from madoop.__main__ import group_stage
import filecmp
import os
import pathlib
import shutil

def test_group_stage():
    """Test the reduce_stage function using the word_count example"""

    input_dir = pathlib.Path("tests/testdata/word_count/correct/hadooptmp/mapper-output")
    output_dir = pathlib.Path("tests/testdata/word_count/output/grouper-output")

    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir()

    group_stage(input_dir, output_dir)
    correct = pathlib.Path("tests/testdata/word_count/correct/hadooptmp/grouper-output")
    for f in os.listdir(output_dir):
        assert(filecmp.cmp(output_dir/f, correct/f))