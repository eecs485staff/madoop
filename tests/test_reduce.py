from madoop.__main__ import reduce_stage
import filecmp
import os
import pathlib
import shutil

def test_reduce_stage():
    """Test the reduce_stage function using the word_count example"""

    input_dir = pathlib.Path("tests/testdata/word_count/correct/hadooptmp/grouper-output")
    output_dir = pathlib.Path("tests/testdata/word_count/output/reducer-output")
    exe = pathlib.Path("tests/testdata/word_count/reduce.py").resolve()
    num_reducers = 4

    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir()

    reduce_stage(exe, input_dir, output_dir, num_reducers, False)
    correct = pathlib.Path("tests/testdata/word_count/correct/hadooptmp/reducer-output")
    for f in os.listdir(output_dir):
        assert(filecmp.cmp(output_dir/f, correct/f))