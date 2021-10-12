from madoop.__main__ import map_stage
import pathlib
import shutil
import filecmp
import os

def test_map_stage():
    """Test the map_stage function using the word_count example"""

    input_dir = pathlib.Path("tests/testdata/word_count/correct/hadooptmp/mapper-input")
    output_dir = pathlib.Path("tests/testdata/word_count/output/mapper-output")
    exe = pathlib.Path("tests/testdata/word_count/map.py").resolve()
    num_mappers = 2

    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir()

    map_stage(exe, input_dir, output_dir, num_mappers, False)
    correct = pathlib.Path("tests/testdata/word_count/correct/hadooptmp/mapper-output")
    for f in os.listdir(output_dir):
        assert(filecmp.cmp(output_dir/f, correct/f))
