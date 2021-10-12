from os import path
from madoop.__main__ import map_stage
import pathlib

def test_map_stage():
    """Test the map_stage function using the word_count example"""
    input_dir = pathlib.Path("testdata/word_count/correct/hadooptmp/mapper-input")
    output_dir = pathlib.Path("testdata/word_count/output")
    exe = pathlib.Path("testdata/word_count/map.py").resolve()
    num_mappers = 3
    map_stage(exe, input_dir, output_dir, num_mappers, False)