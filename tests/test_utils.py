from madoop.__main__ import check_num_keys
from madoop.__main__ import check_shebang
from madoop.__main__ import part_filename
import pathlib

def test_part_filename():
    part_num = 3
    filename = part_filename(part_num)
    assert(filename == ('part-'+('%05d'%part_num)))


def test_check_shebang():
    file = pathlib.Path("tests/testdata/word_count/map.py")
    check_shebang(file)