"""
System tests for the utility functions of Michigan Hadoop.
"""

import pathlib
from madoop.__main__ import check_shebang
from madoop.__main__ import part_filename


def test_part_filename():
    """Tests the part_filename function"""
    part_num = 3
    filename = part_filename(part_num)
    assert filename == (f"part-{part_num:05d}")


def test_check_shebang():
    """Tests the check_shebang function"""
    file = pathlib.Path("tests/testdata/word_count/map.py")
    check_shebang(file)
