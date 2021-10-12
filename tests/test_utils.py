from madoop.__main__ import check_num_keys
from madoop.__main__ import check_shebang
from madoop.__main__ import part_filename

def test_part_filename():
    part_num = 3
    filename = part_filename(part_num)
    assert(filename == ('part-'+('%05d'%part_num)))


