"""System tests for the map stage of Michigan Hadoop."""
import pathlib
import filecmp
import madoop


# Directory containing unit test input files, etc.
TESTDATA_DIR = pathlib.Path(__file__).parent/"testdata"


def test_map_stage(tmpdir):
    """Test the map stage using word count example."""
    madoop.__main__.map_stage(
        exe=TESTDATA_DIR/"word_count/map.py",
        input_dir=TESTDATA_DIR/"word_count/correct/hadooptmp/mapper-input",
        output_dir=tmpdir,
        num_map=2,
        enforce_keyspace=False,
    )
    correct_dir = TESTDATA_DIR/"word_count/correct/hadooptmp/mapper-output"
    correct_list = sorted(correct_dir.glob("part-*"))
    actual_list = sorted(pathlib.Path(tmpdir/"output").glob("part-*"))
    for correct, actual in zip(correct_list, actual_list):
        assert filecmp.cmp(correct, actual, shallow=False)


def test_group_stage(tmpdir):
    """Test group stage using word count example."""
    madoop.__main__.group_stage(
        input_dir=TESTDATA_DIR/"word_count/correct/hadooptmp/mapper-output",
        output_dir=tmpdir,
    )
    correct_dir = TESTDATA_DIR/"word_count/correct/hadooptmp/grouper-output"
    correct_list = sorted(correct_dir.glob("part-*"))
    actual_list = sorted(pathlib.Path(tmpdir).glob("part-*"))
    for correct, actual in zip(correct_list, actual_list):
        assert filecmp.cmp(correct, actual, shallow=False)


def test_reduce_stage(tmpdir):
    """Test reduce stage using word count example."""
    madoop.__main__.reduce_stage(
        exe=TESTDATA_DIR/"word_count/reduce.py",
        input_dir=TESTDATA_DIR/"word_count/correct/hadooptmp/grouper-output",
        output_dir=tmpdir,
        num_reduce=2,
        enforce_keyspace=False,
    )
    correct_dir = TESTDATA_DIR/"word_count/correct/hadooptmp/reducer-output"
    correct_list = sorted(correct_dir.glob("part-*"))
    actual_list = sorted(pathlib.Path(tmpdir).glob("part-*"))
    for correct, actual in zip(correct_list, actual_list):
        assert filecmp.cmp(correct, actual, shallow=False)
