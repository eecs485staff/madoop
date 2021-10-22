"""System tests for the command line interface."""
import subprocess
import filecmp
from utils import TESTDATA_DIR


def test_version():
    """Verify --version flag."""
    result = subprocess.run(
        ["madoop", "--version"],
        stdout=subprocess.PIPE,
        check=True,
    )
    output = result.stdout.decode("utf-8")
    assert "Fake Hadoop" in output
    assert "by Andrew DeOrio <awdeorio@umich.edu>" in output


def test_help():
    """Verify --help flag."""
    result = subprocess.run(
        ["madoop", "--help"],
        stdout=subprocess.PIPE,
        check=True,
    )
    output = result.stdout.decode("utf-8")
    assert "usage" in output


def test_simple(tmpdir):
    """Run a simple MapReduce job and verify the output."""
    with tmpdir.as_cwd():
        subprocess.run(
            [
                "madoop",
                "-input", TESTDATA_DIR/"word_count/input",
                "-output", "output",
                "-mapper", TESTDATA_DIR/"word_count/map.py",
                "-reducer", TESTDATA_DIR/"word_count/reduce.py",
            ],
            stdout=subprocess.PIPE,
            check=True,
        )
    for path in (TESTDATA_DIR/"word_count/correct").glob("part-*"):
        assert filecmp.cmp(
            path,
            TESTDATA_DIR/"word_count/correct"/path,
            shallow=False,
        )
