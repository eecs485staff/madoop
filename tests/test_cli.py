"""System tests for the command line interface."""
import subprocess
import pkg_resources
import pytest
from . import utils
from .utils import TESTDATA_DIR


def test_version():
    """Verify --version flag."""
    result = subprocess.run(
        ["madoop", "--version"],
        stdout=subprocess.PIPE,
        check=True,
    )
    output = result.stdout.decode("utf-8")
    assert "Madoop" in output
    assert pkg_resources.get_distribution("madoop").version in output


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
    utils.assert_dirs_eq(
        TESTDATA_DIR/"word_count/correct/output",
        tmpdir/"output",
    )


def test_verbose(tmpdir):
    """Run a simple MapReduce job and verify the verbose stdout."""
    with tmpdir.as_cwd():
        completed_process = subprocess.run(
            [
                "madoop",
                "--verbose",
                "-input", TESTDATA_DIR/"word_count/input",
                "-output", "output",
                "-mapper", TESTDATA_DIR/"word_count/map.py",
                "-reducer", TESTDATA_DIR/"word_count/reduce.py",
            ],
            stdout=subprocess.PIPE,
            universal_newlines=True,
            check=True,
        )
    stdout_lines = completed_process.stdout.strip().split("\n")
    any(i.startswith("INFO") for i in stdout_lines)
    any(i.startswith("DEBUG") for i in stdout_lines)
    assert len(stdout_lines) > 20


def test_hadoop_arguments(tmpdir):
    """Hadoop Streaming arguments should be ignored."""
    with tmpdir.as_cwd():
        subprocess.run(
            [
                "madoop",
                "jar", "hadoop-streaming-2.7.2.jar",  # Hadoop args
                "-input", TESTDATA_DIR/"word_count/input",
                "-output", "output",
                "-mapper", TESTDATA_DIR/"word_count/map.py",
                "-reducer", TESTDATA_DIR/"word_count/reduce.py",
            ],
            stdout=subprocess.PIPE,
            check=True,
        )


def test_example(tmpdir):
    """Example option should copy files."""
    with tmpdir.as_cwd():
        subprocess.run(
            ["madoop", "--example"],
            check=True,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
    assert (tmpdir/"example/input/input01.txt").exists()
    assert (tmpdir/"example/input/input02.txt").exists()
    assert (tmpdir/"example/map.py").exists()
    assert (tmpdir/"example/reduce.py").exists()

    # Call it again and it should refuse to clobber
    with tmpdir.as_cwd(), pytest.raises(subprocess.CalledProcessError):
        subprocess.run(
            ["madoop", "--example"],
            check=True,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
