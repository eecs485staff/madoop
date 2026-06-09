# AGENTS.md

This file provides guidance to Claude Code (claude.ai/code) and other agents when working with code in this repository.

Michigan Hadoop (`madoop`) is a lightweight, single-machine MapReduce framework for education that implements a subset of the [Hadoop Streaming](https://hadoop.apache.org/docs/r1.2.1/streaming.html) interface.

## Commands

```console
# Set up dev environment
$ python3 -m venv .venv
$ source .venv/bin/activate
$ pip install --editable .[dev]

# Run the full CI suite (file diffs, pycodestyle, pydocstyle, pylint,
# check-manifest, pytest with coverage) in a clean temp virtualenv
$ tox -e py3

# Run tests
$ pytest
$ pytest -vv --log-cli-level=DEBUG          # verbose with logs
$ pytest tests/test_api.py::test_simple     # single test
$ pytest --cov ./madoop --cov-report term-missing

# Linters individually
$ pycodestyle madoop tests
$ pydocstyle madoop tests
$ pylint madoop tests
$ check-manifest
```

## Architecture

The whole framework is `madoop/mapreduce.py` (~480 lines). `__main__.py` is a thin
CLI wrapper and `__init__.py` re-exports the public API (`mapreduce`, `MadoopError`).

`mapreduce()` is the single entry point. It validates args, then runs three stages
inside one auto-cleaned `tempfile.TemporaryDirectory`, passing intermediate files
between stages via subdirectories (`mapper-output/`, `reducer-input/`, `output/`).
Final results are moved to the user's output dir only at the end.

The three stages, all operating on Hadoop Streaming `key\t value` lines (the key is
everything before the first tab):

1. **Map** (`map_stage`): `normalize_input_paths` collects input files, `split_file`
   breaks each into <=10 MB chunks on line boundaries (`MAX_INPUT_SPLIT_SIZE`), and each
   chunk runs through the mapper executable concurrently via a `ThreadPoolExecutor`
   (one `part-NNNNN` output per chunk).
2. **Group** (`group_stage`): partitions every mapper output line to one of
   `num_reducers` files. Default partitioning is `md5(key) % num_reducers`
   (`partition_keys_default`); a user `-partitioner` executable overrides it
   (`partition_keys_custom`, which pipes lines through the partitioner subprocess and
   validates each returned integer is `0 <= p < num_reducers`). Empty partition files
   are then removed, and the rest are sorted in parallel via a `multiprocessing.Pool`.
3. **Reduce** (`reduce_stage`): runs the reducer executable on each sorted partition
   file concurrently via a `ThreadPoolExecutor`.

User-supplied map/reduce/partition programs are run as **subprocesses** communicating
over stdin/stdout. Before the pipeline runs, `is_executable()` invokes each one with
empty input to surface bad-shebang/permission errors as a clean `MadoopError` (the only
exception type that escapes the API; the CLI catches it and exits with `Error: ...`).

## Gotchas

- **The example and the test fixture are kept byte-identical.** `tox` diffs
  `madoop/example/{input,map.py,reduce.py}` against
  `tests/testdata/word_count/`. If you edit one, edit the other or `tox` fails.
  `madoop/example/` is what `madoop --example` copies into the user's CWD.
- **Don't wrap `multiprocessing.Pool` in a `with` statement** (see comment in
  `group_stage`). It breaks `pytest-cov` subprocess coverage measurement; use explicit
  `close()`/`join()` in a `finally`.
- CLI options mirror Hadoop Streaming names (`-input`, `-output`, `-mapper`,
  `-reducer`, `-numReduceTasks`, `-partitioner`) and use `parse_known_args`, so an
  ignored `jar hadoop-streaming-X.Y.Z.jar` argument is tolerated.
- The package version lives in `pyproject.toml`; the release procedure (tag, build,
  twine) is in `CONTRIBUTING.md`.
- **Supported Python versions live in two places that must stay in lock step.** When
  adding or dropping a version, update both together:
  1. `pyproject.toml` `requires-python` (the minimum version floor).
  2. `.github/workflows/continuous_integration.yml` `python-version` matrix (tests the
     floor and the latest minor, e.g. `["3.9", "3.x"]`).
