"""A light weight MapReduce framework for education.

Andrew DeOrio <awdeorio@umich.edu>

"""
import argparse
import logging
import pathlib
import shutil
import sys
import textwrap
import pkg_resources
from .mapreduce import mapreduce
from .exceptions import MadoopError


def main():
    """Parse command line arguments and options then call mapreduce()."""
    parser = argparse.ArgumentParser(
        description='A light weight MapReduce framework for education.'
    )

    optional_args = parser.add_argument_group('optional arguments')
    version = pkg_resources.get_distribution("madoop").version
    optional_args.add_argument(
        '--version', action='version',
        version=f'Madoop {version}'
    )
    optional_args.add_argument(
        '--example', action=ExampleAction, nargs=0,
        help="create example MapReduce program",
    )
    optional_args.add_argument(
        '-v', '--verbose', action='count', default=0,
        help="verbose output"
    )
    required_args = parser.add_argument_group('required arguments')
    required_args.add_argument('-input', dest='input', required=True)
    required_args.add_argument('-output', dest='output', required=True)
    required_args.add_argument('-mapper', dest='mapper', required=True)
    required_args.add_argument('-reducer', dest='reducer', required=True)

    args, _ = parser.parse_known_args()

    # Handle verbose flag with logging configuration
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    if args.verbose > 0:
        root_logger.setLevel(logging.DEBUG)
    else:
        root_logger.setLevel(logging.INFO)

    # Run MapReduce API
    try:
        mapreduce(
            input_dir=args.input,
            output_dir=args.output,
            map_exe=args.mapper,
            reduce_exe=args.reducer,
        )
    except MadoopError as err:
        sys.exit(f"Error: {err}")


class ExampleAction(argparse.Action):
    """Copy example MapReduce program to PWD.

    We're using a custom Action because it runs before the check for required
    arguments executes.

    Doc: https://docs.python.org/3/library/argparse.html#argparse.Action
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __call__(self, parser, *args, **kwargs):
        madoop_dir = pathlib.Path(__file__).parent
        src = madoop_dir/"example"
        dst = pathlib.Path()/"example"
        if dst.exists():
            print(f"Error: directory already exists: {dst}")
            parser.exit(1)
        shutil.copytree(src, dst)
        print(textwrap.dedent(f"""\
            Created {dst}, try:

            madoop \\
              -input example/input \\
              -output example/output \\
              -mapper example/map.py \\
              -reducer example/reduce.py\
        """))
        parser.exit()


if __name__ == '__main__':
    main()
