"""A light weight MapReduce framework for education.

Andrew DeOrio <awdeorio@umich.edu>

"""
import argparse
import logging
import sys
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

    required_args = parser.add_argument_group('required arguments')
    required_args.add_argument('-input', dest='input', required=True)
    required_args.add_argument('-output', dest='output', required=True)
    required_args.add_argument('-mapper', dest='mapper', required=True)
    required_args.add_argument('-reducer', dest='reducer', required=True)

    args, _ = parser.parse_known_args()

    # Configure logging
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s %(name)s %(levelname)s %(message)s'
    )
    handler.setFormatter(formatter)
    madoop_logger = logging.getLogger("madoop")
    madoop_logger.addHandler(handler)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    try:
        mapreduce(
            input_dir=args.input,
            output_dir=args.output,
            map_exe=args.mapper,
            reduce_exe=args.reducer,
        )
    except MadoopError as err:
        sys.exit(f"Error: {err}")

    # Remind user where to find output
    print(f"Output directory: {args.output}")


if __name__ == '__main__':
    main()
