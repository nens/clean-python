"""TODO Docstring, used in the command line help text."""
import argparse
import logging

logger = logging.getLogger(__name__)


def get_parser():
    """Return argument parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
        help="Verbose output",
    )
    # add arguments here
    # parser.add_argument(
    #     'path',
    #     metavar='FILE',
    # )
    return parser


def main():  # pragma: no cover
    """Call main command with args from parser.

    This method is called when you run 'bin/run-clean-python',
    this is configured in 'setup.py'. Adjust when needed. You can have multiple
    main scripts.

    """
    options = get_parser().parse_args()
    if options.verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    logging.basicConfig(level=log_level, format="%(levelname)s: %(message)s")

    try:
        print("Call some function from another file here")
        # ^^^ TODO: pass in options.xyz where needed.
    except:  # noqa: E722
        logger.exception("An exception has occurred.")
        return 1
