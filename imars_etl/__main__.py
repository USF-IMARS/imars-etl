#!/usr/bin/env python
"""cmd line interface definition for imars-etl"""
import sys

from imars_etl.cli import main


def _main():
    main(sys.argv[1:])


if __name__ == "__main__":
    _main()
