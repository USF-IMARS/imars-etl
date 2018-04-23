#!/usr/bin/env python
""" cmd line interface definition for imars-etl """
import sys

from imars_etl.cli import parse_args

assert __name__ == "__main__"

args = parse_args(sys.argv[1:])
args.func(args)
