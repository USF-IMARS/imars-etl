#!/usr/bin/env python
"""cmd line interface definition for imars-etl"""
import sys

from imars_etl.cli import main

assert __name__ == "__main__"

main(sys.argv[1:])
