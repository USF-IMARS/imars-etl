#!/usr/bin/env python
""" cmd line interface definition for imars-etl """
from argparse import ArgumentParser
import logging
from logging.handlers import RotatingFileHandler
import os

from imars_etl import imars_etl

assert __name__ == "__main__"
# =========================================================================
# === set up arguments
# =========================================================================
parser = ArgumentParser(description='Interface for IMaRS ETL operations')

# === arguments for the main command
parser.add_argument("-v", "--verbose", help="increase output verbosity",
                    action="count",
                    default=0
)
# other examples:
# parser.add_argument("source", help="directory to copy from")
# parser.add_argument('-l', '--log',
#     help="desired filepath of log file",
#     default="/var/opt/projectname/backup.log"
# )
# parser.add_argument('--rclonelog',
#     help="desired path of rclone log file",
#     default=None
# )

parser.set_defaults(func=imars_etl.extract)  # set default behavior if subcommand not given

# === subcommands
subparsers = parser.add_subparsers(
    title='subcommands',
    description='usage: `projectname $subcommand` ',
    help='addtnl help for subcommands: `projectname $subcommand -h`'
)

parser_extract = subparsers.add_parser('extract', help='download file from data warehouse')
parser_extract.set_defaults(func=imars_etl.extract)
parser_extract.add_argument("sql",
    help="SQL `WHERE _____` style selector string"
)

parser_load = subparsers.add_parser('load', help='upload file to data warehouse')
parser_load.set_defaults(func=imars_etl.load)

args = parser.parse_args()
# =========================================================================
# === set up logging behavior
# =========================================================================
if (args.verbose == 0):
    logging.basicConfig(level=logging.WARNING)
elif (args.verbose == 1):
    logging.basicConfig(level=logging.INFO)
else: #} (args.verbose == 2){
    logging.basicConfig(level=logging.DEBUG)

# === (optional) create custom logging format(s)
# https://docs.python.org/3/library/logging.html#logrecord-attributes
formatter = logging.Formatter(
   '%(asctime)s|%(levelname)s\t|%(filename)s:%(lineno)s\t|%(message)s'
)

# === (optional) create handlers
# https://docs.python.org/3/howto/logging.html#useful-handlers
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)

# LOG_DIR = "/var/opt/imars_etl/"
# if not os.path.exists(LOG_DIR):
#     os.makedirs(LOG_DIR)
# file_handler = RotatingFileHandler(
#    LOG_DIR+'imars_etl.log', maxBytes=1e6, backupCount=5
# )
# file_handler.setLevel(logging.DEBUG)
# file_handler.setFormatter(formatter)

# === add the handlers (if any) to the logger
_handlers = [
    stream_handler
    # file_handler
]

logging.basicConfig(
    handlers=_handlers,
    level=logging.DEBUG  # this must be set to lowest of all levels used in handlers
)
# =========================================================================
args.func(args)
