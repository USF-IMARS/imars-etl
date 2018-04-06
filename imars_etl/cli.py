"""
defines CLI interface using argparse
"""
from argparse import ArgumentParser
import logging
from logging.handlers import RotatingFileHandler
import os

from imars_etl.load import load
from imars_etl.extract import extract
from imars_etl.id_lookup import id_lookup
from imars_etl.get_metadata import get_metadata

def parse_args(argvs):
    # print(argvs)
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

    # =========================================================================
    # === subcommands
    # =========================================================================
    subparsers = parser.add_subparsers(
        title='subcommands',
        description='usage: `projectname $subcommand` ',
        help='addtnl help for subcommands: `projectname $subcommand -h`'
    )

    # === extract
    parser_extract = subparsers.add_parser(
        'extract',
        help='download file from data warehouse'
    )
    parser_extract.set_defaults(func=extract)
    parser_extract.add_argument("sql",
        help="SQL `WHERE _____` style selector string."
    )  # NOTE: same as get_metadata.sql

    # === get_metadata
    parser_get_metadata = subparsers.add_parser(
        'get_metadata',
        help="prints json-formatted metadata for first entry in given args.sql"
    )
    parser_get_metadata.set_defaults(func=get_metadata)
    parser_get_metadata.add_argument("sql",
        help="SQL `WHERE _____` style selector string."
    )  # NOTE: same as extract.sql

    # === id_lookup
    parser_id_lookup = subparsers.add_parser(
        'id_lookup',
        help="translates between numeric id numbers & short names"
    )
    parser_id_lookup.set_defaults(func=id_lookup)

    parser_id_lookup.add_argument( "table",
        help="name of the table we use (eg: area, product, status)"
    )
    parser_id_lookup.add_argument( "value",
        help="id # or short_name to translate."
    )

    # === load
    parser_load = subparsers.add_parser(
        'load',
        help='upload file to data warehouse'
    )
    parser_load.set_defaults(func=load)
    # required args
    required_named_args = parser_load.add_mutually_exclusive_group(
        required=True
    )
    required_named_args.add_argument("-f", "--filepath",
        help="path to file to upload"
    )
    required_named_args.add_argument("-d", "--directory",
        help="path to directory of files to be loaded"
    )
    # args required only with --directory
    parser_load.add_argument("-n",
        "--product_type_name", "--name", "--short_name",
        help="product type id short_name"
    )
    parser_load.add_argument("-p", "--product_type_id", "--pid",
    help="product type id (pid)", type=int
    )
    # optional args
    parser_load.add_argument("-t", "--time",
        help="ISO8601-formatted date-time string of product"
    )
    parser_load.add_argument("-i", "--ingest_key",
        help="explicitly identifies what ingest format to expect"
    )
    parser_load.add_argument("-j", "--json",
        help="string of json with given file's metadata."
    )
    parser_load.add_argument("--dry_run",
        help="test run only, does not actually insert into database",
        action="store_true"
    )
    # ===
    args = parser.parse_args(argvs)
    # =========================================================================
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
    return args