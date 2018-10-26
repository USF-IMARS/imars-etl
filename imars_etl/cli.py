"""
Define CLI interface using argparse.

"""
from argparse import ArgumentParser
import logging

from imars_etl.util.ConstMapAction import ConstMapAction
from imars_etl.BaseHookHandler import get_hooks_list
from imars_etl.BaseHookHandler import DEFAULT_OBJ_STORE_CONN_ID
from imars_etl.drivers_metadata.get_metadata_driver_from_key \
    import DRIVER_MAP_DICT as METADATA_DRIVER_KEYS
from imars_etl.api import load
from imars_etl.api import extract
from imars_etl.api import id_lookup
from imars_etl.api import select

from imars_etl.Load.Load import LOAD_DEFAULTS

from imars_etl.extract import EXTRACT_DEFAULTS


def main(argvs):
    args = parse_args(argvs)
    return args.func(**vars(args))


def parse_args(argvs):
    # print(argvs)
    # =========================================================================
    # === set up arguments
    # =========================================================================
    parser = ArgumentParser(description='Interface for IMaRS ETL operations')

    # === arguments for the main command
    parser.add_argument(
        "-v", "--verbose",
        help="increase output verbosity",
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

    # === sub-cmd arguments shared between multiple subcommands:
    SQL = {  # "sql"
        "help": (
            "' AND'-separated list of metadata key-value pairs using "
            "SQL `WHERE _____` style syntax."
            "\nExample: \n\t"
            "\"product_id=3 AND area_id=7 AND date_time='2018-01-02T03:45'\""
        )
    }
    FIRST = {  # "--first"
        "help": "return first result if multiple rather than exiting w/ error",
        "action": "store_true"
    }

    # === extract
    parser_extract = subparsers.add_parser(
        'extract',
        help='download file from data warehouse'
    )
    parser_extract.set_defaults(func=extract, **EXTRACT_DEFAULTS)
    parser_extract.add_argument("sql", **SQL)
    parser_extract.add_argument(
        "-o", "--output_path",
        help="where to save the requested file. " +
        "If excluded cwd and filename from db is used."
    )
    parser_extract.add_argument("--first", **FIRST)

    # === select
    parser_select = subparsers.add_parser(
        'select',
        help="prints json-formatted metadata for first entry in given args.sql"
    )
    parser_select.set_defaults(func=select)
    parser_select.add_argument("sql", **SQL)
    parser_select.add_argument(
        "-c", "--cols",
        help=(
            "comma-separated list of columns to select from metadatabase."
            "eg: 'filepath,date_time'"
        ),
        default="*"
    )
    parser_select.add_argument("--first", **FIRST)

    # === id_lookup
    parser_id_lookup = subparsers.add_parser(
        'id_lookup',
        help="translates between numeric id numbers & short names"
    )
    parser_id_lookup.set_defaults(func=id_lookup)

    parser_id_lookup.add_argument(
        "table",
        help="name of the table we use (eg: area, product, status)"
    )
    parser_id_lookup.add_argument(
        "value",
        help="id # or short_name to translate."
    )

    # === load
    parser_load = subparsers.add_parser(
        'load',
        help='upload file to data warehouse'
    )
    parser_load.set_defaults(func=load, **LOAD_DEFAULTS)
    # required args
    required_named_args = parser_load.add_mutually_exclusive_group(
        required=True
    )
    required_named_args.add_argument(
        "-f", "--filepath",
        help="path to file to upload"
    )
    required_named_args.add_argument(
        "-d", "--directory",
        help="path to directory of files to be loaded"
    )
    # args required only with --directory
    parser_load.add_argument(
        "-n",
        "--product_type_name", "--name", "--short_name",
        help="product type id short_name"
    )
    parser_load.add_argument(
        "-p", "--product_id", "--pid",
        help="product type id (pid)", type=int
    )
    # optional args
    parser_load.add_argument(
        "-t", "--time",
        help="ISO8601-formatted date-time string of product"
    )
    parser_load.add_argument(
        "-i", "--ingest_key",
        help="explicitly identifies what ingest format to expect"
    )
    parser_load.add_argument(
        "-j", "--json",
        help="string of json with given file's metadata."
    )
    parser_load.add_argument("-s", "--sql", **SQL)
    parser_load.add_argument(
        "-l", "--load_format",
        help="python strptime-enabled format string describing input basename."
    )
    parser_load.add_argument(
        "-m", "--metadata_file",
        help=(
            "File containing metadata for the file being loaded."
            "This argument can use template variables if used with --directory"
            " Template variables are pulled from the arguments passed in."
            " Example: `--metadata_file /my/path/{filename}.xml` to specify "
            " That the metadata file has the same name as the data file, but "
            " is in the `/my/path/` directory. Other template vars: \n"
            " {basename} {filename} {filepath} {time} {date_time} "
            "{date_time.year} "
        )
    )
    parser_load.add_argument(  # todo change terminology to "parser"
        "--metadata_file_driver",
        help="driver to use to parse the file given by `metadata_file`",
        action=ConstMapAction,
        options_map_dict=METADATA_DRIVER_KEYS
    )
    parser_load.add_argument(
        "--dry_run",
        help="test run only, does not actually insert into database",
        action="store_true"
    )
    parser_load.add_argument(
        "--object_store",
        help=(
            "Connection id to use for loading the file into object storage. " +
            "ie: which backend to use"
        ),
        default=DEFAULT_OBJ_STORE_CONN_ID,
        choices=get_hooks_list()
    )
    parser_load.add_argument(
        "--duplicates_ok",
        help="do not raise error if trying to load file already in database",
        action="store_true"
    )
    parser_load.add_argument(
        "--nohash",
        help="do not compute hash of the file. WARN: may disable features",
        action="store_true"
    )
    parser_load.add_argument(
        "--noparse",
        help="do not parse filename for metadata. WARN: may disable features",
        action="store_true"
    )
    # ===
    args = parser.parse_args(argvs)
    try:
        args.func
    except AttributeError:
        SEP = "\n-----------------------------------------------------------\n"
        print(SEP)
        parser.print_help()
        print(SEP)
        raise ValueError(
            "\n\n\tSubcommand is required. See help above."
        )
    # =========================================================================
    # =========================================================================
    # === set up logging behavior
    # =========================================================================
    if (args.verbose == 0):
        logging.basicConfig(level=logging.WARNING)
    elif (args.verbose == 1):
        logging.basicConfig(level=logging.INFO)
    else:  # } (args.verbose == 2){
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

    # basicConfig.level must be set to *lowest* of all levels used in handlers
    logging.basicConfig(
        handlers=_handlers,
        level=logging.DEBUG
    )
    # =========================================================================
    return args
