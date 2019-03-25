"""
Define CLI interface using argparse.

"""
import logging
from logging import getLoggerClass, addLevelName, setLoggerClass, NOTSET
from argparse import ArgumentParser

import imars_etl
from imars_etl.util.ConstMapAction import ConstMapAction
from imars_etl.BaseHookHandler import get_hooks_list
from imars_etl.object_storage.ObjectStorageHandler \
    import DEFAULT_OBJ_STORE_CONN_ID
from imars_etl.drivers_metadata.get_metadata_driver_from_key \
    import DRIVER_MAP_DICT as METADATA_DRIVER_KEYS
from imars_etl.api import load
from imars_etl.api import extract
from imars_etl.api import id_lookup
from imars_etl.api import select
from imars_etl.api import find

from imars_etl.Load.Load import LOAD_DEFAULTS

from imars_etl.extract import EXTRACT_DEFAULTS


def main(argvs):
    args = parse_args(argvs)
    config_logger(args.verbose)

    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
    ))
    HELLO = '=== IMaRS Extract-Transform-Load Tool v{} ==='.format(
        imars_etl.__version__
    )
    logger.info(HELLO)
    logger.info('=' * len(HELLO))
    # # log test:
    # logger.critical('c')
    # logger.warn('w')
    # logger.info('i')
    # logger.debug('d')
    # logger.trace('t')
    # exit()
    if args.version:
        print("v{}".format(imars_etl.__version__))
        exit()
    else:
        result = args.func(**vars(args))

    if args.func in [extract, id_lookup, select]:
        # print return value
        print(result)
    elif args.func in [find, load]:
        # do nothing w/ return value
        pass
    else:
        raise NotImplementedError(
            "unsure how to handle returned value for func {}".format(args.func)
        )
    return result


TRACE = 5


class MyLogger(getLoggerClass()):
    def __init__(self, name, level=NOTSET):
        super().__init__(name, level)

        addLevelName(TRACE, "TRACE")

    def trace(self, msg, *args, **kwargs):
        if self.isEnabledFor(TRACE):
            self._log(TRACE, msg, args, **kwargs)

setLoggerClass(MyLogger)


def config_logger(verbosity=0):
    # =========================================================================
    # === set up logging behavior
    # =========================================================================
    if (verbosity == 0):
        lvl_console = logging.INFO
    elif (verbosity == 1):
        lvl_console = logging.DEBUG
    else:
        assert verbosity >= 2
        lvl_console = TRACE
        # stream_handler.setLevel(logging.DEBUG)
        # file_handler.setLevel(logging.DEBUG)
    # set up console root logger
    # === (optional) create custom logging format(s)
    # https://docs.python.org/3/library/logging.html#logrecord-attributes
    # long_formatter = logging.Formatter(
    #     '%(asctime)s|%(levelname)s\t|%(filename)s:%(lineno)s\t|%(message)s'
    # )
    short_formatter = logging.Formatter(
        '%(name)-12s: %(levelname)-8s %(message)s'
    )

    # === create handlers
    # https://docs.python.org/3/howto/logging.html#useful-handlers
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(short_formatter)
    stream_handler.setLevel(lvl_console)

    logging.getLogger("imars_etl").addHandler(stream_handler)
    logging.getLogger("imars_etl").setLevel(lvl_console)

    # disable misbehaving root logger
    # logging.getLogger("").setLevel(logging.WARNING)
    # config our loggers
    logging.getLogger("imars_etl").propagate = False
    # === config lib loggers
    logging.getLogger("airflow").setLevel(logging.WARNING)
    logging.getLogger("airflow").propagate = False
    logging.getLogger("parse").setLevel(logging.WARNING)
    logging.getLogger("parse").propagate = False

    # LOG_DIR = "/var/opt/imars_etl/"
    # if not os.path.exists(LOG_DIR):
    #     os.makedirs(LOG_DIR)
    # file_handler = RotatingFileHandler(
    #    LOG_DIR+'imars_etl.log', maxBytes=1e6, backupCount=5
    # )
    # file_handler.setFormatter(formatter)


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
    parser.add_argument(
        "-V", "--version",
        help="print version & exit",
        action="store_true"
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
    NAME_ARGS = [
        "-n",
        "--product_type_name", "--name", "--short_name"
    ]
    NAME_KWARGS = {
            "help": "product type id short_name"
    }
    PID_ARGS = [
            "-p", "--product_id", "--pid",
    ]
    PID_KWARGS = {
        "help": "product type id (pid)",
        "type": int
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
    parser_select.add_argument(
        "-p", "--post_where",
        help=(
            "Additional argument clauses to follow the \"WHERE\" clause."
            "eg: 'ORDER BY last_processed DESC LIMIT 1'"
        ),
        default=""
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

    # === find
    parser_find = subparsers.add_parser(
        'find',
        help='list files in dir matching given data'
    )
    parser_find.set_defaults(func=find)
    parser_find.add_argument(*NAME_ARGS, **NAME_KWARGS)
    parser_find.add_argument(*PID_ARGS, **PID_KWARGS)
    parser_find.add_argument(
        "directory",
        help="path to directory of files to be searched",
    )

    # === load
    parser_load = subparsers.add_parser(
        'load',
        help='upload file to data warehouse'
    )
    parser_load.set_defaults(func=load, **LOAD_DEFAULTS)
    # required args
    parser_load.add_argument(
        "filepath",
        help="path to file to upload",
    )
    # optional args
    parser_load.add_argument(*NAME_ARGS, **NAME_KWARGS)
    parser_load.add_argument(*PID_ARGS, **PID_KWARGS)
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
            " This argument can use template variables. "
            " Template variables are pulled from the arguments passed in."
            " Example: `--metadata_file /metadir/{basename}.xml` to specify "
            " That the metadata file has the same name as the data file"
            " (without file extension), but is in the `/metadir/` directory."
            "Other template vars: \n"
            " {basename} {filename} {filepath} {time} {date_time} "
            "{date_time.year} {ext}"
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
        try:
            args.version
        except AttributeError:
            SEP = "\n-------------------------------------------------------\n"
            print(SEP)
            parser.print_help()
            print(SEP)
            raise ValueError(
                "\n\n\tSubcommand is required. See help above."
            )
    # =========================================================================
    return args
