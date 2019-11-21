import logging
from logging import getLoggerClass, addLevelName, setLoggerClass, NOTSET

TRACE = 5


class MyLogger(getLoggerClass()):
    def __init__(self, name, level=NOTSET):
        super().__init__(name, level)

        addLevelName(TRACE, "TRACE")

    def trace(self, msg, *args, **kwargs):
        if self.isEnabledFor(TRACE):
            self._log(TRACE, msg, args, **kwargs)

setLoggerClass(MyLogger)


def run_once(f):
    def wrapper(*args, **kwargs):
        if not wrapper.has_run:
            wrapper.has_run = True
            return f(*args, **kwargs)
    wrapper.has_run = False
    return wrapper


@run_once
def config_logger(verbosity=0, quiet=False):
    # =========================================================================
    # === set up logging behavior
    # =========================================================================
    if quiet:
        if verbosity > 0:
            raise ValueError(
                "ParadoxError: Cannot be both quiet (-q) and verbose (-v)."
            )
        else:
            lvl_console = logging.ERROR
            lvl_libs = logging.ERROR
    else:
        if (verbosity == 0):
            lvl_console = logging.INFO
            lvl_libs = logging.WARN
        elif (verbosity == 1):
            lvl_console = logging.DEBUG
            lvl_libs = logging.INFO
        else:
            assert verbosity >= 2
            lvl_console = TRACE
            lvl_libs = logging.DEBUG
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
    logging.getLogger("airflow").setLevel(lvl_libs)
    logging.getLogger("airflow").propagate = False
    logging.getLogger("parse").setLevel(lvl_libs)
    logging.getLogger("parse").propagate = False

    # LOG_DIR = "/var/opt/imars_etl/"
    # if not os.path.exists(LOG_DIR):
    #     os.makedirs(LOG_DIR)
    # file_handler = RotatingFileHandler(
    #    LOG_DIR+'imars_etl.log', maxBytes=1e6, backupCount=5
    # )
    # file_handler.setFormatter(formatter)
