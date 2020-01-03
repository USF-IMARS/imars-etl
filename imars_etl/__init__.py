
from imars_etl.util.config_logger import preconfig_logger
preconfig_logger()

from imars_etl.api import *  # noqa F401

__version__ = "0.15.0"  # NOTE: this should match version in setup.py
