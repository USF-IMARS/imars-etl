import logging
import os

from imars_etl.filepath.data import valid_pattern_vars
from imars_etl.filepath.parse import _parse_date

def check_match(filename, pattern):
    """ returns true iff filename matches given filename_pattern """
    logger = logging.getLogger(__name__)
    filename = os.path.basename(filename)  # trim path if given

    # check for valid date in filename that matches pattern
    try:
        # NOTE: parse_date checks all the named args as a side-effect,
        #   so we don't need to check those here.
        _parse_date(filename, pattern)
    except ValueError as v_err:
        logger.info(v_err)
        return False

    return True
