from datetime import datetime
import logging
import os

from imars_etl.filepath.data import valid_pattern_vars
from imars_etl.filepath.parse import parse_regex, parse_list

def check_match(filename, pattern):
    """ returns true iff filename matches given filename_pattern """
    logger = logging.getLogger(__name__)
    filename = os.path.basename(filename)  # trim path if given

    # these strings need to be built up so strptime can read them
    strptime_pattern  = pattern.replace("{","").replace("}","")
    strptime_filename = filename  # values replaced by key string below

    # === check for potential named args
    for key in valid_pattern_vars:  # for each possible argname
        if (key in pattern):  # if the argname is in the pattern
            logger.debug("     key    : " + key)
            # validate value in filename
            if "*" in valid_pattern_vars[key]:  # if we should regex
                val, strptime_filename = parse_regex(
                    key, strptime_filename, filename
                )
            else:
                val, strptime_filename = parse_list(
                    key, strptime_filename, filename
                )
    # check for valid date in filename that matches pattern
    try:
        datetime.strptime(strptime_filename, strptime_pattern)
    except ValueError as v_err:
        logger.info(v_err)
        return False

    return True
