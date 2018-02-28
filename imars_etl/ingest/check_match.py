from datetime import datetime
import logging
import re
import os

from imars_etl.ingest.filepath_data import valid_pattern_vars

def _parse_from_regex(key, strptime_filename, filename):
    """
    returns
    ----------
    value : str
        value of key read from filename. `None` if failed to read
    strptime_filename : str
        modified filename with read in value replaced by key. Unmodified if
        value failed to read.
    """
    logger = logging.getLogger(__name__)
    # cut out
    regex = (
        valid_pattern_vars[key][0]
        + "[^"+valid_pattern_vars[key][0]+"]+".format(key)
        + valid_pattern_vars[key][2]
    )
    try:
        regex_result = re.search(regex, strptime_filename)
        logger.debug("regex_result: " + str(regex_result))
        matched_string = regex_result.group(0)
        strptime_filename = strptime_filename.replace(
            matched_string,
            valid_pattern_vars[key][0] + key + valid_pattern_vars[key][2]
        )
        logger.debug("matched_str : " + matched_string)
        return matched_string, strptime_filename
    except AttributeError as a_err:  # no regex match
        logger.info(
            "no match for '" + key + "'"
            + " regex '" + regex + "'"
            + " in filename '" + filename + "'"
        )
        return None, strptime_filename


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
                val, strptime_filename = _parse_from_regex(
                    key, strptime_filename, filename
                )
            else:
                # check for each of the possible valid values
                for valid_pattern in valid_pattern_vars[key]:
                    logger.debug("pattern test: " + valid_pattern)
                    if valid_pattern in filename:
                        strptime_filename = strptime_filename.replace(
                            valid_pattern,
                            key
                        )
                        break
                else:
                    logger.info(
                        "value read from filename for " + key
                        + " is not in list of valid keys: "
                        + str(valid_pattern_vars[key])
                    )
                    return False
    # check for valid date in filename that matches pattern
    try:
        datetime.strptime(strptime_filename, strptime_pattern)
    except ValueError as v_err:
        logger.info(v_err)
        return False

    return True
