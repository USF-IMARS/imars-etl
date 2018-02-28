"""
functions to parse metadata out of filepath based on known patterns in
filepath.data
"""
import logging
import re

from imars_etl.filepath.data import valid_pattern_vars

def parse(key, strptime_filename, filename):
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
    try:
        if "*" in valid_pattern_vars[key]:  # if we should regex
            return parse_regex(
                key, strptime_filename, filename
            )
        else:
            return parse_list(
                key, strptime_filename, filename
            )
    except KeyError as k_err:
        logger.error("no filepath.data for key '" + key + "'")
        return None, strptime_filename

def parse_regex(key, strptime_filename, filename):
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

def parse_list(key, strptime_filename, filename):
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
    # check for each of the possible valid values
    for valid_pattern in valid_pattern_vars[key]:
        logger.debug("pattern test: " + valid_pattern)
        if valid_pattern in filename:
            strptime_filename = strptime_filename.replace(
                valid_pattern,
                key
            )
            return valid_pattern, strptime_filename
    else:
        logger.info(
            "value read from filename for " + key
            + " is not in list of valid keys: "
            + str(valid_pattern_vars[key])
        )
        return None, strptime_filename
