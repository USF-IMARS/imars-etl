"""
functions to parse metadata out of filepath based on known patterns in
filepath.data
"""
from datetime import datetime
import logging
import re
import os

from imars_etl.filepath.data import valid_pattern_vars, filename_patterns

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
    # logger.debug("parsing '" + key + "' from filename '" + filename + "'")
    try:
        if key == "date":  # must handle date specially
            return parse_date(filename), strptime_filename
        elif "*" in valid_pattern_vars[key]:  # if we should regex
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

def parse_date(filename):
    """
    attempts to read date from filename by checking against all patterns in
    filepath.data.filename_patterns
    """
    logger = logging.getLogger(__name__)
    dates_matched=[]
    for pattern_name in filename_patterns:
        pattern = filename_patterns[pattern_name]
        if "/" in filename and "/" not in pattern:
            filename = os.path.basename(filename)
        try:
            dates_matched.append(_parse_date(filename, pattern))
        except ValueError as v_err:  # filename does not match pattern
            logger.debug("filename does not match date pattern")
            pass
    if len(dates_matched) == 1:
        return dates_matched[0]
    elif len(dates_matched) > 1:
        # TODO: try to get & return consensus
        raise ValueError(
            "filename matches multiple known patterns. Possible dates are: \n"
            + str(dates_matched)
        )
    else: # len(dates_matched) < 1:
        raise ValueError("filename does not match any known patterns")


def _parse_date(filename, pattern):
    """reads a date from filename using given pattern"""
    logger = logging.getLogger(__name__)

    # these strings need to be built up so strptime can read them
    strptime_pattern  = pattern.replace("{","").replace("}","")
    strptime_filename = filename  # values replaced by key string below

    # === replace named args with key so we can strptime
    for key in valid_pattern_vars:  # for each possible argname
        if (key in pattern):  # if the argname is in the pattern
            val, strptime_filename = parse(key, strptime_filename, filename)
    logger.debug("parse_date(\n\tfilename='{}',\n\tpattern= '{}'\n)".format(
        strptime_filename,
        strptime_pattern
    ))
    result = datetime.strptime(strptime_filename, strptime_pattern).isoformat()
    logger.debug("parsed_date={}".format(result))
    return result

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
