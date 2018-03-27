"""
functions to parse metadata out of filepath based on known patterns in
filepath.data
"""
from datetime import datetime
import logging
import sys
import re
import os

from parse import parse
logging.getLogger("parse").setLevel(logging.WARN)

from imars_etl.filepath.data import valid_pattern_vars, get_ingest_formats, get_product_id, get_ingest_format

STRFTIME_MAP = {
    "%a":  "{dt_a:3w}",  # Weekday as locale's abbreviated name.   |  Mon
    "%A":  "{dt_A:w}day",  # Weekday as locale's full name.   |  Monday
    "%w":  "{dt_w:1d}",  # Weekday as a decimal number, where 0 is Sunday and 6 is Saturday.   |  1
    "%d":  "{dt_d:2d}",  # Day of the month as a zero-padded decimal number.   |  30
    "%-d": "{dt_dd:d}", # Day of the month as a decimal number. (Platform specific)   |  30
    "%b":  "{dt_b:3w}",  # Month as locale's abbreviated name.   |  Sep
    "%B":  "{dt_B:w}",  # Month as locale's full name.   |  September
    "%m":  "{dt_m:2d}",  # Month as a zero-padded decimal number.   |  09
    "%-m": "{dt_mm:d}", # Month as a decimal number. (Platform specific)   |  9
    "%y":  "{dt_y:2d}",  # Year without century as a zero-padded decimal number.   |  13
    "%Y":  "{dt_Y:4d}",  # Year with century as a decimal number.   |  2013
    "%H":  "{dt_H:2d}",  # Hour (24-hour clock) as a zero-padded decimal number.   |  07
    "%-H": "{dt_HH:d}", # Hour (24-hour clock) as a decimal number. (Platform specific)   |  7
    "%I":  "{dt_I:2d}",  # Hour (12-hour clock) as a zero-padded decimal number.   |  07
    "%-I": "{dt_II:}", # Hour (12-hour clock) as a decimal number. (Platform specific)   |  7
    "%p":  "{dt_p:2w}",  # Locale's equivalent of either AM or PM.   |  AM
    "%M":  "{dt_M:2d}",  # Minute as a zero-padded decimal number.   |  06
    "%-M": "{dt_MM:d}", # Minute as a decimal number. (Platform specific)   |  6
    "%S":  "{dt_S:2d}",  # Second as a zero-padded decimal number.   |  05
    "%-S": "{dt_SS:d}", # Second as a decimal number. (Platform specific)   |  5
    "%f":  "{dt_f:6d}",  # Microsecond as a decimal number, zero-padded on the left.   |  000000
    "%z":  "{dt_z:4w}",  # UTC offset in the form +HHMM or -HHMM (empty string if the the object is naive).
    "%Z":  "{dt_Z:w}",  # Time zone name (empty string if the object is naive).
    "%j":  "{dt_j:3d}",  # Day of the year as a zero-padded decimal number.   |  273
    "%-j": "{dt_jj:d}", # Day of the year as a decimal number. (Platform specific)   |  273
    "%U":  "{dt_U:}",  # Week number of the year (Sunday as the first day of the week) as a zero padded decimal number. All days in a new year preceding the first Sunday are considered to be in week 0.   |  39
    "%W":  "{dt_W:2d}",  # Week number of the year (Monday as the first day of the week) as a decimal number. All days in a new year preceding the first Monday are considered to be in week 0.   |  39
    "%c":  "{dt_c}",  # Locale's appropriate date and time representation.   |  Mon Sep 30 07:06:05 2013
    "%x":  "{dt_x}",  # Locale's appropriate date representation.   |  09/30/13
    "%X":  "{dt_X}",  # Locale's appropriate time representation.   |  07:06:05
    "%%":  "%"  # A literal '%' character.   |  %
}

def _replace_strftime_dirs(in_string):
    """ replaces strftime directives with something usable by parse()"""
    for direc, fmt in STRFTIME_MAP.items():
        in_string = in_string.replace(direc, fmt)
    return in_string

def _strptime_parsed_pattern(input_str, format_str, params):
    """
    Parameters
    ----------
    params : dict
        named parameters from previous parse of input_str using format_str
    format_str : str
        original format string like "{whatever}-%Y.txt"
    input_str : str
        same raw input string as passed to parse()
    """
    # fill fmt string with all parameters (except strptime dirs)
    filled_fmt_str = format_str.format(**params)
    return datetime.strptime(input_str, filled_fmt_str)

def parse_all_from_filename(args):
    """
    attempts to fill all arguments in args using args.filepath and information
    from `imars_etl.filepath.data`.

    Parameters
    ----------
    args : ArgParse arg obj
        arguments we have to start with. these will be used to guess at others.

    Returns
    -------
    args : ArgParse arg obj
        modified version of input args with any missing args filled.
    """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    for pattern_name, pattern in get_ingest_formats().items():
        try:
            filename = args.filepath
            # switch to basepath if path info not part of pattern
            if "/" in filename and "/" not in pattern:
                filename = os.path.basename(filename)

            # logger.debug('trying pattern "{}"'.format(pattern))
            logger.debug("\n{}\n\t=?=\n{}".format(filename,pattern))

            # logger.debug('args:\n{}'.format(args))

            product_type_name, ingest_key = pattern_name.split('.')  # TODO: get these from args
            path_fmt_str = get_ingest_format(product_type_name, ingest_key)
            path_fmt_str = _replace_strftime_dirs(path_fmt_str)
            params_parsed = parse(path_fmt_str, filename).named

            dt = _strptime_parsed_pattern(filename, pattern, params_parsed)

            logger.info("params parsed from fname: \n\t{}".format(params_parsed))
            # NOTE: setattr LAST here, else args will get set before we know
            #   that this filename matches the given pattern
            for param in params_parsed:
                if param[:3] != "dt_":  # ignore these
                    val = params_parsed[param]
                    setattr(args, param, val)
                    # logger.debug('{} extracted :"{}"'.format(param, val))

            setattr(args, 'datetime', dt)
            setattr(args, 'time', args.datetime.isoformat())
            logger.debug('date extracted: {}'.format(args.time))
            setattr(args, 'product_type_name', product_type_name)
            setattr(args, 'product_type_id',   get_product_id(product_type_name))

            return args
        except AttributeError as a_err:  # 'NoneType' object has no attribute 'named'
            logger.debug("nope. caught error: \n>>>{}".format(a_err))
    else:
        logger.warn("could not match filepath to any known patterns.")
        return args

def parse_param(key, strptime_filename, filename):
    """
    Returns
    ----------
    value : str
        value of key read from filename. `None` if failed to read
    strptime_filename : str
        modified filename with read in value replaced by key. Unmodified if
        value failed to read.
    """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    # logger.debug("parsing '" + key + "' from filename '" + filename + "'")
    try:
        if key == "time":  # must handle date specially
            logger.debug("parsing date from filename")
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

def filename_matches_pattern(filename, pattern):
    """ returns true if given filename matches given pattern """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    logger.setLevel(logging.INFO)
    # switch to basepath if path info not part of pattern
    if "/" in filename and "/" not in pattern:
        filename = os.path.basename(filename)
        logger.debug("checking basename only")

    logger.debug("\n{}\n\t=?=\n{}".format(filename,pattern))
    try:
        # filename matches if we can successfully get the date
        _parse_date(filename, pattern)
        return True
    except ValueError as v_err:  # filename does not match pattern
        return False

def parse_date(filename):
    """
    attempts to read date from filename by checking against all patterns in
    filepath.data
    """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    dates_matched=[]
    for pattern_name, pattern in get_ingest_formats().items():
        # switch to basepath if path info not part of pattern
        if "/" in filename and "/" not in pattern:
            filename = os.path.basename(filename)

        if filename_matches_pattern(filename, pattern):
            dates_matched.append(_parse_date(filename, pattern))

    if len(dates_matched) == 1:
        logger.debug("date read from {} pattern".format(pattern_name))
        return dates_matched[0]
    elif len(dates_matched) > 1:
        # TODO: try to get & return consensus
        raise LookupError(
            "filename matches multiple known patterns. Possible dates are: \n"
            + str(dates_matched)
        )
    else: # len(dates_matched) < 1:
        raise LookupError("filename does not match any known patterns")

def _parse_date(filename, pattern):
    """reads a date from filename using given pattern"""
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    logger.setLevel(logging.INFO)

    # switch to basepath if path info not part of pattern
    if "/" in filename and "/" not in pattern:
        filename = os.path.basename(filename)

    # these strings need to be built up so strptime can read them
    strptime_pattern  = pattern.replace("{","").replace("}","")
    strptime_filename = filename  # values replaced by key string below

    # === replace named args with key so we can strptime

    for key in valid_pattern_vars:  # for each possible argname
        if (key in pattern):  # if the argname is in the pattern
            val, strptime_filename = parse_param(key, strptime_filename, filename)


    logger.debug("parse_date(\n\tfilename='{}',\n\tpattern= '{}'\n)".format(
        strptime_filename,
        strptime_pattern
    ))
    result = datetime.strptime(strptime_filename, strptime_pattern)
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
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    logger.setLevel(logging.INFO)

    # cut out
    escaped_pre = re.escape(valid_pattern_vars[key][0])
    escaped_post= re.escape(valid_pattern_vars[key][2])
    regex = (
        escaped_pre
        + "[^"+escaped_pre+"]+".format(key)
        + escaped_post
    )
    try:
        logger.debug("re.search({},{})".format(regex,strptime_filename))
        regex_result = re.search(regex, strptime_filename)
        # logger.debug("regex_result: " + str(regex_result))
        matched_string = regex_result.group(0)
        strptime_filename = strptime_filename.replace(
            matched_string,
            valid_pattern_vars[key][0] + key + valid_pattern_vars[key][2]
        )
        # remove pre & post strings:
        matched_string = matched_string.replace(valid_pattern_vars[key][0],"")
        matched_string = matched_string.replace(valid_pattern_vars[key][2],"")
        logger.debug("matched_str : " + matched_string)
        return matched_string, strptime_filename
    except AttributeError as a_err:  # no regex match
        logger.debug(
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
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
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
