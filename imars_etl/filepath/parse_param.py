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

from imars_etl.filepath.data import get_ingest_formats, get_product_id, get_ingest_format

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

def _parse_from_product_type_and_filename(args, pattern, pattern_name):
    """
    Uses given pattern to parse args.filepath and fill any other arguments
    that can be inferred.
    """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    logger.debug("parsing as {}".format(pattern_name))
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
    params_parsed = parse(path_fmt_str, filename)
    if params_parsed is None:
        raise SyntaxError("filepath does not match pattern")
    else:
        params_parsed = params_parsed.named

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


def parse_all_from_filename(args):
    """
    attempts to fill all arguments in args using args.filepath and information
    from `imars_etl.filepath.data`. Tries to match against all possible product
    types if args.product_type_name is not given

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
    if (
        getattr(args, 'product_type_name', None) is not None and
        getattr(args, 'ingest_key', None) is not None
    ):
        args = _parse_from_product_type_and_filename(
            args,
            get_ingest_format(args.product_type_name, args.ingest_key),
            '{}.{}'.format(args.product_type_name, args.ingest_key)
        )
        return args
    else:  # try all patterns
        for pattern_name, pattern in get_ingest_formats().items():
            try:
                args = _parse_from_product_type_and_filename(args, pattern, pattern_name)
                return args
            except SyntaxError as s_err:  # filepath does not match
                logger.debug("nope. caught error: \n>>>{}".format(s_err))
        else:
            logger.warn("could not match filepath to any known patterns.")
            return args
