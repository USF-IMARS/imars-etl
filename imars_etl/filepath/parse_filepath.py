"""
parse metadata out of a filepath
"""
from datetime import datetime
import logging
import os

from parse import parse

from imars_etl.filepath.get_filepath_formats import get_filepath_formats


def parse_filepath(
    metadb_handle,
    load_format=None,
    filepath=None,
    product_type_name=None,
    product_id=None,
    ingest_key=None,
    testing=False,
    **kwargs
):
    """
    Attempts to fill all arguments in args using args.filepath and information
    from `imars_etl.filepath.data`. Tries to match against all possible product
    types if args.product_type_name is not given
    parse_filepath but for argparse namespaces
    """
    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
        )
    )
    logger.trace((
        "parse_filepath(\n\tfmt={},\n\tfpath={},\n\tpname={},\n\ting_k={},"
        "\n\tpid={}\n)"
        ).format(
            load_format, filepath, product_type_name, ingest_key, product_id
        )
    )
    args_dict = {}
    if (load_format is not None):
        args_parsed = _parse_from_product_type_and_filename(
            filepath,
            load_format,
            'manually set custom load_format',
            product_type_name,
            testing=testing
        )
    else:  # try all patterns (limiting by product name & ingest key if given)
        for pattern_name, pattern in get_filepath_formats(
            metadb_handle, short_name=product_type_name,
            ingest_name=ingest_key,
            product_id=product_id
        ).items():
            try:
                product_type_name = pattern_name.split(".")[0]
                args_parsed = _parse_from_product_type_and_filename(
                    filepath,
                    pattern,
                    pattern_name,
                    product_type_name,
                    testing=testing
                )
                break
            except SyntaxError as s_err:  # filepath does not match
                logger.trace("nope. caught error: \n>>>{}".format(s_err))
                product_type_name = None
        else:
            logger.warning("could not match filepath to any known patterns.")
            args_parsed = {}

    for key in args_parsed.keys():
        # TODO: args_dict = ...
        _set_unless_exists(args_dict, key, args_parsed[key])

    return args_dict


def _replace_strftime_dirs(in_string):
    """Replaces strftime directives with something usable by parse()"""
    for direc, fmt in _STRFTIME_MAP.items():
        in_string = in_string.replace(direc, fmt)
    return in_string


def _parse_multidirective(
    input_str, fmt_str, directive
):
    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
        )
    )
    logger.setLevel(5)
    n_duplicates = fmt_str.count(directive) - 1

    this_d_key = _STRFTIME_MAP[directive].split(
        ":"
    )[0].replace('{', '').replace('}', '')

    this_d_fmt = _STRFTIME_MAP[directive].split("}")[0].split(":")
    if len(this_d_fmt) == 2:
        this_d_fmt = this_d_fmt[1]
    else:
        this_d_fmt = ""

    logger.trace("parsing {}-duplicated dt '{}'...".format(
        n_duplicates, this_d_key
    ))
    dedirred_new_fmt_str = _replace_strftime_dirs(fmt_str)
    logger.trace(
        "parse '{}' from strings:\n\t{}\n\t{}".format(
            directive,
            dedirred_new_fmt_str, input_str
        )
    )
    parsed_params = parse(dedirred_new_fmt_str, input_str)
    # assert that all parsed_params are equal
    if parsed_params is None:
        raise ValueError(
            "Failed to parse filepath with multiple strptime directives. "
            "Possible conflicting values found for same directive."
        )
    logger.trace(parsed_params)
    read_value = parsed_params[this_d_key]

    new_str = fmt_str.replace(  # fill all values except last one
        directive,
        ('{:' + this_d_fmt + '}').format(int(read_value)),
        n_duplicates
    )
    return read_value, new_str


def _strptime_safe(input_str, fmt_str):
    """
    Wraps strptime to handle duplicate datetime directives.
    eg: "error: redefinition of group name..."
    """
    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
        )
    )
    # TODO: do we need to handle escapes:
    # fmt_str = fmt_str.replace("\%", "_P_")
    # fmt_str = fmt_str.replace("%%", "_PP_")
    directives = [str[0] for str in fmt_str.split('%')[1:]]
    new_str = fmt_str
    for dir in _STRFTIME_MAP.keys():
        dir = dir[1:]
        d_count = directives.count(dir)
        if d_count > 1:  # if duplicate
            logger.info(
                "Duplicate strptime directive detected."
                "Assuming all values equal; will throw ValueError if not."
            )
            read_value, new_str = _parse_multidirective(
                input_str, fmt_str, directive="%{}".format(dir)
            )
            fmt_str = fmt_str.replace(
                "%{}".format(dir),
                str(read_value),
                d_count - 1
            )
    return datetime.strptime(input_str, fmt_str)


def _strptime_parsed_pattern(input_str, format_str, params):
    """
    Extracts datetime from given input_str matching given format_str.
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
    return _strptime_safe(input_str, filled_fmt_str)


def _parse_from_product_type_and_filename(
    filepath, pattern, pattern_name, product_type_name,
    testing=False
):
    """
    Uses given pattern to parse args.filepath and fill any other arguments
    that can be inferred.

    args.product_type_name must be set before calling

    Returns:
    --------
    parsed_vars : dict
        dict of vars that were parsed from the given info
    """
    logger = logging.getLogger("imars_etl.{}".format(
        __name__,

    ))
    logger.info("attempt parse as {}...".format(pattern_name))
    filename = filepath
    # switch to basepath if path info not part of pattern
    # logger.trace('fname: \n\t{}'.format(filename))
    # logger.trace('pattern: \n\t{}'.format(pattern))
    if "/" in filename and "/" not in pattern:
        filename = os.path.basename(filename)

    # logger.debug('trying pattern "{}"'.format(pattern))
    logger.trace("\n{}\n\t=?=\n{}".format(filename, pattern))
    # logger.debug('args:\n{}'.format(args))

    path_fmt_str = _replace_strftime_dirs(pattern)
    params_parsed = parse(path_fmt_str, filename)
    if params_parsed is None:
        raise SyntaxError(
            "filepath does not match pattern\n\tpath: {}\n\tpattern:{}".format(
                filename,
                path_fmt_str
            )
        )
        params_parsed = {}
    else:
        params_parsed = params_parsed.named

    dt = _strptime_parsed_pattern(filename, pattern, params_parsed)

    logger.debug("params parsed from fname: \n\t{}".format(params_parsed))
    # NOTE: setattr LAST here, else args will get set before we know
    #   that this filename matches the given pattern
    parsed_vars = {}
    for param in params_parsed:
        if param[:3] != "dt_":  # ignore these
            val = params_parsed[param]
            # arg_dict = _set_unless_exists(arg_dict, param, val)
            parsed_vars[param] = val
            # logger.debug('{} extracted :"{}"'.format(param, val))

    parsed_vars['date_time'] = dt
    parsed_vars['time'] = dt.isoformat()
    logger.debug('date extracted: {}'.format(parsed_vars['time']))
    parsed_vars['product_type_name'] = product_type_name

    return parsed_vars


def _set_unless_exists(the_dict, key, val):
    """
    Sets the_dict[key] with val unless the_dict[key] already exists
    Like `the_dict.setdefault(key, val)`, but this *will* overwrite `None`.
    """
    if the_dict.get(key) is None:
        the_dict[key] = val
    return the_dict


_STRFTIME_MAP = {
    "%a": "{dt_a:3w}",  # Weekday as locale's abbreviated name.   |  Mon
    "%A": "{dt_A:w}day",  # Weekday as locale's full name.   |  Monday
    # Weekday as a decimal number, where 0 is Sunday and 6 is Saturday.   |  1
    "%w": "{dt_w:1d}",
    # Day of the month as a zero-padded decimal number.   |  30
    "%d": "{dt_d:2d}",
    # Day of the month as a decimal number. (Platform specific)   |  30
    "%-d": "{dt_dd:d}",
    "%b": "{dt_b:3w}",  # Month as locale's abbreviated name.   |  Sep
    "%B": "{dt_B:w}",  # Month as locale's full name.   |  September
    "%m": "{dt_m:2d}",  # Month as a zero-padded decimal number.   |  09
    # Month as a decimal number. (Platform specific)   |  9
    "%-m": "{dt_mm:d}",
    # Year without century as a zero-padded decimal number.   |  13
    "%y": "{dt_y:2d}",
    "%Y": "{dt_Y:4d}",  # Year with century as a decimal number.   |  2013
    # Hour (24-hour clock) as a zero-padded decimal number.   |  07
    "%H": "{dt_H:2d}",
    # Hour (24-hour clock) as a decimal number. (Platform specific)   |  7
    "%-H": "{dt_HH:d}",
    # Hour (12-hour clock) as a zero-padded decimal number.   |  07
    "%I": "{dt_I:2d}",
    # Hour (12-hour clock) as a decimal number. (Platform specific)   |  7
    "%-I": "{dt_II:}",
    "%p": "{dt_p:2w}",  # Locale's equivalent of either AM or PM.   |  AM
    "%M": "{dt_M:2d}",  # Minute as a zero-padded decimal number.   |  06
    # Minute as a decimal number. (Platform specific)   |  6
    "%-M": "{dt_MM:d}",
    "%S": "{dt_S:2d}",  # Second as a zero-padded decimal number.   |  05
    # Second as a decimal number. (Platform specific)   |  5
    "%-S": "{dt_SS:d}",
    # Microsecond as a decimal number, zero-padded on the left.   |  000000
    "%f": "{dt_f:6d}",
    # UTC offset in the form +HHMM or -HHMM (empty string if object is naive).
    "%z": "{dt_z:4w}",
    "%Z": "{dt_Z:w}",  # Time zone name (empty string if the object is naive).
    # Day of the year as a zero-padded decimal number.   |  273
    "%j": "{dt_j:3d}",
    # Day of the year as a decimal number. (Platform specific)   |  273
    "%-j": "{dt_jj:d}",
    # Week number of the year (Sunday as the first day of the week)
    # as a zero padded decimal number. All days in a new year preceding the
    # first Sunday are considered to be in week 0.   |  39
    "%U": "{dt_U:}",
    # Week number of the year (Monday as the first day of the week)
    # as a decimal number. All days in a new year preceding the first
    # Monday are considered to be in week 0.   |  39
    "%W": "{dt_W:2d}",
    # Locale's appropriate date and time representation.
    # |  Mon Sep 30 07:06:05 2013
    "%c": "{dt_c}",
    "%x": "{dt_x}",  # Locale's appropriate date representation.   |  09/30/13
    "%X": "{dt_X}",  # Locale's appropriate time representation.   |  07:06:05
    "%%": "%"  # A literal '%' character.   |  %
}
