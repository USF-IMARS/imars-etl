"""
functions to parse metadata out of filepath based on known patterns in
filepath.data
"""
from datetime import datetime
import logging
import sys
import re
import os

from imars_etl.filepath.data import valid_pattern_vars, get_ingest_formats

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
    # these are soft-required args, ones that we might try to guess if not
    # given, but we have to give up if we cannot figure them out.
    # required_args = ['product_type_id','date']
    #
    # for arg in required_args:
    #     try:
    #         if getattr(args, arg, None) is None:
    #             logger.debug("attempting to guess {}".format(arg))
    #             guessed_val = _guess_arg_value(args, arg)
    #             logger.debug("my guess: {}".format(guessed_val))
    #             setattr(args, arg, guessed_val)
    #         # else keep the given value
    #     except ValueError as v_err:
    #         logger.debug("failed to guess value for '{}'".format(arg))
    #         logger.debug(v_err)
    logger.debug("parsing '{}'".format(args.filepath))
    for pattern_name, pattern in get_ingest_formats().items():
        # check if args.filepath matches this pattern
        if filename_matches_pattern(args.filepath, pattern):
            logger.debug('matches pattern "{}"'.format(pattern))
            setattr(args, 'date', _parse_date(args.filepath, pattern))
            logger.debug('date extracted: {}'.format(args.date))
            # get list of attributes which are in the pattern:
            attribs_in_pattern = [ s.split("}")[0] for s in pattern.split("{")[1:] ]
            for param in attribs_in_pattern:
                val = parse(param, args.filepath, args.filepath)[0]
                # TODO: check guessed argument value does not overwrite
                setattr(args, param, val)
                logger.debug('{} extracted :"{}"'.format(param, val))
            return args
    else:
        logger.warn("could not match filepath to any known patterns.")
        return args

def parse(key, strptime_filename, filename):
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
        if key == "date":  # must handle date specially
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

# TODO: this is a duplicate of check_match?
def filename_matches_pattern(filename, pattern):
    """ returns true if given filename matches given pattern """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
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
