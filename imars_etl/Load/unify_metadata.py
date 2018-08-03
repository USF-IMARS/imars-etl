import json
import logging
import sys

from imars_etl.exceptions.InputValidationError import InputValidationError
from imars_etl.Load.metadata_constraints import ensure_constistent_metadata
from imars_etl.filepath.parse_filepath import parse_filepath


def unify_metadata(**kwargs):
    """
    Combines metadata from file & json dicts into kwargs.
    Raises error if data is mismatched.
    """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    logger.setLevel(logging.DEBUG)
    logger.info('input metadata:\n{}\n'.format(kwargs))

    logger.debug("constrain input meta...")
    kwargs = ensure_constistent_metadata(kwargs)
    # === filepath metadata
    # TODO: should be `kwargs.get('noparse', LOAD_DEFAULTS['noparse'])`
    if kwargs.get('noparse', False) is False:
        path_metadata = parse_filepath(**kwargs)
    else:
        path_metadata = {}

    # === file metadata
    if kwargs.get('metadata_file') is not None:
        file_metadata = _read_metadata_file(
            kwargs['metadata_file_driver'],
            kwargs['metadata_file']
        )
    else:
        file_metadata = {}

    # === json metadata
    try:  # add json args to kwargs
        json_metadata = json.loads(kwargs['json'])
        _dict_union_raise_on_conflict(kwargs, json_metadata)
    except TypeError:
        logger.debug("json str is empty")
        json_metadata = {}
    except KeyError:
        logger.debug("kwargs['json'] is None")
        json_metadata = {}

    # === sql metadata
    try:  # add sql args to kwargs
        sql_metadata = sql_str_to_dict(kwargs['sql'])
        _dict_union_raise_on_conflict(kwargs, sql_metadata)
    except TypeError:
        logger.debug("sql str is empty")
        sql_metadata = {}
    except KeyError:
        logger.debug("kwargs['sql'] is None")
        sql_metadata = {}

    logger.debug("constrain path meta...")
    path_metadata = ensure_constistent_metadata(path_metadata)
    logger.debug("constrain file meta...")
    file_metadata = ensure_constistent_metadata(file_metadata)
    logger.debug("constrain json meta...")
    json_metadata = ensure_constistent_metadata(json_metadata)
    logger.debug("constrain sql meta..")
    sql_metadata = ensure_constistent_metadata(sql_metadata)

    final_meta = _union_dicts_raise_on_conflict(
        kwargs,
        path_metadata,
        file_metadata,
        json_metadata,
        sql_metadata
    )
    final_meta = _rm_dict_none_values(final_meta)

    input_set = set(kwargs.items())
    final_set = set(final_meta.items())
    logger.info('added metadata:\n{}\n'.format(input_set ^ final_set))
    return final_meta


def sql_str_to_dict(sql_str):
    """
    Transforms sql selector string into key-val dict.
    Expects sql string in the a form like `a=1 AND col="val"`, which transforms
    into `{'a': 1, 'col': 'val'}`.

    NOTE: 'AND' *is* case-sensitive here.
    """
    result = {}
    if sql_str is None or len(sql_str) < 1:
        return result
    else:
        pairs = sql_str.split(" AND ")
        for pair in pairs:
            key, val = pair.split('=')
            try:
                val = int(val)
            except ValueError:
                pass  # failure to convert to int just means it isn't an int.
            result[key] = val
        return result


def _read_metadata_file(driver, filepath):
    """reads metadata using driver on given filepath"""
    parser = driver(filepath)
    # TODO: metad = parser.get_metadata()

    metad = {
        'uuid': parser.get_uuid(),
        'date_time': parser.get_datetime(),
    }
    return ensure_constistent_metadata(
        metad
    )


def _union_dicts_raise_on_conflict(*args):
    """Union of all given dicts but raises err if any mismatched keyvals"""
    # check each dict against every dict that follows it
    # | AxB AxC AxD | BxC BxD | CxD |
    result_dict = {}
    for dict_i in args:
        result_dict = _dict_union_raise_on_conflict(result_dict, dict_i)
    return result_dict


def _rm_dict_none_values(dict_w_nones):
    """removes keys with values that are None & returns none-less dict"""
    return {k: v for k, v in dict_w_nones.items() if v is not None}


def _dict_union_raise_on_conflict(dict_a, dict_b, auto_reconsile=True):
    """
    Union of a & b, but raises error if two keys w/ different vals.
    NOTE: value comparison is done after converting values to strings.
        This is _probably_ ok for simple types (int & str), but could cause
        weird things if you use this to union dicts with fancy val types.
    """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    logger.setLevel(logging.DEBUG)

    # TODO: maybe this should be done elsewhere?
    dict_a = _rm_dict_none_values(dict_a)
    dict_b = _rm_dict_none_values(dict_b)

    for key in dict_b:
        if key in dict_a and str(dict_a[key]) != str(dict_b[key]):
            val_a = dict_a[key]
            val_b = dict_b[key]
            if auto_reconsile:
                logger.debug(
                    'conflict: \n\t{} != {}'.format(val_a, val_b) +
                    '\n\ttrying to reconsile dict values for {}...'.format(key)
                )
                newval = _reconsile_conflicting_values(val_a, val_b)
                logger.debug('...resolved to {}'.format(newval))
                dict_a[key] = newval
            else:
                raise InputValidationError(
                    "Conflicting file metadata for key '{}':".format(key) +
                    "\n\tval a : {} ({})".format(val_a, type(val_a)) +
                    "\n\tval b : {} ({})".format(val_b, type(val_b))
                )
        else:
            dict_a[key] = dict_b[key]
    return dict_a


def _reconsile_conflicting_values(vala, valb):
    """attempts to resolve two conflicting values into one"""
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    logger.setLevel(logging.DEBUG)
    newval = None
    try:
        newval = _reconsile_conflicting_datetimes(vala, valb)
    except Exception as dt_err:
        # a & b must not be datetime-like
        logger.debug(
            "cannot reconsile as datetimes, err: \n\t{}".format(dt_err)
        )

    if newval is None:
        raise InputValidationError("cannot reconsile values.")
    else:
        return newval


def _reconsile_conflicting_datetimes(datea, dateb):
    """
    Attempts to pick between two conflicting datetimes by assuming that
    datetimes ending like YYYY-01-01T00:00:00 represent imprecision.
    """
    len_a = _estimate_datetime_precision(datea)
    len_b = _estimate_datetime_precision(dateb)
    if len_a == len_b:
        raise ValueError(
            "cannot resolve conflicting dates of same precision "
            "({}=={})".format(len_a, len_b)
        )
    elif len_a > len_b:
        return datea
    elif len_b > len_a:
        return dateb


def _estimate_datetime_precision(dt):
    """
    Returns an int that is larger if the datetime appears more precise.
    Precision is determined by how precise values _seem_ by comparing with
    placeholder values.
    We start off assuming max precision, then we will whittle down from
    smallest time increment up to year, which we do not check.
    Example: month, day, hour, minute are not checked for placeholder values
    if a non-placeholder second is found. 2018-01-01T00:00:00.02 is maximally
    precise, even though the month, day, hour, minute, and second _seem_ like
    they chould be placeholders.

    Returns
    --------
    precision : int
        min 1. max 7.
        1 means only year _seems_ precise.
        7 means precise to the milliseconds.
        Each integer represents another step up in time increment precision.
    """
    imprecise_placeholders = [
        # these must be smallest to largest
        ('microsecond', 0),
        ('second', 0),
        ('minute', 0),
        ('hour', 0),
        ('day', 1),
        ('month', 1),
    ]
    precision = len(imprecise_placeholders) + 1
    for attribkey, placeholder in imprecise_placeholders:
        if getattr(dt, attribkey) == placeholder:
            precision -= 1
        else:
            return precision
    else:
        return precision  # should be 1 at this point
